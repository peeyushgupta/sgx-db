#include "db.hpp"
#include "time.hpp"
#include "util.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#include "bcache.hpp"
#include "x86.hpp"
#include "spinlock.hpp"

#include <cstdlib>
#include <cstdio>

#include <string.h>
#include <atomic>
#include <cmath>
#include <cerrno>

#include "bitonic_sort.hpp"
#include "column_sort.hpp"
#include "quick_sort.hpp"

//#define FILE_READ_SIZE (1 << 12)

#define FILE_READ_SIZE DATA_BLOCK_SIZE

#define OCALL_VERBOSE 0
#define JOIN_VERBOSE 0
#define IO_VERBOSE 0
#define PIN_VERBOSE 0

data_base_t* g_dbs[MAX_DATABASES];

static unsigned int g_thread_id = 0;
thread_local int thread_id; 

int reserve_tid() { 
	thread_id = __sync_fetch_and_add(&g_thread_id, 1);
	return thread_id; 
}

void reset_tids() { 
	g_thread_id = 0;
}

int tid() { return thread_id; }

data_base_t *get_db(unsigned int id) {
	if (id < MAX_DATABASES)
		return g_dbs[id];
	return NULL;
}

std::string get_schema_type(schema_type_t t) {
	switch (t) {
	case BOOLEAN:
		return "BOOL";
	case BINARY:
		return "BINARY";
	case VARBINARY:
		return "VARBINARY";
	case DECIMAL:
		return "DECIMAL";
	case CHARACTER:
		return "CHARACTER";
	case VARCHAR:
		return "VARCHAR";
	case INTEGER:
		return "INTEGER";
	case TINYTEXT:
		return "TINYTEXT";
	case PADDING:
		return "PADDING";
	}
}

/* Data layout
 *
 * - Data base is a collection of tables
 * - Each table is a collection of data blocks that contain table's data
 * - Each table is padded to its MAX size
 * - Rows are fixed size for each table 
 * 
 */

/* We assume that each data base has several data blocks for processing 
   of unencrypted data in enclave's memory. The idea is that DB reads data 
   from the external storage, e.g., disk or NVM into these data blocks, decrypts 
   data and processes it there. 

   Allocate these data blocks. 
 */
int alloc_data_blocks(data_base_t *db) {
	int i; 
	for (i = 0; i < DATA_BLKS_PER_DB; i++) {
#ifdef ALIGNED_ALLOC
		db->bcache.data_blks[i].data = aligned_malloc(DATA_BLOCK_SIZE, ALIGNMENT);
#else
		db->bcache.data_blks[i].data = malloc(DATA_BLOCK_SIZE);
#endif
		if (!db->bcache.data_blks[i].data) {
			ERR("alloc failed\n"); 
			goto cleanup;
		};
	};

	binit(&db->bcache);

	for (i = 0; i < THREADS_PER_DB; i++) {
		ocall_alloc_io_buf(&db->io_buf[i], FILE_READ_SIZE);
		if (!db->io_buf[i]) {
			ERR("alloc of io buffer failed\n"); 
			goto cleanup_io_bufs;
		};
	};

	return 0; 


cleanup_io_bufs:
	for (int j = 0; j < i; j++ ) {
		ocall_free_io_buf(db->io_buf[j]);
		db->io_buf[j] = NULL; 
	};

	i = DATA_BLKS_PER_DB; 
cleanup: 

	for (int j = 0; j < i; j++ ) {
#ifdef ALIGNED_ALLOC
		aligned_free(db->bcache.data_blks[j].data);
#else
		free(db->bcache.data_blks[j].data);
#endif
		db->bcache.data_blks[j].data = NULL; 
	};
	return -ENOMEM;
};

/* Free data blocks in enclave's memory */
void free_data_blocks(data_base_t *db) {
	for (int i = 0; i < DATA_BLKS_PER_DB; i++ ) {
		if(db->bcache.data_blks[i].data) {
#ifdef ALIGNED_ALLOC
			aligned_free(db->bcache.data_blks[i].data);
#else
			free(db->bcache.data_blks[i].data);
#endif
			db->bcache.data_blks[i].data = NULL; 
		}
	};

	for (int j = 0; j < THREADS_PER_DB; j++ ) {
		if (db->io_buf[j]) {
			ocall_free_io_buf(db->io_buf[j]);
			db->io_buf[j] = NULL; 
		}
	};

	return; 
};


/* Read data block from external storage into enclave's memory (the memory 
   region is passed as the  DataBlock argument), decrypt on the fly */
int read_data_block(table *table, unsigned long blk_num, void *buf) {
	unsigned long long total_read = 0; 
	int read, ret; 

#if defined(REPORT_IO_STATS)
	unsigned long long start, end; 
	start = RDTSC();
#endif
	if(tid() >= THREADS_PER_DB) {
		ERR("tid():%d >= THREADS_PER_DB (%d)\n", tid(), THREADS_PER_DB); 
		return -1; 
	}

#if defined(IO_LOCK)
	acquire(&table->db->bcache.iolock); 	
#endif 
	ocall_seek(&ret, table->fd[tid()], blk_num*DATA_BLOCK_SIZE); 

#if defined(REPORT_IO_STATS)	
	end = RDTSC();
	DBG_ON(IO_VERBOSE, "ocall_seek: %llu cycles\n", end - start);
#endif

#if defined(REPORT_IO_STATS)
	start = RDTSC();
#endif
	while (total_read < DATA_BLOCK_SIZE) { 
		ocall_read_file(&read, table->fd[tid()], 
				table->db->io_buf[tid()], 
				FILE_READ_SIZE);
		if (read < 0) {
#if defined(IO_LOCK)
			release(&table->db->bcache.iolock); 	
#endif
			ERR("read failed\n");
			return read;
		}

		if (read == 0) {
			/* We've reached the end of file, pad with zeroes */
			read = DATA_BLOCK_SIZE - total_read; 
			memset((void *)((char *)buf + total_read), 0, read); 
#if defined(IO_LOCK)
			release(&table->db->bcache.iolock); 	
#endif
			return 0; 
		} else {
			/* Copy data from the I/O buffer into bcache buffer */
			memcpy((void *)((char *)buf + total_read), table->db->io_buf[tid()], read); 
		}
		total_read += read;  
	}
#if defined(IO_LOCK)
	release(&table->db->bcache.iolock); 	
#endif

#if defined(REPORT_IO_STATS)
	end = RDTSC();
	DBG_ON(IO_VERBOSE, 
		"ocall_read_file: %llu cycles\n", end - start);
#endif
	return 0; 
}

/* Write data block from enclave's memory back to disk. 
 * For temporary results, we'll create temporary tables that will 
 * have corresponding encryption keys (huh?)
*/
int write_data_block(table *table, unsigned long blk_num, void *buf) {
	unsigned long long total_written = 0, write_size; 
	int written, ret; 

	if(tid() >= THREADS_PER_DB) {
		ERR("tid():%d >= THREADS_PER_DB (%d)\n", tid(), THREADS_PER_DB); 
		return -1; 
	}	

#if defined(IO_LOCK)
	acquire(&table->db->bcache.iolock); 	
#endif

	ocall_seek(&ret, table->fd[tid()], blk_num*DATA_BLOCK_SIZE);
 
	while (total_written < DATA_BLOCK_SIZE) { 
		/* make sure we don't write more than DATA_BLOCK_SIZE */
		write_size = (total_written + FILE_READ_SIZE) <= DATA_BLOCK_SIZE ? 
			FILE_READ_SIZE : DATA_BLOCK_SIZE - total_written;  

		/* Copy data into the I/O buffer */
		memcpy(table->db->io_buf[tid()], 
			(void *)((char *)buf + total_written), write_size); 
		/* Submit I/O buffer */
		ocall_write_file(&written, table->fd[tid()], 
			table->db->io_buf[tid()], 
			write_size);
		if (written < 0) {
#if defined(IO_LOCK)
			release(&table->db->bcache.iolock); 	
#endif
			ERR("write filed\n"); 
			return written;
		} 
		total_written += written;  
	}
#if defined(IO_LOCK)
	release(&table->db->bcache.iolock); 	
#endif
	return 0; 
}

/*
 *
 * Public DB interace, i.e., something clients can invoke
 *
 */

/* Create data base, returns dbId */
int ecall_create_db(const char *cname, int name_len, int *db_id) {

	data_base *db;
	int i, ret; 
	std::string name(cname, name_len);
	
	/* AB: Check for enclave-related validation... what needs to be 
	   done? */
	if (!cname || (name_len == 0) || !db_id)
		return -1;  

	/* Look up an empty DB slot */
	for (i = 0; i < MAX_DATABASES; i++) {
		if(!g_dbs[i])
			break; 
	}

	if (i == MAX_DATABASES)
		return -2;

	db = new data_base_t();
	if (!db) 
		return -ENOMEM;
	
	db->name = name;

	g_dbs[i] = db;
	*db_id = i; 

	/* Allocate data blocks */
	ret = alloc_data_blocks(db);
	if (ret)
		goto cleanup;

	return 0;

cleanup: 
	delete db; 
	g_dbs[i] = NULL;
	return ret;
};

/* Free data base */
int ecall_free_db(int db_id) {
	data_base *db;

	db = g_dbs[db_id];

	if (db_id >= MAX_DATABASES) {
		ERR("Trying to free wrong DB: %d > MAX_DATABASES (%d)\n", 
			db_id, MAX_DATABASES);
		return -EINVAL;  
	}

	if (!db) {
		ERR("Trying to free NULL db: %d\n", db_id);
		return -EINVAL;  
	}

	
	/* Free all tables */
	for (auto i = 0; i < MAX_TABLES; i++) {
		if(db->tables[i]) {
			free_table(db->tables[i]); 
			db->tables[i] = NULL; 
		}
	}


	g_dbs[db_id] = NULL;
	free_data_blocks(db);
	delete db;
 
	return 0;
}


int create_table(data_base_t *db, std::string &name, schema_t *schema, table_t **new_table) {
	table_t *table; 
	int i, fd, ret, sgx_ret; 

	/* Look up an empty Table slot */
	for (i = 0; i < MAX_TABLES; i++) {
		if(!db->tables[i])
			break; 
	}

	if (i == MAX_TABLES)
		return -3; 

	table = new table_t();
	if (!table) 
		return -ENOMEM;

	db->tables[i] = table;
	table->id = i; 
#ifdef PAD_SCHEMA
	schema_t new_sc;
 	int pad_bytes; 
	if(row_size(schema) % ALIGNMENT != 0) {
		pad_bytes = ((row_size(schema) + ALIGNMENT) & ~(ALIGNMENT - 1)) - row_size(schema);
		DBG("pad new table:%s, pad_bytes:%d, row_size(schema):%d\n", 
			name.c_str(), pad_bytes, row_size(schema)); 
		pad_schema(schema, pad_bytes, &new_sc);
		table->sc = new_sc;
	} else {
		table->sc = *schema;
	}
#else
	table->sc = *schema;
#endif

	table->name = name;
	table->num_rows = 0; 
	table->num_blks = 0; 
	table->db = db; 
	table->pinned_blocks = NULL; 
	table->rows_per_blk = DATA_BLOCK_SIZE / row_size(table); 

	/* Call outside of enclave to open a file for the table */
	sgx_ret = ocall_open_file(&fd, name.c_str());
	if (sgx_ret || fd < 0) {
		ret = -5;
		goto cleanup; 
	} 
	
	for (i = 0; i < THREADS_PER_DB; i++) {
		/* Call outside of enclave to open a file for the table */
		//sgx_ret = ocall_open_file(&fd, name.c_str());
		//if (sgx_ret || fd < 0) {
		//	ret = -5;
		//	goto cleanup; 
		//} 

		//DBG("table:%s, fd[%d]:%d\n", table->name.c_str(), i, fd); 
		table->fd[i] = fd;
		
	};

	*new_table = table; 
	return 0;

cleanup:
	delete table;
	db->tables[i] = NULL; 
	return ret; 

}

/* Create table, in the database dbId, described by the schema, returns tableId */
int ecall_create_table(int db_id, const char *cname, int name_len, schema_t *schema, int *table_id) {
	int ret;
	std::string name(cname, name_len);
	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)) || !schema || !table_id || !cname ||
			(name_len == 0))
		return -1; 

	ret = create_table(db, name, schema, &table); 
	if (ret) {
		ERR("failed to create table %s, ret:%d\n", cname, ret); 
		return ret; 
	}

	*table_id = table->id; 
	return 0; 
};

void free_table(table_t *table) {
	int ret, sgx_ret; 

	for(int i = 0; i < 1 /* THREADS_PER_DB*/; i++) {
		/* Call outside of enclave to open a file for the table */
		sgx_ret = ocall_close_file(&ret, table->fd[i]);
		if (sgx_ret || ret) {
			ERR("Failed to close table file (table:%s, fd[%d]), sgx_ret:%d, ret:%d\n", 
				table->name.c_str(), table->fd[i], sgx_ret, ret); 
		} 
	}

	delete table;
	return; 
}

int delete_table(data_base_t *db, table_t *table) {
	int ret, sgx_ret; 

	//DBG("deleting table %p\n", table); 
	
	db->tables[table->id] = NULL;


	for(int i = 0; i < 1 /* THREADS_PER_DB*/; i++) {
		sgx_ret = ocall_close_file(&ret, table->fd[i]);
		if (sgx_ret) {
			ERR("Failed to close table file (table:%s, fd[%d]), err:%d\n", 
				table->name.c_str(), table->fd[i], sgx_ret); 
			ret = sgx_ret;
		} 
	}

	/* Call outside of enclave to delete a file for the table */
	sgx_ret = ocall_rm_file(&ret, table->name.c_str());
	if (sgx_ret) {
		ret = sgx_ret;
	} 

	delete table;
	return ret; 

}

/* Flush all data to disk */
int ecall_flush_table(int db_id, int table_id) {
	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)))
		return -1;

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];
	
	bflush(table);
	return 0;
	

};

/* Insert one row */
int ecall_insert_row(int db_id, int table_id, void *row_data) {

   /* An oblivious version of row insert will update all 
      rows in the table to conceal its size */
	return -1; 
}

void *get_column(schema_t *sc, int field, row_t *row) {
	//printf("%s: row:%p, field:%d, offset:%d\n", __func__, row, field, sc->offsets[field]); 
	return(void*)&row->data[sc->offsets[field]];
}

void write_column(schema_t *sc, int field, row_t *row, const void *data) {
	void* dest = get_column(sc, field, row);
	switch (sc->types[field])
	{
		case TINYTEXT:
			strncpy((char*)dest, (char*)data, sc->sizes[field]);
			break;
	
		default:
			memcpy(dest, data, sc->sizes[field]);
			break;
	}
}

/* returns ture if column of row_l is found respectively to be greater then column of row_r */

bool compare_rows(schema_t *sc, int column, row_t *row_l, row_t *row_r) {
	bool res; 

	/* make sure fake touples are always greater */
	if (row_l->header.fake)
		return true; 

	switch(sc->types[column]) {
	case BOOLEAN:
		res = *(bool*)&row_l->data[sc->offsets[column]] >
			*(bool*)&row_r->data[sc->offsets[column]]; 
		break;
	
	case CHARACTER: 
		res = *(char*)&row_l->data[sc->offsets[column]] >
			*(char*)&row_r->data[sc->offsets[column]]; 
		break;
 
	case TINYTEXT:
	case VARCHAR: {
		int str_ret = strcmp (&row_l->data[sc->offsets[column]], 
			&row_r->data[sc->offsets[column]]) ;
		res = (str_ret > 0);
		break;
	}
	case INTEGER:
		res = *(int*)&row_l->data[sc->offsets[column]] > 
			*(int*)&row_r->data[sc->offsets[column]]; 

		break;
	default: 
		res = false;
	}
	return res;
}

/* XXX: accidentally ended up writing the same function twice */

bool cmp_row(table_t *tbl_left, row_t *row_left, int field_left, table_t *tbl_right, row_t *row_right, int field_right) {
	
	/*DBG("field left (%d), left type (%d), field right (%d), right type (%d)\n",
	field_left, tbl_left->sc.types[field_left], 
	field_right, tbl_right->sc.types[field_right]);
	*/
	
	if(tbl_left->sc.types[field_left] != tbl_right->sc.types[field_right])
		return false;

	switch (tbl_left->sc.types[field_left]) {
	case BOOLEAN: {
		return (*((bool*)get_column(&tbl_left->sc, field_left, row_left)) 
			== *((bool*)get_column(&tbl_right->sc, field_right, row_right))); 
	}
	case INTEGER: {
		return (*((int*)get_column(&tbl_left->sc, field_left, row_left)) 
			== *((int*)get_column(&tbl_right->sc, field_right, row_right))); 
	}
	case TINYTEXT: {
		char *left = (char*)get_column(&tbl_left->sc, field_left, row_left); 
		char *right = (char*)get_column(&tbl_right->sc, field_right, row_right);

		DBG_ON(JOIN_VERBOSE, "left:%s, right:%s\n", left, right); 

		int ret = strncmp(left, right, MAX_ROW_SIZE);
		if (ret == 0) 
			return true;  
		return false;
	}
	default: 
		return false; 
	}

	return false;
}

int join_schema(schema_t *sc, schema_t *left, schema_t *right) {
	
	sc->num_fields = left->num_fields + right->num_fields;
	if(sc->num_fields > MAX_COLS)
		return -1; 

	for(int i = 0; i < left->num_fields; i++) {
		sc->offsets[i] = left->offsets[i];
		sc->sizes[i] = left->sizes[i];
		sc->types[i] = left->types[i];	
	}

	for(int i = 0; i < right->num_fields; i++) {
		sc->offsets[i + left->num_fields] = left->row_data_size + right->offsets[i];
		sc->sizes[i + left->num_fields] = right->sizes[i];
		sc->types[i + left->num_fields] = right->types[i];	
	}
	sc->row_data_size = sc->offsets[sc->num_fields - 1] + sc->sizes[sc->num_fields - 1];

	return 0;
}

int join_schema_algo(schema_t *sc, schema_t *left, schema_t *right,
		int *join_columns, int num_join_columns)
{
	auto num_fields = left->num_fields + right->num_fields - num_join_columns;

	if (num_fields > MAX_COLS)
		return -1; 

	sc->num_fields = num_fields;

	for (auto i = 0; i < left->num_fields; i++) {
		sc->offsets[i] = left->offsets[i];
		sc->sizes[i] = left->sizes[i];
		sc->types[i] = left->types[i];	
	}

	auto counter = left->num_fields;

	auto offset_adj = [&]()->int {
		auto sum = 0u;
		for (auto j = 0; j < num_join_columns; j++) {
			sum += right->sizes[join_columns[j]];
		}
		return sum;
	}();

	INFO("Offset adjustment: %d\n", offset_adj);

	for (auto i = 0; i < right->num_fields; i++) {

		auto should_skip = [&]() {
			for (auto j = 0; j < num_join_columns; j++) {
				if (join_columns[j] == i){
					// skip matching column
					INFO(" skipping join (%d)th column\n", j);
					return true;
				}
			}
			return false;
		}();

		if (should_skip)
			continue;

		sc->offsets[counter] = left->row_data_size + right->offsets[i] - offset_adj;
		sc->sizes[counter] = right->sizes[i];
		sc->types[counter] = right->types[i];
		counter++;
	}

	sc->row_data_size = sc->offsets[sc->num_fields - 1] + sc->sizes[sc->num_fields - 1];

	return 0;
}

/* Pin table in buffer cache  */
int pin_table(table_t *table) {

	unsigned long blk_num;
	unsigned long number_of_blks; 
	data_block_t *b;

	ERR_ON(table->num_rows == 0, 
		"Nothing to pin, table:%s empty\n", table->name.c_str()); 

	number_of_blks = table->num_rows / table->rows_per_blk + 1; 

	table->pinned_blocks = (data_block_t **) malloc(number_of_blks*sizeof(data_block_t*)); 

	if (!table->pinned_blocks)
		return -ENOMEM;

	for(blk_num = 0; blk_num*table->rows_per_blk < table->num_rows; blk_num++) 
	{
		DBG_ON(PIN_VERBOSE, "pin:%s, blk_num: %d\n", table->name.c_str(), blk_num);

        	b = bread(table, blk_num);
		ERR_ON(!b, "got NULL block"); 
		table->pinned_blocks[blk_num] = b; 
	} 	
	return 0; 
}

/* Unpin table in buffer cache  */
int unpin_table_dirty(table_t *table) {

	unsigned long blk_num;
	data_block_t *b;


	for(blk_num = 0; blk_num*table->rows_per_blk < table->num_rows; blk_num++) 
	{ 
		bwrite(table->pinned_blocks[blk_num]); 
		brelse(table->pinned_blocks[blk_num]); 
	} 
	
	if (table->pinned_blocks) {
		free(table->pinned_blocks);
		table->pinned_blocks = NULL;  
	}
	return 0; 
}

int join_rows(row_t *join_row, unsigned int join_row_data_size, row_t *
		row_left, unsigned int row_left_data_size, row_t * row_right,
		unsigned int row_right_data_size, unsigned int offset) {

	/*
	   if(row_left_data_size + row_right_data_size > join_row_data_size)
	   return -1;
	   */

	memcpy(join_row, row_left, row_header_size() + row_left_data_size);
	memcpy((void*)((char*)join_row + row_header_size() +
				row_left_data_size), row_right->data + offset,
			row_right_data_size-offset);
	return 0; 
}; 

int _join_rows(row_t *join_row, unsigned int join_row_data_size, row_t *
		row_r, schema_t *sc_r, row_t * row_s, schema_t *sc_s) {

	auto calc_bytes_to_copy = [&](auto sc) {
		for (int i = 0; i < sc->num_fields; i++) {
			if (sc->types[i] == PADDING) {
				return sc->offsets[i];
			}
		}
	};

	// We should skip the joining column
	auto row_s_offset = sc_s->sizes[0];
	// compute bytes to copy from table R
	auto row_r_bytes = calc_bytes_to_copy(sc_r);
	// compute bytes to copy from table S
	auto row_s_bytes = calc_bytes_to_copy(sc_s);

	memcpy(join_row, row_r, row_header_size() + row_r_bytes);

	memcpy((void*)((char*)join_row + row_header_size() +
				row_r_bytes), row_s->data + row_s_offset,
			row_s_bytes - row_s_offset);
	return 0;
}

/* Join */
int ecall_join(int db_id, join_condition_t *c, int *join_table_id) {
	int ret;
	data_base_t *db;
	table_t *tbl_left, *tbl_right, *join_table;
	row_t *row_left = NULL, *row_right = NULL, *join_row = NULL;
	schema_t join_sc;
	std::string join_table_name;  

	if (!(db = get_db(db_id)) || !c )
		return -1; 

	tbl_left = db->tables[c->table_left];
	tbl_right = db->tables[c->table_right];
	if (! tbl_left || ! tbl_right)
		return -3; 

	join_table_name = "join:" + tbl_left->name + tbl_right->name; 

	ret = join_schema(&join_sc, &tbl_left->sc, &tbl_right->sc); 
	if (ret) {
		ERR("create table error:%d\n", ret);
		return ret; 
	}

	ret = create_table(db, join_table_name, &join_sc, &join_table);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	*join_table_id = join_table->id; 

	DBG("Created join table %s, id:%d\n", join_table_name.c_str(), join_table_id); 

	join_row = (row_t *) malloc(row_size(tbl_left) + row_size(tbl_right) - row_header_size());
	if(!join_row)
		return -ENOMEM;

	row_left = (row_t *) malloc(row_size(tbl_left));
	if(!row_left)
		return -ENOMEM;

	row_right = (row_t *) malloc(row_size(tbl_right));
	if(!row_right)
		return -ENOMEM;

#if defined(REPORT_JOIN_STATS)
	unsigned long long start = RDTSC(), end; 
	unsigned int i_start = 0;

	/* Reporting interval = 5 seconds */
	const unsigned long long REPORTING_INTERVAL = cycles_per_sec * 5;
#endif

	for (unsigned int i = 0; i < tbl_left->num_rows; i ++) {

#if defined(REPORT_JOIN_STATS)

		end = RDTSC();
		if(end - start > REPORTING_INTERVAL) {
			unsigned long long num_recs = (i - i_start) * tbl_right->num_rows;
			unsigned long long cycles = end - start;
			unsigned long long secs = (cycles / cycles_per_sec);

			printf("Joined %llu recs (%llu recs per second, %llu cycles per rec)\n",
					num_recs, num_recs/secs, cycles/num_recs);
			i_start = i;
			start = RDTSC();
		};
#endif

		// Read left row
		ret = read_row(tbl_left, i, row_left);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, tbl_left->name.c_str());
			goto cleanup;
		}

				
		for (unsigned int j = 0; j < tbl_right->num_rows; j ++) {
			bool equal = true;
			bool eq;

			// Read right row
			ret = read_row(tbl_right, j, row_right);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					i, tbl_right->name.c_str());
				goto cleanup;
			}

			for(unsigned int k = 0; k < c->num_conditions; k++) {
				// DBG_ON(JOIN_VERBOSE, "comparing (i:%d, j:%d, k:%d\n", i, j, k); 
				// DBG("comparing (i:%d, j:%d, k:%d\n", i, j, k); 
				eq = cmp_row(tbl_left, row_left, c->fields_left[k], tbl_right, row_right, c->fields_right[k]);
				if (!eq) {
					equal = eq; 
				}

			}

			if (equal) { 
				INFO("value is equal: joining (i:%d, j:%d)\n", i, j); 
				//DBG_ON(JOIN_VERBOSE, "joining (i:%d, j:%d)\n", i, j); 

				DBG("size of join row data size: %d, right row size: %d, left row data size\n", join_sc.row_data_size, 
					tbl_left->sc.row_data_size, tbl_right->sc.row_data_size); 

				ret = join_rows(join_row, join_sc.row_data_size, 
					row_left, tbl_left->sc.row_data_size, 
					row_right, tbl_right->sc.row_data_size, 0); 
				if(ret) {
					ERR("failed to produce a joined row %d of table %s with row %d of table %s\n",
						i, tbl_left->name.c_str(), j, tbl_right->name.c_str());
					goto cleanup;
				}
			
				/* Add row to the join */
				ret = insert_row_dbg(join_table, join_row);
				if(ret) {
					ERR("failed to join row %d of table %s with row %d of table %s\n",
						i, tbl_left->name.c_str(), j, tbl_right->name.c_str());
					goto cleanup;
				}
			}

		}
	}

	bflush(join_table); 

	ret = 0;
cleanup: 
	if (join_row)
		free(join_row); 
	
	if (row_left)
		free(row_left); 

	if (row_right)
		free(row_right); 

	return ret; 
};

int promote_schema(schema_t *old_sc, int column, schema_t *new_sc) {
	schema_type_t type;
	int size;   
 
	/* Offsets of the columns that are right of the promoted column 
 		don't change since we just flip two columns on the left. The 
		offsets of the columns that are left of the promoted column 
		are simply shifted by the size of the promoted column */
	size = old_sc->sizes[column];
	type = old_sc->types[column];  
	for(int i = column; i > 0; i--) {
		new_sc->offsets[i] = old_sc->offsets[i-1] + size;
		new_sc->sizes[i] = old_sc->sizes[i-1];
		new_sc->types[i] = old_sc->types[i-1];	
	}

	for(int i = column + 1;  i < old_sc->num_fields; i++) {
		new_sc->offsets[i] = old_sc->offsets[i];
		new_sc->sizes[i] = old_sc->sizes[i];
		new_sc->types[i] = old_sc->types[i];	
	}

	new_sc->offsets[0] = 0;
	new_sc->sizes[0] = size; 
	new_sc->types[0] = type; 

        new_sc->num_fields = old_sc->num_fields; 
        new_sc->row_data_size = old_sc->row_data_size; 

	return 0;
}

int promote_row(row_t *old_row, schema_t *sc, int column, row_t * new_row) {
      
	/* Copy the header */
	memcpy(new_row, old_row, row_header_size()); 
 
	/* Copy data */
	memcpy(new_row->data, (char*)old_row->data + sc->offsets[column], sc->sizes[column]); 
	memcpy((char*)new_row->data + sc->sizes[column], old_row->data, sc->offsets[column]); 
	memcpy((char*)new_row->data + sc->sizes[column] + sc->offsets[column], 
		(char*)old_row->data + sc->offsets[column] + sc->sizes[column], 
		sc->row_data_size - (sc->offsets[column] + sc->sizes[column]));
	
        return 0; 
}; 

/* Before sorting the table we promote the column that we sort on 
   to the front -- this allows us to compare the bits of the columns 
   from two tables in an oblivious fashion by just comparing the first 
   N bytes of each row */

int promote_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl) {
	int ret;
	std::string p_tbl_name;  
        schema_t p_sc;
	row_t *row_old, *row_new; 

	p_tbl_name = "p:" + tbl->name; 

	ret = promote_schema(&tbl->sc, column, &p_sc); 
	if (ret) {
		ERR("create table error:%d\n", ret);
		return ret; 
	}

	ret = create_table(db, p_tbl_name, &p_sc, p_tbl);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	DBG("Created promoted table %s, id:%d\n", 
            p_tbl_name.c_str(), (*p_tbl)->id); 

	row_old = (row_t*) malloc(row_size(tbl));
	if(!row_old)
		return -ENOMEM;

	row_new = (row_t*)malloc(row_size(tbl));
	if(!row_new)
		return -ENOMEM;

	for (unsigned int i = 0; i < tbl->num_rows; i ++) {

		// Read old row
		ret = read_row(tbl, i, row_old);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, tbl->name.c_str());
			goto cleanup;
		}

				
		// Promote row
		ret = promote_row(row_old, &tbl->sc, column, row_new);
		if(ret) {
			ERR("failed to promote row %d of table %s\n",
				i, tbl->name.c_str());
			goto cleanup;
		}

		/* Add row to the promoted table */
		ret = insert_row_dbg(*p_tbl, row_new);
		if(ret) {
			ERR("failed to insert row %d of promoted table %s\n",
				i, (*p_tbl)->name.c_str());
			goto cleanup;
		}

	}

	bflush(*p_tbl); 

	ret = 0;
cleanup: 
	if (row_old)
		free(row_old); 

	if (row_new)
		free(row_new); 

	return ret; 
};

/* 
 * 
 * Insecure interfaces... debug only 
 *
 */
/* Read one row. */
int read_row(table_t *table, unsigned int row_num, row_t *row) {

	unsigned long dblk_num;
	unsigned long row_off; 
	data_block_t *b;

	/* Make a fake row if it's outside of the table
           assuming it's padding */
	if(row_num >= table->num_rows) {
		row->header.fake = true; 
		return 0;
	} 

	dblk_num = row_num / table->rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (row_num - dblk_num * table->rows_per_blk) * row_size(table); 
	
        b = bread(table, dblk_num);
	ERR_ON(!b, "got NULL block"); 

	/* Copy the row into the data block */
	memcpy(row, (char*)b->data + row_off, row_size(table)); 

	brelse(b);
	return 0; 
}

int write_row_dbg(table_t *table, row_t *row, unsigned int row_num) {
	unsigned long dblk_num;
	unsigned long row_off; 
	unsigned int old_num_rows, tmp_num_rows; 
	data_block_t *b;

	do {
		tmp_num_rows = table->num_rows;
		old_num_rows = tmp_num_rows;  
		if(row_num >= tmp_num_rows) {
			old_num_rows = xchg(&table->num_rows, (row_num + 1)); 
		}
	} while (old_num_rows != tmp_num_rows);  

	dblk_num = row_num / table->rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (row_num - dblk_num * table->rows_per_blk) * row_size(table); 
	
        b = bread(table, dblk_num);
	ERR_ON(!b, "got NULL block"); 

	/* Copy the row into the data block */
	memcpy((char*)b->data + row_off, row, row_size(table)); 

	bwrite(b);
	ERR_ON(b->blk_num != dblk_num, "b->blk_num (%d) != dblk_num (%d) blk:%p (flags:%x), num:%d, b->table:%s for table:%s\n", 
				b->blk_num, dblk_num, b, b->flags, b->blk_num, b->table ? b->table->name.c_str() : "NULL", table->name.c_str()); 



	brelse(b);
	return 0; 
}


int insert_row_dbg(table_t *table, row_t *row) {
	unsigned long dblk_num, row_num;
	unsigned long row_off; 
	data_block_t *b;

	//DBG("insert row, row size:%lu\n", table->sc.row_size); 
	
	row_num = __sync_fetch_and_add(&table->num_rows, 1);

	dblk_num = row_num / table->rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (row_num - dblk_num * table->rows_per_blk) * row_size(table); 
	
        b = bread(table, dblk_num);
	ERR_ON(!b, "got NULL block"); 

	/* Copy the row into the data block */
	memcpy((char*)b->data + row_off, row, row_size(table)); 
	
	bwrite(b);
	brelse(b);
	return 0; 


}

/* Insert one row. This one is not oblivious, will insert 
   a row at the very end of the table which is pointed by 
   table->num_rows 
 */
int ecall_insert_row_dbg(int db_id, int table_id, void *row_data) {

	data_base_t *db;
	table_t *table;
	row_t *row;
	int ret; 

	if (!(db = get_db(db_id)) || !row_data)
		return -1; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	row = (row_t*) malloc(row_size(table)); 

	if(!row)
		return -ENOMEM;

	row->header.fake = false;
	row->header.from = table_id;
	memcpy(row->data, row_data, row_data_size(table)); 
	ret = insert_row_dbg(table, row); 	

	free(row); 
	return ret; 
}

int print_schema(schema_t *sc, std::string name)
{
	auto total_sz = 0u;
	printf("Dumping" TXT_FG_GREEN " %s " TXT_FG_WHITE "schema with %d "
			"fields\n", name.c_str(), sc->num_fields);
	printf("+-------------------------------------+\n");
	printf("| column | offset |    type (size)    |\n");
	printf("+-------------------------------------+\n");
	for(int i = 0;  i < sc->num_fields; i++) {
		printf("|%8d|%8d|%10s(%5d)  |\n", i, sc->offsets[i],
				get_schema_type(sc->types[i]).c_str(),
				sc->sizes[i]);
		total_sz += sc->sizes[i];
	}
	total_sz += row_header_size();
	printf("+-------------------------------------+\n");
	printf("row_size " TXT_FG_GREEN "%d" TXT_FG_WHITE " bytes"
		" row_data_sz " TXT_FG_GREEN "%d" TXT_FG_WHITE " bytes\n",
			total_sz, sc->row_data_size);
}

int print_row(schema_t *sc, row_t *row) {
	bool first = true; 	
	for(int i = 0;  i < sc->num_fields; i++) {
		if (first) {
			first = false; 
		} else {
			printf(", "); 
		}
		switch(sc->types[i]) {
		case BOOLEAN:
			
			break;
	
		case CHARACTER: 
			printf("%c", *(char*)&row->data[sc->offsets[i]]);
			break;
 
		case TINYTEXT:
		case VARCHAR:
			printf("%s", (char*)&row->data[sc->offsets[i]]);
			break; 
		case INTEGER:
			printf("%d", *(int*)&row->data[sc->offsets[i]]);
			break; 		
		case PADDING:
			//skip padding fields
			continue;
		default: 
			printf("unknown type %d", sc->types[i]);
		}

	}
	printf("\n"); 
}

/* 
 * 
 * Scan the table... debug only 
 *
 */

int scan_table_dbg(table_t *table) {
	unsigned long i;
	row_t *row;

	printf("scan table:%s with %lu rows\n", 
		table->name.c_str(), table->num_rows); 

	row = (row_t *)malloc(row_size(table)); 
	if (!row) {
		ERR("can't allocate memory for the row\n");
		return -ENOMEM;
	}

	unsigned long long start, end; 
	start = RDTSC();

	for (i = 0; i < table->num_rows; i++) {
	
		/* Read one row. */
		read_row(table, i, row);

	}
	
	end = RDTSC();
	unsigned long long cycles = end - start;
	unsigned long long msecs = (cycles / cycles_per_msec);

	printf("Scanned table %s in %llu msecs (%llu cycles, %llu cycles per row)\n",
			table->name.c_str(), msecs, cycles, cycles/table->num_rows);

	return 0; 

}


/* Scan the table touching all rows */
int ecall_scan_table_dbg(int db_id, int table_id) {

	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)))
		return -1;

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	return scan_table_dbg(table); 	
}

/* 
 * 
 * Print the table... debug only 
 *
 */

int print_table_dbg(table_t *table, int start, int end) {
	unsigned long i;
	row_t *row;

	printf("printing table:%s with %lu rows (row size:%d) from %d to %d\n", 
		table->name.c_str(), table->num_rows, row_size(table), start, end); 

	row = (row_t *)calloc(row_size(table), 1);
	if (!row) {
		ERR("can't allocate memory for the row\n");
		return -ENOMEM;
	}

	if (end > table->num_rows) {
		end = table->num_rows; 
	}

	for (i = start; i < end; i++) {
	
		/* Read one row. */
		read_row(table, i, row);

		if (row->header.fake) {
			printf("FAKE\n");
			continue;
		}

		print_row(&table->sc, row); 

	}
	
	return 0; 

}


/* Print some number of rows from the database */
int ecall_print_table_dbg(int db_id, int table_id, int start, int end) {

	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)))
		return -1;
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	return print_table_dbg(table, start, end); 	
}

int project_schema(schema_t *old_sc, int* columns, int num_columns, schema_t *new_sc) {
    int j;
    int size = 0;
    for(int i = 0; i < num_columns; i++) {
        j = columns[i];
        new_sc->offsets[i] = old_sc->offsets[j];
        new_sc->sizes[i] = old_sc->sizes[j];
        size += new_sc->sizes[i];
        new_sc->types[i] = old_sc->types[j];
    }
    new_sc->num_fields = num_columns;
    new_sc->row_data_size = size;
    return 0;
}

int pad_schema(schema_t *old_sc, int num_pad_bytes, schema_t *new_sc)
{
	if (old_sc->num_fields == MAX_COLS)
		return -1;

	auto i = old_sc->num_fields;

	// copy old schema
	*new_sc = *old_sc;

	// pad only if padding is zero to avoid creating an extra padding field
	if (num_pad_bytes) {
		new_sc->offsets[i] = old_sc->row_data_size;
		new_sc->sizes[i] = num_pad_bytes;
		new_sc->types[i] = schema_type::PADDING;

		new_sc->num_fields = i + 1;
		new_sc->row_data_size = old_sc->row_data_size + num_pad_bytes;
	}
	return 0;
}

int project_row(row_t *old_row, schema_t *sc, row_t *new_row) {
	int offset = 0; 

	/* Copy the header */
	memcpy(new_row, old_row, row_header_size()); 
 
	for(int i = 0; i < sc->num_fields; i++) {
		memcpy((char *)new_row->data + offset, (char*)old_row->data + sc->offsets[i], sc->sizes[i]);
		offset += sc->sizes[i];
	}
	return 0; 
}

int project_promote_pad_table(
    data_base_t *db, 
    table_t *tbl, 
    int *project_columns, 
    int num_project_columns,
    int *promote_columns,
    int num_pad_bytes,
    table_t **p3_tbl,
	schema_t *p2_schema,
    schema_t *p3_schema
)
{
    int ret;
    std::string p3_tbl_name;
    schema_t project_sc, project_promote_sc, project_promote_pad_sc;
    row_t *row_old, *row_new, *row_new2;
    p3_tbl_name = "p3:" + tbl->name;

    ret = project_schema(&tbl->sc, 
                         project_columns, 
                         num_project_columns, 
                         &project_sc);
    if (ret) {
        ERR("project_schema failed:%d\n", ret);
        return ret;
    }
    ret = promote_schema(&project_sc,
                         promote_columns[0],
                         &project_promote_sc);
    if (ret) {
        ERR("promote_schema failed:%d\n", ret);
        return ret;
    }
    ret = pad_schema(&project_promote_sc,
                     num_pad_bytes,
                     &project_promote_pad_sc);
    if (ret) {
        ERR("pad_schema failed:%d\n", ret);
        return ret;
    }

    ret = create_table(db, p3_tbl_name, &project_promote_pad_sc, p3_tbl);
    if (ret) {
        ERR("create_table failed:%d\n", ret);
        return ret;
    }

    row_old = (row_t*) malloc(row_size(tbl));
    if(!row_old)
	    return -ENOMEM;

    row_new = (row_t*)malloc(row_size(*p3_tbl));
    if(!row_new)
	    return -ENOMEM;

    row_new2 = (row_t*)malloc(row_size(*p3_tbl));
    if(!row_new2)
	    return -ENOMEM;

    for (unsigned int i = 0; i < tbl->num_rows; i++) {
        // Read original row
        ret = read_row(tbl, i, row_old);
        if(ret) {
            ERR("read_row failed on row %d of table %s\n", i, tbl->name.c_str());
            goto cleanup;
        }

        // Project row
        // Since the schema was projected, promoted, and padded, this is all we need to do.
        ret = project_row(row_old, &project_promote_pad_sc, row_new);
        if(ret) {
            ERR("project_row failed on row %d of table %s\n", i, tbl->name.c_str());
            goto cleanup;
        }

		ret = promote_row(row_new, &project_promote_pad_sc, promote_columns[0], row_new2);
        if(ret) {
            ERR("project_row failed on row %d of table %s\n", i, tbl->name.c_str());
            goto cleanup;
        }

        // Add row to table
        ret = insert_row_dbg(*p3_tbl, row_new2);
        if(ret) {
            ERR("insert_row_db failed on row %d of table %s\n", i,
                (*p3_tbl)->name.c_str());
            goto cleanup;
        }
    }

    bflush(*p3_tbl);
	*p2_schema = project_promote_sc;
    *p3_schema = project_promote_pad_sc;
    ret = 0;

cleanup:
    if (row_old)
        free(row_old);

    if (row_new)
        free(row_new);

    if (row_new2)
	free(row_new2);

    return ret;
    
} 

/* Number of parameters -- needs improvement */
int ecall_merge_and_sort_and_write(int db_id, 
		int left_table_id, 
		int *project_columns_left, 
		int num_project_columns_left,
		int *promote_columns_left,
		int num_pad_bytes_left,
		int right_table_id, 
		int *project_columns_right, 
		int num_project_columns_right,
		int *promote_columns_right,
		int num_pad_bytes_right,
		int *write_table_id)
{

	int ret;

	table_t *p3_tbl_left, *p3_tbl_right, *append_table, *s_table;
	row_t *row_left = NULL, *row_right = NULL, *join_row = NULL;
	schema_t append_sc, join_sc, p3_left_schema, p3_right_schema, p2_left_schema, p2_right_schema;
	std::string append_table_name;  
	int append_table_id;

	unsigned long long start, end;
	unsigned long long cycles;
	double secs;

	data_base_t *db;
	if (!(db = get_db(db_id)))
		return -1;
	
	table_t* tbl_left = db->tables[left_table_id];
	table_t* tbl_right = db->tables[right_table_id];

	int join_columns_right[1] = {0};
	int num_join_columns_right = 1;

#if defined(REPORT_3P_APPEND_SORT_JOIN_WRITE_STATS)
	start = RDTSC();
#endif

	/* Assuming tables are coming from the same db? */
	if (! tbl_left || ! tbl_right)
		return -3; 

	/* Project promote pad R */
#if defined(REPORT_3P_STATS)
	start = RDTSC();
#endif
	ret = project_promote_pad_table(db, tbl_left, project_columns_left,
			num_project_columns_left, promote_columns_left,
			num_pad_bytes_left, &p3_tbl_left, &p2_left_schema, &p3_left_schema);

#if defined(REPORT_3P_STATS)
	end = RDTSC();

	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" Project Promote Pad R took %llu cycles (%f sec)\n", cycles, secs);
#endif

	print_table_dbg(p3_tbl_left, 0, 10);

	/* Project promote pad S */
#if defined(REPORT_3P_STATS)
	start = RDTSC();
#endif

	ret = project_promote_pad_table(db, tbl_right, project_columns_right,
			num_project_columns_right, promote_columns_right,
			num_pad_bytes_right, &p3_tbl_right, &p2_right_schema, &p3_right_schema);

#if defined(REPORT_3P_STATS)
	end = RDTSC();

	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" Project Promote Pad S took %llu cycles (%f sec)\n", cycles, secs);
#endif

	print_table_dbg(p3_tbl_right, 0, 10);
	
	row_left = (row_t *) calloc(row_size(p3_tbl_left), 1);
	if(!row_left)
		return -ENOMEM;

	row_right = (row_t *) calloc(row_size(p3_tbl_right), 1);
	if(!row_right)
		return -ENOMEM;

	/* Validate the size of row for each tablee */
	// Is this validation enough before appending?
	if( row_size(p3_tbl_left) != row_size(p3_tbl_right) )
		return -6;

	INFO(" Size of the left table AFTER 3P %d | row_size=%d\n", p3_tbl_left->num_rows, row_size(p3_tbl_left));
	INFO(" Size of the right table AFTER 3P %d | row_size=%d\n", p3_tbl_right->num_rows, row_size(p3_tbl_right));
	
	/* Append R and S */
	append_table_name = "append:" + tbl_left->name + tbl_right->name; 

	/* Is this the right way to create a schema to append two tables? */
	ret = join_schema(&append_sc, &p3_left_schema, &p3_right_schema);
	if (ret) {
		ERR("create table error:%d\n", ret);
		return ret; 
	}

	ret = create_table(db, append_table_name, &append_sc, &append_table);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	append_table_id = append_table->id;

	DBG(" Created append table %s, id:%d\n", append_table_name.c_str(), append_table_id); 

	// Read R row and append
#if defined(REPORT_APPEND_STATS)
	start = RDTSC();
#endif

	for(int i=0; i < p3_tbl_left->num_rows; i ++)
	{
		ret = read_row(p3_tbl_left, i, row_left);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, tbl_left->name.c_str());
			goto cleanup;
		}

		/* Add left row to the append table */
		ret = insert_row_dbg(append_table, row_left);
		if(ret) {
			ERR("failed to append row %d of table %s to %s table\n",
				i, p3_tbl_left->name.c_str(), append_table->name.c_str());
			goto cleanup;
		}
	}

#if defined(REPORT_APPEND_STATS)
	end = RDTSC();

	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" Append R took %llu cycles (%f sec)\n", cycles, secs);
#endif

	// READ S first then R 
#if defined(REPORT_APPEND_STATS)
	start = RDTSC();
#endif

	// Read S row and append	
	for (unsigned int j = 0; j < p3_tbl_right->num_rows; j ++) {

		// Read right row
		ret = read_row(p3_tbl_right, j, row_right);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				j, p3_tbl_right->name.c_str());
			goto cleanup;
		}

		/* Add row to the append table */
		ret = insert_row_dbg(append_table, row_right);
		if(ret) {
			ERR("failed to append row %d of table to %s table\n",
				j, p3_tbl_right->name.c_str(), append_table->name.c_str());
			goto cleanup;
		}
	}

	print_table_dbg(append_table, 0, 29);
	
#if defined(REPORT_APPEND_STATS)
	end = RDTSC();

	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" Append S took %llu cycles (%f sec)\n", cycles, secs);
#endif

//// 1 THREAD SERIEAL

	INFO(" Size of the table BEFORE sorting table %d", append_table->num_rows);

	// Sort: 1) bitonic or 2) quick
	/* Which field to sort? */
	int field;
	field = 0;

#if defined(REPORT_SORT_STATS)
	start = RDTSC();
#endif
	// Refer to parallelization and update - column_sort_table_parallel();
	//ret = column_sort_table(db, append_table, field);
	//ret = bitonic_sort_table(db, append_table, field, &s_table);
	ret = quick_sort_table(db, append_table, field, &s_table);
	if(ret) {
		ERR("failed to bitonic sort table %s\n",
			append_table->name.c_str());
		goto cleanup;
	}

	print_table_dbg(append_table, 0, 29);

#if defined(REPORT_SORT_STATS)
	end = RDTSC();
	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" Sorting merged table took %llu cycles (%f sec)\n", cycles, secs);
#endif

	/* Later remove join condition - each row has the info where it came from */
	join_condition_t c;
	c.table_left = p3_tbl_left->id;
	c.table_right = p3_tbl_right->id;
	c.max_joinability = 5;
	c.num_conditions = 1;
	c.fields_left[0] = 0;
	c.fields_right[0] = 0;
#if defined(REPORT_JOIN_WRITE_STATS)
	start = RDTSC();
#endif	

	INFO(" Size of the table BEFORE writing sorted table %d\n", append_table->num_rows);

	append_table->num_rows = tbl_left->num_rows + tbl_right->num_rows;

	/* Is this the right way to create a schema to append two tables? */
	ret = join_schema_algo(&join_sc, &p2_left_schema, &p2_right_schema, join_columns_right, 
    		num_join_columns_right);
	if (ret) {
		ERR("create table error:%d\n", ret);
		return ret; 
	}

	print_schema(&join_sc, "join_schema");

	// Join and write sorted table
	ret = join_and_write_sorted_table( db, append_table, &c, &join_sc, write_table_id );
	if(ret) {
		ERR("failed to join and write sorted table %s\n",
			append_table->name.c_str());
		goto cleanup;
	}

	INFO(" Finished appended table writing \n");
	
#if defined(REPORT_JOIN_WRITE_STATS)
	end = RDTSC();
	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" Join and write sorted table took %llu cycles (%f sec)\n", cycles, secs);
#endif
	ret = 0;

cleanup:
	if (row_left)
		free(row_left); 

	if (row_right)
		free(row_right); 

#if defined(REPORT_3P_APPEND_SORT_JOIN_WRITE_STATS)
	end = RDTSC();
	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" 3p, append, sort, join and write sorted table took %llu cycles (%f sec)\n", cycles, secs);
#endif
	return ret; 
}

/* Later replace db_id with db */
int join_and_write_sorted_table(data_base_t *db, table_t *tbl, join_condition_t *c, 
	schema_t* join_sc, int *join_table_id)
{
	int ret;
	table_t *tbl_left, *tbl_right, *join_table;
	row_t *row_left = NULL, *row_right = NULL, *join_row = NULL;
	std::string join_table_name;  

	if (!c)	
		return -1; 

	if(!db)
		return -2;

	tbl_left = db->tables[c->table_left];
	tbl_right = db->tables[c->table_right];

	if (!tbl_left || ! tbl_right)
		return -3; 

	/* To create a join table with combination of names */
	join_table_name = "join:" + tbl_left->name + tbl_right->name; 

	ret = create_table(db, join_table_name, join_sc, &join_table);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	*join_table_id = join_table->id; 

	DBG("Created join table %s, id:%d\n", join_table_name.c_str(), *join_table_id); 

	join_row = (row_t *) calloc(row_size(tbl_left) + row_size(tbl_right), 1);
	if (!join_row)
		return -ENOMEM;

	row_left = (row_t *) calloc(std::max(row_size(tbl_left), row_size(tbl_right)), 1);
	if(!row_left)
		return -ENOMEM;

	row_right = (row_t *) calloc(std::max(row_size(tbl_left), row_size(tbl_right)), 1);
	if(!row_right)
		return -ENOMEM;

	unsigned int size = tbl->num_rows;
	unsigned int joinability = c->max_joinability;
	
	// We cannot join the last row with anything
	for (auto i = 0; i < size - 1; i++) {
		unsigned int start = i + 1;

		// Read left row
		ret = read_row(tbl, i, row_left);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, tbl_left->name.c_str());
			goto cleanup;
		}

		// Read right row
		for (auto j = start; j < (start + joinability) && j < size; j++) {
			bool equal = true;
			ret = read_row(tbl, j, row_right);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					j, tbl_right->name.c_str());
				goto cleanup;
			}

			// If row_left and row_right came from the same table, add a fake row 
			if (row_left->header.from == row_right->header.from) {
				join_row->header.fake = true; 

				// Add a fake row to the join table
				ret = insert_row_dbg(join_table, join_row);
				if(ret) {
					ERR("failed to insert fake row %d of table %s with row %d of table %s\n",
						i, tbl_left->name.c_str(), j, tbl_right->name.c_str());
					goto cleanup;
				}
			} else {
				// Else if left_row and right_row came from different table, perform real join
				for (auto k = 0; k < c->num_conditions; k++) {
					DBG_ON(JOIN_VERBOSE, "comparing (i:%d, j:%d, k:%d\n", i, j, k);
					equal &= cmp_row(tbl_left, row_left, c->fields_left[k], tbl_right, row_right, c->fields_right[k]);
				}

				if (equal) {
					DBG_ON(JOIN_VERBOSE, "joining (i:%d, from:%d) with (j:%d, from:%d)\n",
							i, row_left->header.from,
							j, row_right->header.from);

					if (row_left->header.from) {
						// if from is '1' then the row is from table S and it joins with a row in table R
						// So, copy row_right first followed by row_left.
						// TODO: This should always be mandated when creating tables.
						ret = _join_rows(join_row,
								join_sc->row_data_size,
								row_right,
								&tbl_left->sc,
								row_left,
								&tbl_right->sc);
					} else {
						// if from is '0' then the row is from table R and it joins with a row in table S
						// So, copy row_left first followed by row_right.
						ret = _join_rows(join_row,
								join_sc->row_data_size,
								row_left,
								&tbl_left->sc,
								row_right,
								&tbl_right->sc);
					}

					if (ret) {
						ERR("failed to produce a joined row %d of table %s with row %d of table %s\n",
							i, tbl_left->name.c_str(), j, tbl_right->name.c_str());
						goto cleanup;
					}
				
					// Add row to the join 
					ret = insert_row_dbg(join_table, join_row);
					if (ret) {
						ERR("failed to join row %d of table %s with row %d of table %s\n",
							i, tbl_left->name.c_str(), j, tbl_right->name.c_str());
						goto cleanup;
					}
				} else {
					// if not equal write a fake row
					join_row->header.fake = true;

					// Add a fake row to the join table
					ret = insert_row_dbg(join_table, join_row);
					if (ret) {
						ERR("failed to insert fake row %d of table %s with row %d of table %s\n",
							i, tbl_left->name.c_str(), j, tbl_right->name.c_str());
						goto cleanup;
					}
				}
			}	// if left_row and right_row came from different table, perform real join
		}
	}

	bflush(join_table);

	print_table_dbg(join_table, 0, 135);
	INFO(" Finished writing the table\n");
	ret = 0;

cleanup:
	if (join_row)
		free(join_row);

	if (row_left)
		free(row_left);

	if (row_right)
		free(row_right);

	return ret;
}
