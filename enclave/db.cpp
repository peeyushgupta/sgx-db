#include "db.hpp"
#include "time.hpp"
#include "obli.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#include "bcache.hpp"

#include <cstdlib>
#include <cstdio>

#include <string.h>
#include <atomic>
//#define FILE_READ_SIZE (1 << 12)

#define FILE_READ_SIZE DATA_BLOCK_SIZE

#define OCALL_VERBOSE 0
#define JOIN_VERBOSE 0
#define COLUMNSORT_VERBOSE 1
#define IO_VERBOSE 0

data_base_t* g_dbs[MAX_DATABASES];

const int ASCENDING  = 1;
const int DESCENDING = 0;

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


#if 0



s64 varchar_cmp(u8 *va, u8 *vb, u64 max)
{
  u64 la = *((u64 *)va), lb = *((u64 *)vb);
  s64 oblires = obli_cmp64(la, lb);
  s64 out = obli_varcmp(&va[8], &vb[8], obli_cmov64(la, lb, oblires, 1), max);
  return obli_cmov64(out, oblires, out, 0);
}
s64 str_cmp(u8 *s1, u8 *s2, u64 len)
{
  return obli_strcmp(s1, s2, len);
}
s64 varbinary_cmp(u8 *va, u8 *vb, u64 max)
{
  u64 la = (7+*((u64 *)va))/8, lb = (7+*((u64 *)vb))/8;
  s64 oblires = obli_cmp64(la, lb);
  s64 out = obli_varcmp(&va[8], &vb[8], obli_cmov64(la, lb, oblires, 1), max);
  return obli_cmov64(out, oblires, out, 0);
}
u64 col_len(Column *col)
{
  u64 ret;
  switch (col[0].ty)
  {
    case BOOLEAN:
    {
      ret= 1;
    }
    case BINARY:
    {
      ret= (col[0].buf.len + 7) / 8;
    }
    case VARBINARY:
    {
      ret= 8 + ((col[0].buf.len + 7) / 8);
    }
    case CHARACTER:
    {
      ret= col[0].buf.len;
    }
    case VARCHAR:
    {
      ret= 8 + col[0].buf.len;
    }
    case DECIMAL:
    {
      ret= (col[0].dec.decimal + col[0].dec.integral + 7) / 8;
    }
  }
  return ret;
}
u64 schema_rowlen(Schema *tbl)
{
  u64 cur = 0;
  for (u64 i = 0; i < MAX_COLS; i++)
  {
    s64 res = obli_cmp64(i, tbl->lenRow);
    res -=1;
    res*=-1;
    res= res>>1;
    cur += col_len(&tbl->schema[i])*res;

  }
  return cur;
}
u64 schema_rowoffs(Schema *tbl, u64 num)
{
  u64 cur = 0;
  for (auto i = 0; i < MAX_COLS; i++)
  {
    s64 res = obli_cmp64(i, num);
    res -=1;
    res*=-1;
    res= res>>1;
    cur += col_len(&tbl->schema[i])*res;
  }
  return cur;
}

template <typename T>
void db_tbl_join(void * blkIn, void * blkOut, u64 t1, u64 c1, u64 t2, u64 c2, T cb)
{
  Column col1, col2;
  u64 off1 = 0, off2 = 0;
  for(u64 i = 0;i<MAX_TABLES;i++){
    Schema * cur = g_tbl[i].sc;
    s64 tind1 = obli_cmp64(t1, i);
    s64 tind2 = obli_cmp64(t2, i);
    for (u64 j = 0; j < MAX_COLS; j++)
    {
      s64 cind1 = obli_cmp64(j, c1), cind2 = obli_cmp64(j, c2);
      s64 eq1 = (cind1*cind1)+(tind1*tind1), eq2 = (cind2*cind2)+(tind2*tind2); 
      obli_cmov(&col1,&cur->schema[j],sizeof(Column), eq1, 0);
      obli_cmov(&col2,&cur->schema[j],sizeof(Column), eq2, 0);
      s64 lt1 = 1-((-1*(cind1-1))>>1), lt2 = 1-((-1*(cind2-1))>>1);
      lt1 = (lt1*lt1)+(tind1*tind1);
      lt2 = (lt2*lt2)+(tind2*tind2); 
      off1+=obli_cmov64(col_len(&cur->schema[j]), 0, lt1, 0);
      off2+=obli_cmov64(col_len(&cur->schema[j]), 0, lt2, 0);
      
    }
  }
}

#endif

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
		db->bcache.data_blks[i].data = malloc(DATA_BLOCK_SIZE);
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
		free(db->bcache.data_blks[j].data);
		db->bcache.data_blks[j].data = NULL; 
	};
	return -1; 
};

/* Free data blocks in enclave's memory */
void free_data_blocks(data_base_t *db) {
	for (int i = 0; i < DATA_BLKS_PER_DB; i++ ) {
		if(db->bcache.data_blks[i].data) {
			free(db->bcache.data_blks[i].data);
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

	unsigned long long start, end; 
	start = RDTSC();
	
	ocall_seek(&ret, table->fd, blk_num*DATA_BLOCK_SIZE); 
	
	end = RDTSC();
	DBG_ON(IO_VERBOSE, "ocall_seek: %llu cycles\n", end - start);

	start = RDTSC();

	while (total_read < DATA_BLOCK_SIZE) { 
		ocall_read_file(&read, table->fd, 
				table->db->io_buf[tid()], 
				FILE_READ_SIZE);
		if (read < 0) {
			ERR("read filed\n"); 
			return -1; 
		}

		if (read == 0) {
			/* We've reached the end of file, pad with zeroes */
			read = DATA_BLOCK_SIZE - total_read; 
			memset((void *)((char *)buf + total_read), 0, read); 
			return 0; 
		} else {
			/* Copy data from the I/O buffer into bcache buffer */
			memcpy((void *)((char *)buf + total_read), table->db->io_buf[tid()], read); 
		}
		total_read += read;  
	}

	end = RDTSC();
	DBG_ON(IO_VERBOSE, 
		"ocall_read_file: %llu cycles\n", end - start);

	return 0; 
}

/* Write data block from enclave's memory back to disk. 
 * For temporary results, we'll create temporary tables that will 
 * have corresponding encryption keys (huh?)
*/
int write_data_block(table *table, unsigned long blk_num, void *buf) {
	unsigned long long total_written = 0, write_size; 
	int written, ret; 

	ocall_seek(&ret, table->fd, blk_num*DATA_BLOCK_SIZE);
 
	while (total_written < DATA_BLOCK_SIZE) { 
		/* make sure we don't write more than DATA_BLOCK_SIZE */
		write_size = (total_written + FILE_READ_SIZE) <= DATA_BLOCK_SIZE ? 
			FILE_READ_SIZE : DATA_BLOCK_SIZE - total_written;  

		/* Copy data into the I/O buffer */
		memcpy(table->db->io_buf[tid()], 
			(void *)((char *)buf + total_written), write_size); 
		/* Submit I/O buffer */
		ocall_write_file(&written, table->fd, 
			table->db->io_buf[tid()], 
			write_size);
		if (written < 0) {
			ERR("write filed\n"); 
			return -1; 
		} 
		total_written += written;  
	}
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
		return -3;	
	
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
		return -4;

	db->tables[i] = table;
	table->id = i; 

	table->name = name;
	table->num_rows = 0; 
	table->num_blks = 0; 
	table->sc = *schema;
	table->db = db; 	

	/* Call outside of enclave to open a file for the table */
	sgx_ret = ocall_open_file(&fd, name.c_str());
	if (sgx_ret || fd < 0) {
		ret = -5;
		goto cleanup; 
	} 

	table->fd = fd;

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

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] 
	   || !schema || !table_id || !cname || (name_len == 0))	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

	ret = create_table(db, name, schema, &table); 
	if (ret) {
		ERR("failed to create table %s, ret:%d\n", cname, ret); 
		return ret; 
	}

	*table_id = table->id; 
	return 0; 
};

/* Flush all data to disk */
int ecall_flush_table(int db_id, int table_id) {
	data_base_t *db;
	table_t *table;

	if (db_id > (MAX_DATABASES - 1))	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];
	
	bflush(table);
	return 0;
	

};

/* Insert one row */
int ecall_insert_row(int db_id, int table_id, void *row) {

   /* An oblivious version of row insert will update all 
      rows in the table to conceal its size */
	return -1; 
}

void *get_column(schema_t *sc, int field, void *row) {
	//printf("%s: row:%p, field:%d, offset:%d\n", __func__, row, field, sc->offsets[field]); 
	return(void*)((char*)row + sc->offsets[field]);
}

bool cmp_row(table_t *tbl_left, void *row_left, int field_left, table_t *tbl_right, void *row_right, int field_right) {

	if(tbl_left->sc.types[field_left] != tbl_right->sc.types[field_right])
		return false;
	switch (tbl_left->sc.types[field_left]) {
	case BOOLEAN: 
		return (*((bool*)get_column(&tbl_left->sc, field_left, row_left)) 
			== *((bool*)get_column(&tbl_right->sc, field_right, row_right))); 
	case INTEGER: 
		return (*((int*)get_column(&tbl_left->sc, field_left, row_left)) 
			== *((int*)get_column(&tbl_right->sc, field_right, row_right))); 
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
		sc->offsets[i + left->num_fields] = left->row_size + right->offsets[i];
		sc->sizes[i + left->num_fields] = right->sizes[i];
		sc->types[i + left->num_fields] = right->types[i];	
	}
	sc->row_size = sc->offsets[sc->num_fields - 1] + sc->sizes[sc->num_fields - 1];

	return 0;
}


/* Read one row. */
int read_row(table_t *table, unsigned int row_num, void *row) {

	unsigned long dblk_num;
	unsigned long row_off, rows_per_blk; 
	data_block_t *b;

	if(row_num >= table->num_rows)
		return -1; 

	rows_per_blk = DATA_BLOCK_SIZE / table->sc.row_size; 
	dblk_num = row_num / rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (row_num - dblk_num * rows_per_blk) * table->sc.row_size; 
	
        b = bread(table, dblk_num);
	 	
	/* Copy the row into the data block */
	memcpy(row, (char*)b->data + row_off, table->sc.row_size); 

	brelse(b);
	return 0; 
}

int join_rows(void *join_row, unsigned int join_row_size, void * row_left, unsigned int row_left_size, void * row_right, unsigned int row_right_size) {

	if(row_left_size + row_right_size > join_row_size)
		return -1; 
	memcpy(join_row, row_left, row_left_size); 
	memcpy((void*)((char*)join_row + row_left_size),row_right, row_right_size);
	return 0; 
}; 



/* Join */
int ecall_join(int db_id, join_condition_t *c, int *join_table_id) {
	int ret;
	data_base_t *db;
	table_t *tbl_left, *tbl_right, *join_table;
	void *row_left = NULL, *row_right = NULL, *join_row = NULL;
	schema_t join_sc;
	std::string join_table_name;  

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] || !c )	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

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

	join_row = malloc(MAX_ROW_SIZE*2);
	if(!join_row)
		return -4;

	row_left = malloc(MAX_ROW_SIZE);
	if(!row_left)
		return -5;

	row_right = malloc(MAX_ROW_SIZE);
	if(!row_right)
		return -6;

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
				DBG("comparing (i:%d, j:%d, k:%d\n", i, j, k); 

				eq = cmp_row(tbl_left, row_left, c->fields_left[k], tbl_right, row_right, c->fields_right[k]);
				if (!eq) {
					equal = eq; 
				}

			}

			if (equal) { 
				DBG("joining (i:%d, j:%d)\n", i, j); 

				ret = join_rows(join_row, join_sc.row_size, row_left, tbl_left->sc.row_size, row_right, tbl_right->sc.row_size); 
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
        new_sc->row_size = old_sc->row_size; 

	return 0;
}

int promote_row(void *old_row, schema_t *sc, int column, void * new_row) {
       
	memcpy(new_row, (char*)old_row + sc->offsets[column], sc->sizes[column]); 
	memcpy((char*)new_row + sc->sizes[column], old_row, sc->offsets[column]); 
	memcpy((char*)new_row + sc->sizes[column] + sc->offsets[column], 
		(char*)old_row + sc->offsets[column] + sc->sizes[column], 
		sc->row_size - (sc->offsets[column] + sc->sizes[column]));
	
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
	void *row_old, *row_new; 

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

	row_old = malloc(MAX_ROW_SIZE);
	if(!row_old)
		return -5;

	row_new = malloc(MAX_ROW_SIZE);
	if(!row_new)
		return -6;

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

/* r -- number of rows
   s -- number of columns
*/

int column_sort_table(data_base_t *db, table_t *table, int r, int s, int column) {
	int ret;
	std::string tmp_tbl_name;  
	table_t **s_tables, **st_tables, *tmp_table;
	void *row;
	unsigned long row_num;  
	unsigned long shift, unshift;

	if( r % s != 0) {
		ERR("r (%d) is not divisible by s (%d)\n", r, s);
		return -1;
	}

	if ( r < 2*(s - 1)*(s - 1)) {
		ERR("r (%d) is < 2*(s - 1)^2 (%d), where r=%d, s=%d\n", 
			r, 2*(s - 1)*(s - 1), r, s);  
		return -1; 
	}
 
	s_tables = (table_t **)malloc(s * sizeof(table_t *)); 
	if(!s_tables) {
		ERR("failed to allocate s_tables\n");
		goto cleanup; 
	}

	st_tables = (table_t **)malloc(s * sizeof(table_t *)); 
	if(!st_tables) {
		ERR("failed to allocate s_tables\n");
		goto cleanup; 
	}

	/* Create s temporary column tables */
	for (int i = 0; i < s; i++) {

		tmp_tbl_name = "s" + std::to_string(i) + ":" + table->name ; 

		ret = create_table(db, tmp_tbl_name, &table->sc, &s_tables[i]);
		if (ret) {
			ERR("create table:%d\n", ret);
			goto cleanup; 
		}

		DBG("Created tmp table %s, id:%d\n", 
            		tmp_tbl_name.c_str(), s_tables[i]->id); 
	}

	/* Create another set of s transposed column tables */
	for (int i = 0; i < s; i++) {

		tmp_tbl_name = "st" + std::to_string(i) + ":" + table->name ; 

		ret = create_table(db, tmp_tbl_name, &table->sc, &st_tables[i]);
		if (ret) {
			ERR("create table:%d\n", ret);
			goto cleanup; 
		}

		DBG("Created tmp table %s, id:%d\n", 
            		tmp_tbl_name.c_str(), st_tables[i]->id); 
	}

	row = malloc(MAX_ROW_SIZE);
	if(!row)
		goto cleanup;

	if ( r * s > table->num_rows ) {
		ERR("r (%d) * s (%d) > num_rows (%d)\n", r, s, table->num_rows); 
		goto cleanup; 
	}

	row_num = 0; 

	/* Rewrite the table as s column tables  */
	for (unsigned int i = 0; i < s; i ++) {

		for (unsigned int j = 0; j < r; j ++) {

			// Read old row
			ret = read_row(table, row_num, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, table->name.c_str());
				goto cleanup;
			}

				
			/* Add row to the s table */
			ret = insert_row_dbg(s_tables[i], row);
			if(ret) {
				ERR("failed to insert row %d of column table %s\n",
					row_num, s_tables[i]->name.c_str());
				goto cleanup;
			}
			row_num ++;
		}
	}

	
#if defined(COLUMNSORT_DBG)
	printf("Column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
	}
#endif

	for (unsigned int i = 0; i < s; i++) {
		bitonic_sort_table(db, s_tables[i], column, &tmp_table);
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 1: Sorted column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
	}
#endif

	/* Transpose s column tables into s transposed tables  */
	for (unsigned int i = 0; i < s; i ++) {

		for (unsigned int j = 0; j < r; j ++) {

			// Read old row
			ret = read_row(s_tables[i], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					j, s_tables[i]->name.c_str());
				goto cleanup;
			}

				
			/* Add row to the st table */
			//DBG_ON(COLUMNSORT_VERBOSE,
			//	"insert row %d of transposed table #%d (%s)\n", 
			//	st_tables[j % s]->num_rows, j % s, 
			//	st_tables[j % s]->name.c_str()); 

			ret = insert_row_dbg(st_tables[j % s], row);
			if(ret) {
				ERR("failed to insert row %d of transposed column table %s\n",
					j, st_tables[j % s]->name.c_str());
				goto cleanup;
			}
		}
	}


#if defined(COLUMNSORT_DBG)
	printf("Step 2: Transposed column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
	}
#endif

	for (unsigned int i = 0; i < s; i++) {
		bitonic_sort_table(db, st_tables[i], column, &tmp_table);
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 3: Sorted transposed column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
	}
#endif
	row_num = 0; 

	/* Untranspose st transposed column tables into s tables  */
	for (unsigned int i = 0; i < r; i ++) {

		for (unsigned int j = 0; j < s; j ++) {

			/* Read row from st table */
			ret = read_row(st_tables[j], i, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					i, st_tables[j]->name.c_str());
				goto cleanup;
			}

				
			/* Add row to the s table */
			ret = write_row_dbg(s_tables[row_num / r], row, row_num % r);
			if(ret) {
				ERR("failed to insert row %d of untransposed column table %s\n",
					row_num / r, s_tables[i]->name.c_str());
				goto cleanup;
			}
			row_num ++; 
		}
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 4: Untransposed column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
	}
#endif

	for (unsigned int i = 0; i < s; i++) {
		bitonic_sort_table(db, s_tables[i], column, &tmp_table);
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 5: Sorted untransposed column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
	}
#endif

	shift = r / 2 ;
	row_num = 0;  
	
	/* Shift s tables into st tables  */
	for (unsigned int i = 0; i < s; i ++) {

		for (unsigned int j = 0; j < r; j ++) {

			/* Read row from s table */
			ret = read_row(s_tables[i], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, s_tables[i]->name.c_str());
				goto cleanup;
			}

			/* Add row to the st table */
			//DBG_ON(COLUMNSORT_VERBOSE,
			//	"insert row %d of shifted table (%s) at row %d, row_num:%d, shitf:%d\n", 
			//	j, st_tables[((row_num + shift) / r) % s]->name.c_str(), 
			//	(row_num + shift) % r, row_num, shift); 


				
			/* Add row to the st table */
			ret = write_row_dbg(st_tables[((row_num + shift) / r) % s], 
						row, (row_num + shift) % r);
			if(ret) {
				ERR("failed to insert row %d of shifted column table %s\n",
					row, st_tables[i]->name.c_str());
				goto cleanup;
			}
			row_num ++;
		}
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 6: Shifted column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
	}
#endif
	for (unsigned int i = 0; i < s; i++) {
		bitonic_sort_table(db, st_tables[i], column, &tmp_table);
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 7: Sorted shifted column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
	}
#endif

	row_num = 0;  
	
	/* Unshift st tables into s tables  */

	/* In Shantanu's implementation (no +/-infinity) the first column
           is special --- it's sorted so instead of shifting we have to 
           splice it: first half of the column stays in it's place, the 
	   second half (the max elements) go to the last column */

	unshift = r - r / 2; 	
	for (unsigned int j = 0; j < unshift; j ++) {

		/* Read row from st table */
		ret = read_row(st_tables[0], j, row);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				row_num, st_tables[0]->name.c_str());
			goto cleanup;
		}

		/* Add row to the s table */
		ret = write_row_dbg(s_tables[0], row, j);
		if(ret) {
			ERR("failed to insert row %d of unshifted column table %s\n",
				row, s_tables[0]->name.c_str());
			goto cleanup;
		}
		row_num ++;
	}

	for (unsigned int j = unshift; j < s; j ++) {

		/* Read row from st table */
		ret = read_row(st_tables[0], j, row);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				row_num, st_tables[0]->name.c_str());
			goto cleanup;
		}

		/* Add row to the s table */
		ret = write_row_dbg(s_tables[s - 1], row, j);
		if(ret) {
			ERR("failed to insert row %d of unshifted column table %s\n",
				row, s_tables[s - 1]->name.c_str());
			goto cleanup;
		}
		row_num ++;
	}

	/* Now shift the rest of the table */
	for (unsigned int i = 1; i < s; i ++) {

		for (unsigned int j = 0; j < r; j ++) {

			/* Read row from st table */
			ret = read_row(st_tables[i], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, st_tables[i]->name.c_str());
				goto cleanup;
			}

			/* Add row to the st table */
			DBG_ON(COLUMNSORT_VERBOSE,
				"insert row %d of unshifted table (%s) at row %d, row_num:%d, shitf:%d\n", 
				j, s_tables[((row_num + (r * s) - shift) / r) % s]->name.c_str(), 
				(row_num + (r * s) - shift) % r, row_num, shift); 


				
			/* Add row to the s table */
			ret = write_row_dbg(s_tables[((row_num + (r * s) - shift) / r) % s], 
						row, (row_num + (r * s) - shift) % r);
			if(ret) {
				ERR("failed to insert row %d of unshifted column table %s\n",
					row, s_tables[i]->name.c_str());
				goto cleanup;
			}
			row_num ++;
		}
	}

#if defined(COLUMNSORT_DBG)
	printf("Step 8: Unshifted column tables\n");
	for (unsigned int i = 0; i < s; i++) {
		print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
	}
#endif

	row_num = 0;  
	
	/* Write sorted table back  */
	for (unsigned int i = 0; i < s; i ++) {

		for (unsigned int j = 0; j < r; j ++) {

			/* Read row from s table */
			ret = read_row(s_tables[i], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, s_tables[i]->name.c_str());
				goto cleanup;
			}

			/* Add row to the st table */
			ret = write_row_dbg(table, row, row_num);
			if(ret) {
				ERR("failed to insert row %d of sorted table %s\n",
					row_num, table->name.c_str());
				goto cleanup;
			}
			row_num ++;
		}
	}

#if defined(COLUMNSORT_DBG)
	printf("Sorted table\n");
	print_table_dbg(table, 0, table->num_rows);
#endif

	ret = 0;
cleanup: 
	if (row)
		free(row); 

	if (s_tables) {
		for (unsigned int i = 0; i < s; i++) {
			if (s_tables[i]) {
				bflush(s_tables[i]);
				free(s_tables[i]);
			}
		}
		free(s_tables);
	};

	if (st_tables) {
		for (unsigned int i = 0; i < s; i++) {
			if (st_tables[i]) {
				bflush(st_tables[i]);
				free(st_tables[i]);
			}
		}
		free(st_tables);
	};
		
	return ret; 
};




/// Bitonic sort functions ////
/** INLINE procedure exchange() : pair swap **/
inline int exchange(table_t *tbl, int i, int j, void *row_i, void *row_j) {
  void *row_tmp;

  row_tmp = malloc(MAX_ROW_SIZE);

  if(!row_tmp)
    return -4;

  memcpy(row_tmp, row_i, MAX_ROW_SIZE); 

  write_row_dbg(tbl, row_j, i);
  write_row_dbg(tbl, row_tmp, j);

  free(row_tmp);

  return 0;
}

/* returns ture if column of row_l is found respectively to be greater then column of row_r */

bool compare_rows(schema_t *sc, int column, void *row_l, void *row_r) {
	bool res; 
	switch(sc->types[column]) {
	case BOOLEAN:
		res = *(bool*)((char*)row_l + sc->offsets[column]) >
			*(bool*)((char*)row_r + sc->offsets[column]); 
		break;
	
	case CHARACTER: 
		res = *(char*)((char*)row_l + sc->offsets[column]) >
			*(char*)((char*)row_r + sc->offsets[column]); 
		break;
 
	case TINYTEXT:
	case VARCHAR: {
		int str_ret = strcmp ((char*)((char*)row_l + sc->offsets[column]), 
			(char*)((char*)row_r + sc->offsets[column])) ;
		res = (str_ret > 0);
		break; 
	}
	case INTEGER:
		res = *(int*)((char*)row_l + sc->offsets[column]) > 
			*(int*)((char*)row_r + sc->offsets[column]); 

		break; 		
	default: 
		res = false;
	}
	return res;
}

/** procedure compare() 
   The parameter dir indicates the sorting direction, ASCENDING 
   or DESCENDING; if (a[i] > a[j]) agrees with the direction, 
   then a[i] and a[j] are interchanged.
**/
int compare(table_t *tbl, int column, int i, int j, int dir) {
	void *row_i, *row_j;
	int val_i, val_j;

	row_i = malloc(MAX_ROW_SIZE);
	if(!row_i)
		return -5;

	row_j = malloc(MAX_ROW_SIZE);
	if(!row_j)
		return -6;

	read_row(tbl, i, (void *)row_i);
	read_row(tbl, j, (void *)row_j);

	if (dir == compare_rows(&tbl->sc, column, row_i, row_j)) {
		exchange(tbl, i, j, row_i, row_j);
	}

	free(row_i);
	free(row_j);
	return 0;
}



/** Procedure bitonicMerge() 
   It recursively sorts a bitonic sequence in ascending order, 
   if dir = ASCENDING, and in descending order otherwise. 
   The sequence to be sorted starts at index position lo,
   the parameter cbt is the number of elements to be sorted. 
 **/
void bitonicMerge(table_t *tbl, int lo, int column, int cnt, int dir) {
  if (cnt>1) {
    int k=cnt/2;
    int i;
    for (i=lo; i<lo+k; i++)
      compare(tbl, column, i, i+k, dir);
    bitonicMerge(tbl, lo, column, k, dir);
    bitonicMerge(tbl, lo+k, column, k, dir);
  }
}



/** function recBitonicSort() 
    first produces a bitonic sequence by recursively sorting 
    its two halves in opposite sorting orders, and then
    calls bitonicMerge to make them in the same order 
 **/
void recBitonicSort(table_t *tbl, int lo, int column, int cnt, int dir) {
  if (cnt>1) {
    int k=cnt/2;
    recBitonicSort(tbl, lo, column, k, ASCENDING);
    recBitonicSort(tbl, lo+k, column, k, DESCENDING);
    bitonicMerge(tbl, lo, column, cnt, dir);
  }
}


int bitonic_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl) {
	int ret = 0;

#ifdef CREATE_SORTED_TABLE
	std::string s_tbl_name;  
        schema_t p_sc;

	s_tbl_name = "s:" + tbl->name; 

	ret = create_table(db, s_tbl_name, &p_sc, p_tbl);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	DBG("Created sorted table %s, id:%d\n", 
            s_tbl_name.c_str(), s_tbl->id); 
#endif
	recBitonicSort(tbl, 0, column, tbl->num_rows, ASCENDING);

#ifdef CREATE_SORTED_TABLE
	bflush(*p_tbl);
#endif
	return ret; 
}

int ecall_sort_table(int db_id, int table_id, int field, int *sorted_id) {
	int ret; 
	data_base_t *db;
	table_t *table, *s_table;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )
		return -1; 

	db = g_dbs[db_id]; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = bitonic_sort_table(db, table, field, &s_table); 

#ifdef CREATE_SORTED_TABLE
	*sorted_id = s_table->id; 
#endif
	return ret; 
}

int print_table_dbg(table_t *table, int start, int end);

int bitonicSplit(table_t *tbl, int start_i, int start_j, int end, int column)
{
	for (int i = start_i, j = start_j; i < end; i++, j++) {
		void *row_i, *row_j;
		int val_i, val_j;

		row_i = malloc(MAX_ROW_SIZE);
		if(!row_i)
			return -5;

		row_j = malloc(MAX_ROW_SIZE);

		if(!row_j)
			return -6;

		read_row(tbl, i, (void *)row_i);
		read_row(tbl, j, (void *)row_j);

		val_i = *((int*)get_column(&tbl->sc, column, row_i));
		val_j = *((int*)get_column(&tbl->sc, column, row_j));

		if (val_i > val_j)
			exchange(tbl, i,j, row_i, row_j);

		free(row_i);
		free(row_j);
	}
	return 0;
}
std::atomic_uint stage1, stage2, stage3;
int ecall_sort_table_parallel(int db_id, int table_id, int column, int tid)
{
  int ret;
  data_base_t *db;
  table_t *table;

  if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id])
    return -1;

  db = g_dbs[db_id];

  if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
    return -2;

  table = db->tables[table_id];
  // printf("%s, num_rows %d | tid = %d\n", __func__, table->num_rows, tid);

  // stage 1. Split it into the number of threads and alternate between ascending and descending
  if (tid == 0)
  {
    recBitonicSort(table, 0, column, (table->num_rows / 2), ASCENDING);
    stage1++;
  }
  else
  {
    recBitonicSort(table, (table->num_rows / 2), column, table->num_rows / 2, DESCENDING);
    stage1++;
  }
  if (stage1 == 2)
  {
    printf("after stage 1\n");
    print_table_dbg(table, 0, 8);
  }
//int print_table_dbg(table_t *table, int start, int end) {

//bflush(table);
#define STAGE2
// stage 2
// TODO: replace it with conditional variables
  while (stage1 !=2) ;
#ifdef STAGE2
  if (tid == 0)
  {
    bitonicSplit(table, 0, table->num_rows / 2, table->num_rows / 4, column);
    stage2++;
  }
  else
  {
    bitonicSplit(table, table->num_rows / 4, table->num_rows - (table->num_rows / 4), table->num_rows / 2, column);
    stage2++;
  }
#endif
  if (stage2 == 2)
  {
    printf("after stage 2\n");
    print_table_dbg(table, 0, 8);
  }
#define STAGE3
  //bflush(table);
#ifdef STAGE3
// TODO: replace it with conditional variables
  while (stage2 !=2) ;
  //stage 3
  if (tid == 0)
  {
    //printf("stage3, num_rows %d | tid = %d\n", table->num_rows, tid);
    recBitonicSort(table, 0, column, table->num_rows / 2, ASCENDING);
    stage3++;
  }
  else
  {
    recBitonicSort(table, table->num_rows / 2, column, table->num_rows / 2, ASCENDING);
    stage3++;
  }
  if (stage3 == 2)
  {
    printf(">>>>> after stage 3\n");
    print_table_dbg(table, 0, 8);
  }
#endif
#ifdef CREATE_SORTED_TABLE
  *sorted_id = s_table->id;
#endif
  return ret;
}

/* 
 * 
 * Insecure interfaces... debug only 
 *
 */

int write_row_dbg(table_t *table, void *new_data, int row_num) {
	unsigned long dblk_num;
	unsigned long row_off, rows_per_blk; 
	data_block_t *b;

	if(row_num >= table->num_rows)
		return -1; 

	rows_per_blk = DATA_BLOCK_SIZE / table->sc.row_size; 
	dblk_num = row_num / rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (row_num - dblk_num * rows_per_blk) * table->sc.row_size; 
	
        b = bread(table, dblk_num);
	 	
	/* Copy the row into the data block */
	memcpy((char*)b->data + row_off, new_data, table->sc.row_size); 

	bwrite(b);
	brelse(b);
	return 0; 
}


int insert_row_dbg(table_t *table, void *row) {
	unsigned long dblk_num;
	unsigned long row_off, rows_per_blk; 
	data_block_t *b;

	//DBG("insert row, row size:%lu\n", table->sc.row_size); 

	rows_per_blk = DATA_BLOCK_SIZE / table->sc.row_size; 
	dblk_num = table->num_rows / rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (table->num_rows - dblk_num * rows_per_blk) * table->sc.row_size; 
	
        b = bread(table, dblk_num);
	 	
	/* Copy the row into the data block */
	memcpy((char*)b->data + row_off, row, table->sc.row_size); 

	table->num_rows ++; 

	bwrite(b);
	brelse(b);
	return 0; 


}

/* Insert one row. This one is not oblivious, will insert 
   a row at the very end of the table which is pointed by 
   table->num_rows 
 */
int ecall_insert_row_dbg(int db_id, int table_id, void *row) {

	data_base_t *db;
	table_t *table;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] 
		|| !row )
		return -1; 

	db = g_dbs[db_id]; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	return insert_row_dbg(table, row); 	
}

int print_row(schema_t *sc, void *row) {
	bool first = true; 	
	for(int i = 0;  i < sc->num_fields; i++) {
		if (first) {
			first = false; 
		} else {
			printf(", "); 
		}
		switch(sc->types[i]) {
		case BOOLEAN:
			printf("%b", *(bool*)((char*)row + sc->offsets[i]));
			break;
	
		case CHARACTER: 
			printf("%c", *(char*)((char*)row + sc->offsets[i]));
			break;
 
		case TINYTEXT:
		case VARCHAR:
			printf("%s", (char*)((char*)row + sc->offsets[i]));
			break; 
		case INTEGER:
			printf("%d", *(int*)((char*)row + sc->offsets[i]));
			break; 		
		default: 
			printf("unknown type");
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
	char *row;

	printf("scan table:%s with %lu rows\n", 
		table->name.c_str(), table->num_rows); 

	row = (char *)malloc(table->sc.row_size); 
	if (!row) {
		ERR("can't allocate memory for the row\n");
		return -1;
	}

	unsigned long long start, end; 
	start = RDTSC();

	for (i = 0; i < table->num_rows; i++) {
	
		/* Read one row. */
		read_row(table, i, (void *)row);

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

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )
		return -1; 

	db = g_dbs[db_id]; 
	
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
	char *row;

	printf("printing table:%s with %lu rows\n", 
		table->name.c_str(), table->num_rows); 

	row = (char *)malloc(table->sc.row_size); 
	if (!row) {
		ERR("can't allocate memory for the row\n");
		return -1;
	}

	if (end > table->num_rows) {
		end = table->num_rows; 
	}

	for (i = start; i < end; i++) {
	
		/* Read one row. */
		read_row(table, i, (void *)row);
		print_row(&table->sc, row); 

	}
	
	return 0; 

}


/* Print some number of rows from the database */
int ecall_print_table_dbg(int db_id, int table_id, int start, int end) {

	data_base_t *db;
	table_t *table;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )
		return -1; 

	db = g_dbs[db_id]; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	return print_table_dbg(table, start, end); 	
}


