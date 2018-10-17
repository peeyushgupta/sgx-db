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

//#define FILE_READ_SIZE (1 << 12)

#define FILE_READ_SIZE DATA_BLOCK_SIZE

data_base_t* g_dbs[MAX_DATABASES];

static inline int tid() { return 0; }

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
	DBG("ocall_seek: %llu cycles\n", end - start);

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
	DBG("ocall_read_file: %llu cycles\n", end - start);

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

		DBG("left:%s, right:%s\n", left, right); 

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
		new_sc->offsets[i] = old_sc->offsets[i] + size;
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

int project_schema(schema_t *old_sc, int* columns, int num_columns, schema_t *new_sc) {
    int j;
    int size;
    for(int i = 0; i < num_columns; i++) {
        j = columns[i];
        new_sc->offsets[i] = old_sc->offsets[j];
        new_sc->sizes[i] = old_sc->sizes[j];
        size += new_sc->sizes[i];
        new_sc->types[i] = old_sc->types[j];
    }
    new_sc->num_fields = num_columns;
    new_sc->row_size = size;
    return 0;
}

int pad_schema(schema_t *old_sc, int num_pad_bytes, schema_t *new_sc){
    if (old_sc->num_fields >= MAX_COLS)
        return -1;
    int i;
    for (i = 0; i < old_schema->num_fields; i++) {
        new_sc->offsets[i] = old_sc->offsets[i];
        new_sc->sizes[i] = old_sc->sizes[i];
        new_sc->types[i] = old_schema->types[i];
    }
    new_sc->offsets[i] = old_sc->row_size;
    new_sc->sizes[i] = num_pad_bytes;
    new_sc->types[i] = schema_type::PADDING;

    new_sc->num_fields++;
    new_sc->row_size += num_pad_bytes;
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

int project_row(void *old_row, schema_t *sc, void* new_row) {
    int offset = 0;
    for(int i = 0; i < sc->num_fields; i++) {
        memcpy(new_row + offset, (char*)old_row + sc->offsets[i], sc->sizes[i]);
        offset += sc->sizes[i];
    }
    return 0; 
}

int pad_row(void *old_row, schema_t *sc, void* new_row) {
    
}

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
            p_tbl_name.c_str(), p_tbl->id); 

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

/* Promote column in a table to the front */
int ecall_promote_table_dbg(int db_id, int table_id, int column, int *promoted_table_id) {
	int ret; 
	data_base_t *db;
	table_t *table, *p_table;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )
		return -1; 

	db = g_dbs[db_id]; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = promote_table(db, table, column, &p_table); 
	*promoted_table_id = p_table->id; 

	return ret; 
}

int project_promote_pad_table(
    data_base_t *db, 
    table_t *tbl, 
    int project_columns [], 
    int promote_columns [],
    int num_pad_bytes,
    table_t **p3_tbl    
)
{
    int ret;
    std::string p3_tbl_name;
    schema_t project_sc, project_promote_sc, project_promote_pad_sc;
    void *row_old, *row_new;
    p3_tbl_name = "p3:" + tbl->name;

    ret = project_schema(&tbl->sc, 
                         project_columns, 
                         sizeof(project_columns) / sizeof(int), 
                         &project_sc);
    if (ret) {
        ERR("project_schema failed:%d\n", ret);
        return ret;
    }
    ret = promote_schema(&project_sc,
                         promote_columns,
                         sizeof(promote_columns) / sizeof(int),
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

    for (unsigned int i = 0; i < tbl->num_rows; i++) {
        // Read original row
        ret = read_row(tbl, i, row_old);
        if(ret) {
            ERR("read_row failed on row %d of table %s\n", i, tbl->name.c_str());
            goto cleanup;
        }

        // Project row
        // Since the schema was projected, promoted, and padded, this is all we need to do.
        ret = project_row(old_row, project_promote_pad_sc, new_row);
        if(ret) {
            ERR("project_row failed on row %d of table %s\n", i, tbl->name.c_str());
            goto cleanup;
        }

        // Add row to table
        ret = insert_row_dbg(p3_tbl, row_new);
        if(ret) {
            ERR("insert_row_db failed on row %d of table %s\n", i,
                (*p3_tbl)->name.c_str());
            goto cleanup;
        }
    }

    bflush(*p3_tbl);
    ret = 0;

cleanup:
    if (row_old)
        free(row_old);

    if (row_new)
        free(row_new);

    return ret;
    
} 


/* 
 * 
 * Insecure interfaces... debug only 
 *
 */

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

void ecall_null_ecall() {
	return; 
}


void ecall_test_null_ocall() {

	unsigned long long start, end; 

	printf("Testing: null ocall for %llu iterations\n", ECALL_TEST_LENGTH);

	start = RDTSC();

	for (int i = 0; i < ECALL_TEST_LENGTH; i++) {
	
		ocall_null_ocall();

	}
	
	end = RDTSC();

	printf("Null ocall %llu cycles\n", (end - start)/ECALL_TEST_LENGTH);

	return; 

}

