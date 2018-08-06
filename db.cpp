#include "db.hpp"
#include "obli.hpp"
#include "env.hpp"
#include "bcache.hpp"

#include <cstdlib>
#include <cstdio>

#include <string.h>

#define FILE_READ_SIZE (1 << 12)

data_base_t* g_dbs[MAX_DATABASES];

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
			printf("%s: alloc failed\n", __func__); 
			goto cleanup;
		};
	};

	binit(&db->bcache);
	return 0; 

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
		if(db->bcache.data_blks[i].data)
			free(db->bcache.data_blks[i].data);
	};
	return; 
};


/* Read data block from external storage into enclave's memory (the memory 
   region is passed as the  DataBlock argument), decrypt on the fly */
int read_data_block(table *table, unsigned long blk_num, void *buf) {
	unsigned long long total_read = 0; 
	long long read; 

	if(blk_num >= table->num_blks) {
		/* Allocate new data block */
		memset(buf, 0, DATA_BLOCK_SIZE);
		return 0;
	}
	ocall_seek(table->fd, blk_num*DATA_BLOCK_SIZE); 

	while (total_read < DATA_BLOCK_SIZE) { 
		read = ocall_read_file(table->fd, 
			(void *)((char *)buf + total_read), FILE_READ_SIZE);
		if (read < 0) {
			printf("%s: read filed\n", __func__); 
			return -1; 
		}
		total_read += read;  
	}
	return 0; 
}

/* Write data block from enclave's memory back to disk. 
 * For temporary results, we'll create temporary tables that will 
 * have corresponding encryption keys (huh?)
*/
int write_data_block(table *table, unsigned long blk_num, void *buf) {
	unsigned long long total_written = 0; 
	long long written; 

	ocall_seek(table->fd, blk_num*DATA_BLOCK_SIZE);
 
	while (total_written < DATA_BLOCK_SIZE) { 
		written = ocall_write_file(table->fd, 
			(void *)((char *)buf + total_written), FILE_READ_SIZE);
		if (written < 0) {
			printf("%s: write filed\n", __func__); 
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

/* Create table, in the database dbId, described by the schema, returns tableId */
int ecall_create_table(int db_id, const char *cname, int name_len, schema_t *schema, int *table_id) {
	int i, ret, fd;
	std::string name(cname, name_len);
	data_base_t *db;
	table_t *table;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] 
	   || !schema || !table_id || !cname || (name_len == 0))	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

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
	table->name = name;
	table->num_rows = 0; 
	table->num_blks = 0; 
	table->schema = *schema;
	table->db = db; 	

	/* Call outside of enclave to open a file for the table */
	fd = ocall_open_file(name.c_str());
	if (fd < 0) {
		ret = -5;
		goto cleanup; 
	} 

	table->fd = fd;

	/* Fill table with dummy data */
	

	return 0;

cleanup:
	delete table;
	db->tables[i] = NULL; 
	return ret; 
	

};

/* Insert one row */
int ecall_insert_row(int db_id, int table_id, void *row) {

   /* An oblivious version of row insert will update all 
      rows in the table to conceal its size */
	return -1; 
}

/* Select ... need to think */
int ecall_select() {
	return -1;
};

/* 
 * 
 * Insecure interfaces... debug only 
 *
 */

/* Insert one row. This one is not oblivious, will insert 
   a row at the very end of the table which is pointed by 
   table->num_rows 
 */
int ecall_insert_row_dbg(int db_id, int table_id, void *row) {

	data_base_t *db;
	unsigned long dblk_num;
	unsigned long row_off, rows_per_blk; 
	table_t *table;
	data_block_t *b;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] 
		|| !row )
		return -1; 

	db = g_dbs[db_id]; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];
	
	rows_per_blk = DATA_BLOCK_SIZE / table->schema.row_size; 
	dblk_num = table->num_rows / rows_per_blk;

	/* Offset of the row within the data block in bytes */
	row_off = (table->num_rows - dblk_num * rows_per_blk) * table->schema.row_size; 
	
        b = bread(table, dblk_num);
	 	
	/* Copy the row into the data block */
	memcpy((char*)b->data + row_off, row, table->schema.row_size); 

	table->num_rows ++; 

	bwrite(b);
	brelse(b);
	return 0; 
}

