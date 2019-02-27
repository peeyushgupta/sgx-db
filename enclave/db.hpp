#pragma once

#include <string>

#include "dbg.hpp"
#include "bcache.hpp"

#define MAX_DATABASES 10
#define MAX_ROWS (1 << 20) /* 1 M for now */
#define MAX_ROW_SIZE (1 << 12) /* 4096 for now */
#define MAX_TABLES 512
#define MAX_COLS 20
#define MAX_CONDITIONS 3 // number of ORs allowed in one clause of a condition
#define THREADS_PER_DB 8 // number of threads concurrently working on DB

int reserve_tid();
void reset_tids();
int tid();


typedef enum schema_type {
	BOOLEAN = 1,
	BINARY = 2,
	VARBINARY = 3,
	DECIMAL = 4,
	CHARACTER = 5,
	VARCHAR = 6,
	INTEGER = 7,
	TINYTEXT = 8,
	PADDING = 9,
} schema_type_t;


#if 0 
struct Column{
    SchemaType ty;
    union{
        struct {
            u64 integral, decimal;
        } dec;
        struct {
            u64 len;
        } buf;
    };
};

struct Schema{
    Column schema[MAX_COLS]; 
    u64 lenRow;
};

#endif


typedef struct {
	bool fake;
	// we need to identify which table a particular row came from during
	// join. Basically, we just store the table_id here. For now assume
	// that we will only join tables within a database, where table_id
	// would be unique
	int from;
} row_header_t;

typedef struct {
	row_header_t header; 
	char data[MAX_ROW_SIZE];
} row_t;


typedef struct {
	int num_fields;
	int offsets[MAX_COLS];
	int sizes[MAX_COLS];
	schema_type types[MAX_COLS];
	//std::string names[MAX_COLS];
	int row_data_size; 
} schema_t;

typedef struct table {
	unsigned long id; 
	std::string name;
	schema_t sc;
	unsigned int num_rows;    /* Number of rows in the table (used only 
					 for bulk insertion) */
	unsigned long num_blks;   /* Number of blocks allocated */
	int fd [THREADS_PER_DB];  /* File descriptor backing up the table data */
	data_block_t **pinned_blocks; 
	unsigned long rows_per_blk; 
	struct data_base *db; 
} table_t;


typedef struct data_base {
	std::string name;
	table_t *tables[MAX_TABLES];
	bcache_t bcache;
	void *io_buf[THREADS_PER_DB]; /* each thread has an I/O buffer */
} data_base_t;

// One condition allows you to join two tables (left 
// and right), joining fields should be qual i.e., 
// for all k:
//    left_table.column[fields_left[k]] == right_table.column[fields_right[k]]
//
// I assume we'll be able to join more tables with a 
// linked list of conditions
//
typedef struct join_condition join_condition_t;
struct join_condition {
	unsigned int num_conditions;
	unsigned int table_left; 
	unsigned int table_right; 
	unsigned int fields_left[MAX_CONDITIONS];
	unsigned int fields_right[MAX_CONDITIONS];
	join_condition_t *next;  /* not supported at the moment */
};


static inline unsigned long row_header_size() {

	return sizeof(row_header_t);
}

static inline unsigned long row_data_size(table_t *table) {

	return table->sc.row_data_size;
}

static inline unsigned long row_size(table_t *table) {

	return row_header_size() + row_data_size(table);
}

static inline unsigned long row_data_size(schema_t *sc) {

	return sc->row_data_size;
}

static inline unsigned long row_size(schema_t *sc) {

	return row_header_size() + row_data_size(sc);
}

static inline int get_pinned_row(table_t *table, unsigned int row_num, data_block_t **block,  row_t **row) {

	unsigned long dblk_num;
	unsigned long row_off; 
	data_block_t *b;

	/* Make a fake row if it's outside of the table
           assuming it's padding */
	//if(row_num >= table->num_rows) {
	//	return -1; 
	//} 

	dblk_num = row_num / table->rows_per_blk;

	
	/* Offset of the row within the data block in bytes */
	row_off = (row_num - dblk_num * table->rows_per_blk) * row_size(table); 
	//row_off = (row_num % table->rows_per_blk) * row_size(table); 

        b = table->pinned_blocks[dblk_num]; 
	*block = b;  	
	*row = (row_t*) ((char*)b->data + row_off); 
	return 0; 
}


int create_table(data_base_t *db, std::string &name, schema_t *schema, table_t **new_table);
int read_row(table_t *table, unsigned int row_num, row_t *row);
int write_row_dbg(table_t *table, row_t *row, unsigned int row_num);
int print_row(schema_t *sc, row_t *row); 

int read_data_block(table *table, unsigned long blk_num, void *buf);
int write_data_block(table *table, unsigned long blk_num, void *buf); 

int insert_row_dbg(table_t *table, row_t *row);

int project_schema(schema_t *old_sc, int* columns, int num_columns, schema_t *new_sc);
int pad_schema(schema_t *old_sc, int num_pad_bytes, schema_t *new_sc);
int project_row(row_t *old_row, schema_t *sc, row_t* new_row);
int promote_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl);
int project_promote_pad_table(
    data_base_t *db, 
    table_t *tbl, 
    int project_columns [], 
    int num_project_columns,
    int promote_columns [],
    int num_pad_bytes,
    table_t **p3_tbl    
);

int print_table_dbg(table_t *table, int start, int end);

int join_and_write_sorted_table(data_base_t *db, table_t *tbl, int max_joinability, join_condition_t *c, int *join_table_id);

void *aligned_malloc(size_t size, size_t alignment);
void aligned_free(void *aligned_ptr);
int pin_table(table_t *table);
int unpin_table_dirty(table_t *table);
int delete_table(data_base_t *db, table_t *table);
data_base_t *get_db(unsigned int id);
bool compare_rows(schema_t *sc, int column, row_t *row_l, row_t *row_r);
void *get_column(schema_t *sc, int field, row_t *row);

/* Enclave interface */
#if NO_SGX
int ecall_create_db(const char *cname, int name_len, int *db_id);
int ecall_create_table(int db_id, const char *cname, int name_len, schema_t *schema, int *table_id);
int ecall_insert_row_dbg(int db_id, int table_id, void *row);
int ecall_flush_table(int db_id, int table_id);
int ecall_join(int db_id, join_condition_t *c, int *join_tbl_id);
int ecall_quicksort_table(int db_id, int table_id, int field, int *sorted_id);
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
		int *write_table_id);
#endif

