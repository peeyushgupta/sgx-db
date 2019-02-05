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
int column_sort_pick_params(unsigned long num_records, 
				unsigned long rec_size, 
				unsigned long bcache_rec_size, 
				unsigned long sgx_mem_size, 
				unsigned long *r, 
				unsigned long *s); 

int column_sort_table(data_base_t *db, table_t *table, int column);

int print_table_dbg(table_t *table, int start, int end);

int compare_tables(table_t *left, table_t *right, int tid, int num_threads);
int compare_tables(table_t *left, table_t *right);


inline int exchange(table_t *tbl, int i, int j, row_t *row_i, row_t *row_j);
int compare(table_t *tbl, int column, int i, int j, int dir);
void bitonicMerge(table_t *tbl, int lo, int cnt, int column, int dir);
void recBitonicSort(table_t *tbl, int lo, int cnt, int column, int dir, int tid);
int bitonic_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl);
int sort_table_parallel(table_t *tbl, int column, int tid, int num_threads);

int join_and_write_sorted_table(table_t *tbl, int db_id, join_condition_t *c, int *join_table_id);

int quick_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl);
void quickSort(table_t *tbl, int column, int start, int end);
int partition(table_t *tbl, int column, int start, int end);

void *aligned_malloc(size_t size, size_t alignment);
void aligned_free(void *aligned_ptr);

 
/* Enclave interface */
#if NO_SGX
int ecall_create_db(const char *cname, int name_len, int *db_id);
int ecall_create_table(int db_id, const char *cname, int name_len, schema_t *schema, int *table_id);
int ecall_insert_row_dbg(int db_id, int table_id, void *row);
int ecall_flush_table(int db_id, int table_id);
int ecall_join(int db_id, join_condition_t *c, int *join_tbl_id);
int ecall_quicksort_table(int db_id, int table_id, int field, int *sorted_id);
#endif

