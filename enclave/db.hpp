#pragma once

#include <string>

#include "dbg.hpp"
#include "bcache.hpp"

#define MAX_DATABASES 1
#define MAX_ROWS (1 << 20) /* 1 M for now */
#define MAX_ROW_SIZE (1 << 12) /* 4096 for now */
#define MAX_TABLES 10
#define MAX_COLS 20
#define MAX_CONDITIONS 3 // number of ORs allowed in one clause of a condition
#define THREADS_PER_DB 1 // number of threads concurrently working on DB

#define ECALL_TEST_LENGTH 10000

typedef enum schema_type {
	BOOLEAN = 1,
	BINARY = 2,
	VARBINARY = 3,
	DECIMAL = 4,
	CHARACTER = 5,
	VARCHAR = 6,
	INTEGER = 7,
	TINYTEXT = 8,
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
	int num_fields;
	int offsets[MAX_COLS];
	int sizes[MAX_COLS];
	schema_type types[MAX_COLS];
	//std::string names[MAX_COLS];
	int row_size; 
} schema_t;

typedef struct table {
	unsigned long id; 
	std::string name;
	schema_t sc;
	unsigned long long num_rows;  /* Number of rows in the table (used only 
					 for bulk insertion) */
	unsigned long long num_blks;  /* Number of blocks allocated */
	int fd;                       /* File descriptor backing up the table data */
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



int read_data_block(table *table, unsigned long blk_num, void *buf);
int write_data_block(table *table, unsigned long blk_num, void *buf); 

int insert_row_dbg(table_t *table, void *row);


/* Enclave interface */
#if NO_SGX
int ecall_create_db(const char *cname, int name_len, int *db_id);
int ecall_create_table(int db_id, const char *cname, int name_len, schema_t *schema, int *table_id);
int ecall_insert_row_dbg(int db_id, int table_id, void *row);
int ecall_flush_table(int db_id, int table_id);
int ecall_join(int db_id, join_condition_t *c, int *join_tbl_id);
#endif

