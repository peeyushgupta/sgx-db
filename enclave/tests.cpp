#include "spinlock.hpp"
#include "util.hpp"
#include "time.hpp"
#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif
#include "db.hpp"

#define ECALL_TEST_LENGTH 10000

extern data_base_t* g_dbs[MAX_DATABASES];

struct spinlock s_inc = {.locked = 0};
struct two_numbers {
	unsigned long a; 
	unsigned long b; 
};
struct two_numbers tn = {.a = 0, .b = 0};


/* Test functionality of spinlocks. Add two numbers without 
   and with the lock */
int ecall_spinlock_inc(unsigned long count) {

	int passed = 0; 

	printf("%s: First test without lock to see if we get a race (init tid:%d)\n",
		 __func__, reserve_tid()); 
	for (unsigned long i = 0; i < count; i++) {
		tn.a ++; 
		tn.b ++; 	
	}

	printf("%s: We expect to fail here: a:%lu, b:%lu (%s)\n", 
		__func__, tn.a, tn.b, tn.a == tn.b? "passed" : "failed"); 

	printf("%s: Now run with locks on\n", __func__); 

	acquire(&s_inc); 	
	tn.a = 0; 
	tn.b = 0;
	release(&s_inc);
 
	for (unsigned long i = 0; i < count; i++) {
		acquire(&s_inc); 
		tn.a ++; 
		tn.b ++; 	
		release(&s_inc); 
	}

	acquire(&s_inc);
	passed = (tn.a == tn.b) ? 0 : -1;
	printf("%s: tid:%d a:%lu, b:%lu (%s)\n", 
		__func__, tid(), tn.a, tn.b, tn.a == tn.b? "passed" : "failed"); 
	release(&s_inc); 
	return passed; 
}

/* Test overheads of a NULL ecall */

void ecall_null_ecall() {
	return; 
}

/* Test overheads of a NULL ocall */

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


/* Test the function that promotes column in a table to the front */
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

/* Test the function that sorts the table with column sort */
int ecall_column_sort_table_dbg(int db_id, int table_id, int r, int s) {
	int ret; 
	data_base_t *db;
	table_t *table;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )
		return -1; 

	db = g_dbs[db_id]; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = column_sort_table(db, table, r, s); 

	return ret; 
}

/* Test thread-safety of the buffer cache */

int bcache_test_create_read_write_table(data_base_t *db, table_t *tbl, table_t **test_tbl) {

	int ret;
	std::string test_tbl_name;  

	test_tbl_name = "tbc:" + tbl->name; 

	ret = create_table(db, test_tbl_name, &tbl->sc, test_tbl);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	DBG("Created a table for concurrent read_write test %s, id:%d\n", 
            p_tbl_name.c_str(), p_tbl->id); 

	return 0; 
}

int ecall_bcache_test_create_read_write_table(int db_id, int from_table_id, int *to_table_id) {
	int ret;
	data_base_t *db;
	table_t *from_tbl, *to_tbl;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

	from_tbl = db->tables[from_table_id];
	if (! from_tbl )
		return -3; 

	ret = bcache_test_create_read_write_table(db, from_tbl, &to_tbl); 
	if (ret) {
		ERR("reading/writing table:%d\n", ret);
		return ret; 
	}

	to_tbl->num_rows = from_tbl->num_rows;

	*to_table_id = to_tbl->id;

	return ret; 
};


int bcache_test_read_write(data_base_t *db, table_t *from_tbl, table_t *to_tbl) {
	int ret;  
	void *row_old, *row_new; 

	row_old = malloc(MAX_ROW_SIZE);
	if(!row_old)
		return -5;

	for (unsigned long i = 0; i < from_tbl->num_rows; i ++) {

		// Read old row
		ret = read_row(from_tbl, i, row_old);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, from_tbl->name.c_str());
			goto cleanup;
		}

				
		/* Add row to the new table */
		ret = write_row_dbg(to_tbl, row_old, i);
		if(ret) {
			ERR("failed to insert row %d of the new table %s\n",
				i, to_tbl->name.c_str());
			goto cleanup;
		}

	}

	bflush(to_tbl); 

	ret = 0;
cleanup: 
	if (row_old)
		free(row_old); 

	return ret; 
};


int ecall_bcache_test_read_write(int db_id, int from_table_id, int to_table_id) {
	int ret;
	data_base_t *db;
	table_t *from_tbl, *to_tbl;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

	from_tbl = db->tables[from_table_id];
	to_tbl = db->tables[to_table_id];
	if (! from_tbl || ! to_tbl)
		return -3; 

	ret = bcache_test_read_write(db, from_tbl, to_tbl); 
	if (ret) {
		ERR("reading/writing table:%d\n", ret);
		return ret; 
	}

	return ret; 
};

int bcache_test_cmp_read_write(data_base_t *db, table_t *from_tbl, table_t *to_tbl) { 
	void *from_row, *to_row; 
	int ret;  

	from_row = malloc(MAX_ROW_SIZE);
	if(!from_row)
		return -1;

	to_row = malloc(MAX_ROW_SIZE);
	if(!to_row)
		return -2;

	if(from_tbl->num_rows != to_tbl->num_rows) {
		ERR("tables have different size: (%s,%d) != (%s, %d)\n",
			from_tbl->name.c_str(), from_tbl->num_rows, 
			to_tbl->name.c_str(), to_tbl->num_rows);
		return -3;
	};

	for (unsigned long i = 0; i < from_tbl->num_rows; i ++) {

		// Read old row
		ret = read_row(from_tbl, i, from_row);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, from_tbl->name.c_str());
			goto cleanup;
		}

				
		/* Add row to the promoted table */
		ret = read_row(to_tbl, i, to_row);
		if(ret) {
			ERR("failed to insert row %d of promoted table %s\n",
				i, to_tbl->name.c_str());
			goto cleanup;
		}

		ret = memcmp(from_row, to_row, from_tbl->sc.row_size); 
		if (ret) {
			ERR("tables have different rows: (%s,%d) != (%s, %d)\n",
			from_tbl->name.c_str(), i, 
			to_tbl->name.c_str(), i);
			print_row(&from_tbl->sc, from_row); 
			print_row(&to_tbl->sc, to_row);
			goto cleanup; 
		}
	}

	ret = 0;
cleanup: 
	if (from_row)
		free(from_row); 

	if (to_row)
		free(to_row); 


	return ret; 
};


int ecall_bcache_test_cmp_read_write(int db_id, int from_table_id, int to_table_id) {
	int ret;
	data_base_t *db;
	table_t *from_tbl, *to_tbl;

	if ((db_id > (MAX_DATABASES - 1)) || !g_dbs[db_id] )	
		return -1; 

	db = g_dbs[db_id];
	if(!db)
		return -2;

	from_tbl = db->tables[from_table_id];
	to_tbl = db->tables[to_table_id];
	if (! from_tbl || ! to_tbl)
		return -3; 

	ret = bcache_test_cmp_read_write(db, from_tbl, to_tbl); 
	if (ret) {
		ERR("reading/writing table:%d\n", ret);
		return ret; 
	}

	return ret; 
};

