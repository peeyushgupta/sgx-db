#include "spinlock.hpp"
#include "util.hpp"
#include "time.hpp"
#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif
#include "db.hpp"
#include <cassert>
#include <string.h>
#include "column_sort.hpp"
using namespace std;

#define ECALL_TEST_LENGTH 10000

struct spinlock s_inc = {.locked = 0};
struct two_numbers {
	volatile unsigned long a /*__attribute__((aligned(64)))*/; 
	volatile unsigned long b /*__attribute__((aligned(64)))*/; 
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

barrier_t b;
thread_local volatile unsigned int lsense = 0;

int ecall_barrier_test(unsigned long count, int num_threads, int tid) {

	for (auto i = 0u; i < count; i++) {
		barrier_wait(&b, &lsense, tid, num_threads);
	}
    return 0; 
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

	if (!(db = get_db(db_id)))
		return -1;
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = promote_table(db, table, column, &p_table); 
	*promoted_table_id = p_table->id; 

	return ret; 
}

/* Test the function that sorts the table with column sort */
int ecall_column_sort_table_dbg(int db_id, int table_id, int column) {
	int ret; 
	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)))
		return -1;
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = column_sort_table(db, table, column); 

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
            test_tbl_name.c_str(), (*test_tbl)->id); 

	return 0; 
}

int ecall_bcache_test_create_read_write_table(int db_id, int from_table_id, int *to_table_id) {
	int ret;
	data_base_t *db;
	table_t *from_tbl, *to_tbl;

	if (!(db = get_db(db_id)))
		return -1;

	from_tbl = db->tables[from_table_id];
	if (! from_tbl )
		return -3; 

	ret = bcache_test_create_read_write_table(db, from_tbl, &to_tbl); 
	if (ret) {
		ERR("reading/writing table:%d\n", ret);
		return ret; 
	}

	to_tbl->num_rows.store(from_tbl->num_rows.load());

	*to_table_id = to_tbl->id;

	return ret; 
};


int bcache_test_read_write(data_base_t *db, table_t *from_tbl, table_t *to_tbl) {
	int ret;  
	row_t *row_old; 

	row_old = (row_t*) malloc(row_size(from_tbl));
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

	//bflush(to_tbl); 

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

	if (!(db = get_db(db_id)))
		return -1;

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

int ecall_bcache_test_cmp_read_write(int db_id, int from_table_id, int to_table_id) {
	int ret;
	data_base_t *db;
	table_t *from_tbl, *to_tbl;

	if (!(db = get_db(db_id)))
		return -1;

	from_tbl = db->tables[from_table_id];
	to_tbl = db->tables[to_table_id];
	if (! from_tbl || ! to_tbl)
		return -3; 

	ret = compare_tables(from_tbl, to_tbl); 
	if (ret) {
		ERR("reading/writing table:%d\n", ret);
		return ret; 
	}

	return ret; 
};

void assert_schemas_entries_same(schema_t sc1, schema_t sc2, int i, int j)
{
    assert(sc1.offsets[i] == sc2.offsets[j]);
    assert(sc1.types[i] == sc2.types[j]);
    assert(sc1.sizes[i] == sc2.sizes[j]);
}

void print(char* s) 
{
    printf("%s", s); 
}

int test_project_schema() {
    printf("starting test_project_schema");
    schema_t sc_old, sc_new;
    int *columns;
    sc_old.num_fields = 3; 

    sc_old.offsets[0] = 0;
    sc_old.types[0] = BOOLEAN;
    sc_old.sizes[0] = 1;

    sc_old.offsets[1] = 1;
    sc_old.types[1] = INTEGER;
    sc_old.sizes[1] = 4;

    sc_old.offsets[2] = 5;
    sc_old.types[2] = TINYTEXT;
    sc_old.sizes[2] = 10;

    sc_old.row_data_size = sc_old.sizes[0] + sc_old.sizes[1] + sc_old.sizes[2];

    // all columns
    columns = new int[3];
    columns[0] = 0; columns[1] = 1; columns[2] = 2;
    project_schema(&sc_old, columns, 3, &sc_new);
    assert(sc_new.row_data_size == sc_old.row_data_size);
    assert(sc_new.num_fields == 3);
    for(int i = 0; i < 3; i++)
    {
        assert_schemas_entries_same(sc_new, sc_old, i, i);
    }
    delete [] columns;

    // columns 0 and 2
    columns = new int[2];
    columns[0] = 0; columns[1] = 2;
    project_schema(&sc_old, columns, 2, &sc_new);
    assert(sc_new.row_data_size == sc_old.sizes[0] + sc_old.sizes[2]);
    assert(sc_new.num_fields == 2);

    assert_schemas_entries_same(sc_new, sc_old, 0, 0);
    assert_schemas_entries_same(sc_new, sc_old, 1, 2);
    
    // columns 0 and 1
    columns[1] = 1;
    project_schema(&sc_old, columns, 2, &sc_new);
    assert(sc_new.row_data_size == sc_old.sizes[0] + sc_old.sizes[1]);
    assert(sc_new.num_fields == 2);

    assert_schemas_entries_same(sc_new, sc_old, 0, 0);
    assert_schemas_entries_same(sc_new, sc_old, 1, 1);

    // columns 2 and 0
    columns[0] = 2; columns[1] = 0;
    project_schema(&sc_old, columns, 2, &sc_new);
    assert(sc_new.row_data_size == sc_old.sizes[2] + sc_old.sizes[0]);
    assert(sc_new.num_fields == 2);

    assert_schemas_entries_same(sc_new, sc_old, 0, 2);
    assert_schemas_entries_same(sc_new, sc_old, 1, 0);
    return 0; 
}

void assert_schema_same_except_for_padding_at_end(schema_t sc1, schema_t sc2, int num_pad_bytes) {
    for(int i = 0; i < sc1.num_fields; i++) {
        assert_schemas_entries_same(sc1, sc2, i, i);
    }
    assert(sc2.num_fields == sc1.num_fields + 1);
    int padding_field_num = sc1.num_fields;
    assert(sc2.offsets[padding_field_num] == sc1.offsets[padding_field_num - 1] 
                                             + sc1.sizes[padding_field_num - 1]);
    assert(sc2.types[padding_field_num] == PADDING);
    assert(sc2.sizes[padding_field_num] == num_pad_bytes);
}

int test_pad_schema() {
    schema_t sc_old, sc_new;

    sc_old.offsets[0] = 0;
    sc_old.types[0] = INTEGER;
    sc_old.sizes[0] = 8;

    sc_old.offsets[1] = 8;
    sc_old.types[1] = TINYTEXT;
    sc_old.sizes[1] = 10;

    sc_old.offsets[2] = 18;
    sc_old.types[2] = BOOLEAN;
    sc_old.sizes[2] = 1;

    sc_old.num_fields = 3;
    sc_old.row_data_size = 8 + 10 + 1;

    int num_pad_bytes_values[3] = {0, 4, 7};
    int num_pad_bytes;
    for(int i = 0; i < 3; i++)
    {
        num_pad_bytes = num_pad_bytes_values[i];
        pad_schema(&sc_old, num_pad_bytes, &sc_new);
        assert_schema_same_except_for_padding_at_end(sc_old, sc_new, num_pad_bytes);
    }
    return 0; 
}

int ecall_test_project_schema() 
{
    return test_project_schema();
}

int ecall_test_pad_schema()
{
    return test_pad_schema();
}

void test_project_row() { 
    schema_t sc_old, sc_new;
    int* columns;
    row_t *old_row, *new_row;

    sc_old.offsets[0] = 0;
    sc_old.types[0] = INTEGER;
    sc_old.sizes[0] = 4;

    sc_old.offsets[1] = 4;
    sc_old.types[1] = TINYTEXT;
    sc_old.sizes[1] = 11;

    sc_old.offsets[2] = 15;
    sc_old.types[2] = BOOLEAN;
    sc_old.sizes[2] = 1;

    sc_old.num_fields = 3;
    sc_old.row_data_size = 4 + 11 + 1;

    old_row = (row_t*) new char [row_size(&sc_old)];
    new_row = (row_t*) new char [row_size(&sc_old)];
    int i = 73, i_read;
    char s[] = "Troglodyte", *s_read = new char[10+1];
    bool b = 1, b_read;
    memcpy(old_row, (char *)&i, 4);
    memcpy(old_row + 4, s, 11);
    memcpy(old_row + 4 + 11, (char *)&b, 1);

    // project 0 1 2
    columns = new int[3];
    columns[0] = 0; columns[1] = 1; columns[2] = 2;
    project_schema(&sc_old, columns, 3, &sc_new);
    project_row(old_row, &sc_new, new_row);
    memcpy(&i_read, new_row, 4);
    memcpy(s_read, new_row + 4, 10 + 1);
    memcpy(&b_read, new_row + 4 + 10 + 1, 1);
    assert(i_read == i);
    assert(string(s) == string(s_read));
    assert(b_read == b);

    // project 2 0
    delete [] new_row;
    new_row = (row_t*) new char[row_size(&sc_old)];
    columns[0] = 2; columns[1] = 0;
    project_schema(&sc_old, columns, 2, &sc_new);
    project_row(old_row, &sc_new, new_row);
    memcpy(&b_read, new_row, 1);
    memcpy(&i_read, new_row + 1, 4);
    assert(i_read == i);
    assert(b_read == b);

    // project 1 0
    delete [] new_row;
    new_row = (row_t*) new char[row_size(&sc_old)];
    columns[0] = 1; columns[1] = 0;
    project_schema(&sc_old, columns, 2, &sc_new);
    project_row(old_row, &sc_new, new_row);
    memcpy(s_read, new_row, 11);
    memcpy(&i_read, new_row + 11, 4);
    assert(i_read == i);
    assert(string(s_read) == string(s));

    // project 2
    delete [] new_row;
    new_row = (row_t *) new char[row_size(&sc_old)];
    columns[0] = 2; 
    project_schema(&sc_old, columns, 1, &sc_new);
    project_row(old_row, &sc_new, new_row);
    memcpy(&b_read, new_row, 1);
    assert(b_read == b);
}

int ecall_test_project_row()
{
    test_project_row();
    return 0; 
}

template <typename T>
void obli_cswap_t(T *src, T *dst, bool cond);

void ecall_obli_swap_tests() {
	uint64_t a64 = 0xabcdabcdefefefef, b64 = 0x1111222233334444;
	uint32_t a32 = 0xdeadbeef, b32 = 0x1234abcd;
	uint16_t a16 = 0x8888, b16 = 0xbade;
	uint8_t a8 = 0xab, b8 = 0xed;

	obli_cswap_t(&a64, &b64, true);
	assert(a64 == 0x1111222233334444);

	obli_cswap_t(&a32, &b32, true);
	assert(a32 == 0x1234abcd);

	obli_cswap_t(&a16, &b16, true);
	assert(a16 == 0xbade);

	obli_cswap_t(&a8, &b8, true);
	assert(a8 == 0xed);

	return;
}
