#include "bin_packing_join_helper.hpp"
#include "db-tests.hpp"
#include "db.hpp"
#include "enclave_u.h" // Headers for untrusted part (autogenerated by edger8r)
#include <assert.h>
#include <cassert>
#include <fstream>
#include <sstream>
#include <string.h>
#include <string>
#include <thread>
#include <time.hpp>
#include <vector>

using namespace std;
#define OCALL_TEST_LENGTH 10000
// #define RANKINGS_TABLE_SIZE	(10)
#define RANKINGS_TABLE_SIZE	(360000)
// #define UVISITS_TABLE_SIZE	(20)
#define UVISITS_TABLE_SIZE	(350000)

#define RANDINT_TABLE_SIZE	256

typedef enum {
	RANKINGS_TABLE_ID = 0,
	UVISITS_TABLE_ID = 1,
	RAND_INT_TABLE_ID = 2,
	MAX_TABLE_TYPES_ID,
} table_id_t;


struct schemas {
	// redundant field
	table_id_t table_id;
	// predefined schema
	schema_t schema;
	// if the schema is valid or not
	bool valid;
} predef_schemas[] = {
	{
		.table_id = RANKINGS_TABLE_ID,
		.schema = {
			.num_fields = 3,
			.offsets = { 0, 255, 259},
			.sizes = { 255, 4, 4},
			.types = { TINYTEXT, INTEGER, INTEGER },
			.row_data_size = 259 + 4,
		},
		.valid = true,
	},
	{
		.table_id = UVISITS_TABLE_ID,
		.schema = {
			.num_fields = 9,
			.offsets = { 0, 255, 510, 514, 518, 773, 1028, 1283, 1538 },
			.sizes = { 255, 255, 4, 4, 255, 255, 255, 255, 4 },
			.types = { TINYTEXT, TINYTEXT, INTEGER, INTEGER, TINYTEXT, TINYTEXT, TINYTEXT, TINYTEXT, INTEGER },
			.row_data_size = 1538 + 4,
		},
		.valid = true,
	},
	{
		.table_id = RAND_INT_TABLE_ID,
		.schema = {
			.num_fields = 1,
			.offsets = { 0 },
			.sizes = { 4 },
			.types = { INTEGER },
		},
		.valid = true,
	},
};

schema_t get_predef_schema(table_id_t id)
{
	if (id >= MAX_TABLE_TYPES_ID)
		goto out;
	else {
		auto schemas = &predef_schemas[id];
		if (schemas->valid)
			return schemas->schema;
	}
out:
	DBG("Could not find predefined schema for table_id: %d\n", id);
	return {0};
}

int populate_database_from_csv(std::string fname, int num_rows, int db_id, int
		table_id, schema_t *sc, sgx_enclave_id_t eid)
{
	uint8_t *row;
	char data[MAX_ROW_SIZE];
	char line[MAX_ROW_SIZE];
	int sgx_ret;
	int ret;

	std::ifstream file(fname);

	row = (uint8_t*)malloc(MAX_ROW_SIZE);

	for (auto i = 0u; i < num_rows; i++) {
		memset(row, 0x0, MAX_ROW_SIZE);
		file.getline(line, MAX_ROW_SIZE);

		std::istringstream ss(line);

		for (auto j = 0u; j < sc->num_fields; j++) {
			if (!ss.getline(data, MAX_ROW_SIZE, ',')) {
				break;
			}
			if(sc->types[j] == INTEGER) {
				auto d = 0;
				d = atoi(data);
				memcpy(&row[sc->offsets[j]], &d, 4);
			} else if (sc->types[j] == TINYTEXT) {
				strncpy((char*)&row[sc->offsets[j]], data, strlen(data) + 1);
			}
		}

		sgx_ret = ecall_insert_row_dbg(eid, &ret, db_id, table_id, row);

		if (sgx_ret || ret) {
			ERR("insert row:%d from %s, err:%d (sgx ret:%d)\n",
				i, fname.c_str(), ret, sgx_ret);
			return ret;
		}

	}
	return 0;
}

void column_sort_table_parallel(sgx_enclave_id_t eid, int db_id, int table_id, int field, int num_threads);

/* Test timings for ocalls */
int test_null_ocalls(sgx_enclave_id_t eid) {

	unsigned long long start, end; 

	printf(TXT_FG_YELLOW "Testing null ecall" TXT_NORMAL " for %d iterations\n", OCALL_TEST_LENGTH);
	start = RDTSC();
	for (int i = 0; i < OCALL_TEST_LENGTH; i++) {
		ecall_null_ecall(eid);
	}
	end = RDTSC();
	printf("Null ecall %llu cycles\n", (end - start)/OCALL_TEST_LENGTH);

	ecall_test_null_ocall(eid);
};

/* Trivial thread test, each does a null ecall */
void thread_fn(sgx_enclave_id_t eid) {
	ecall_null_ecall(eid);
}

int test_threads(sgx_enclave_id_t eid) {
	printf(TXT_FG_YELLOW "Simple threads test" TXT_NORMAL ": Creating threads\n");
	thread t1(thread_fn, eid);
	thread t2(thread_fn, eid);
	thread t3(thread_fn, eid);

	t1.join();
	t2.join();
	t3.join();
	printf("Joining threads\n");
	return 0;
}

/* Test correctness of spinlocks */
void spinlock_inc_fn(sgx_enclave_id_t eid, unsigned long count) 
{
	int ret;
	ecall_spinlock_inc(eid, &ret, count); 
}

void test_spinlock_inc(sgx_enclave_id_t eid, unsigned long count)
{
	thread t1(spinlock_inc_fn, eid, count);
	thread t2(spinlock_inc_fn, eid, count);
	thread t3(spinlock_inc_fn, eid, count);
	thread t4(spinlock_inc_fn, eid, count);

	t1.join();
	t2.join();
	t3.join(); 
	t4.join(); 
}

/* Test thread-safety of the buffer cache */
void bcache_read_write_fn(sgx_enclave_id_t eid, int db_id, int from_table_id, int to_table_id) 
{
	int ret;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;

	sgx_ret = ecall_bcache_test_read_write(eid, &ret, db_id, from_table_id, to_table_id);
	if (sgx_ret || ret) {
		ERR("error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return;
	}

	return;  
}

void bcache_test_run_threads(sgx_enclave_id_t eid, int db_id, int from_table_id, int to_table_id)
{

	thread t1(bcache_read_write_fn, eid, db_id, from_table_id, to_table_id);
	thread t2(bcache_read_write_fn, eid, db_id, from_table_id, to_table_id);
	thread t3(bcache_read_write_fn, eid, db_id, from_table_id, to_table_id);
	thread t4(bcache_read_write_fn, eid, db_id, from_table_id, to_table_id);

	t1.join();
	t2.join();
	t3.join(); 
	t4.join(); 
}

int bcache_test(sgx_enclave_id_t eid, int db_id, int from_table_id)
{
	int ret; 
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;
	int to_table_id;

	printf(TXT_FG_YELLOW "%s:starting buffer cache test" TXT_NORMAL "\n", __func__); 

	sgx_ret = ecall_bcache_test_create_read_write_table(eid, &ret, db_id, from_table_id, &to_table_id);
	if (sgx_ret || ret) {
		ERR("error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	for (int i = 0; i < 1; i++)  {
		bcache_test_run_threads(eid, db_id, from_table_id, to_table_id);

		ecall_flush_table(eid, &ret, db_id, from_table_id);
		ecall_flush_table(eid, &ret, db_id, to_table_id);

		ecall_print_table_dbg(eid, &ret, db_id, from_table_id, 0, 23);
		ecall_print_table_dbg(eid, &ret, db_id, to_table_id, 0, 23);

	
		sgx_ret = ecall_bcache_test_cmp_read_write(eid, &ret, db_id, from_table_id, to_table_id);
		if (sgx_ret || ret) {
			ERR("error:%d (sgx ret:%d)\n", ret, sgx_ret);
			return ret;
		}

		printf("%s:buffer cache test passed\n", __func__); 
	}
	return 0;
}

void run_rankings_test(sgx_enclave_id_t eid, int db_id, int rankings_table_id)
{
	/* Column sort tests */
	unsigned int column;
	int ret;
	int num_threads = 4;
	// Rankings is 360000
	//r = 16384;
	//s = 16;
	column = 2;

	printf(TXT_FG_YELLOW "Column sort test" TXT_NORMAL ": sorting rankings table \n");

	unsigned long long start, end;
	start = RDTSC_START();

	column_sort_table_parallel(eid, db_id, rankings_table_id, column, num_threads);
	ecall_flush_table(eid, &ret, db_id, rankings_table_id);
	end = RDTSCP();

	printf("Sorting + flushing took %llu cycles\n", end - start);
	ecall_print_table_dbg(eid, &ret, db_id, rankings_table_id, 0, 23);
}

int test_rankings(sgx_enclave_id_t eid) {
 	
	schema_t sc, sc_udata;
	std::string db_name("rankings-and-udata");
	std::string table_name("rankings");
	std::string udata_table_name("udata");
	int i, db_id, rankings_table_id, udata_table_id, join_table_id = -1, ret; 
	join_condition_t c;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;

	std::string rankings_csv("rankings.csv");
	std::string uvisits_csv("uservisits.csv");


	sc = get_predef_schema(RANKINGS_TABLE_ID);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret; 
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &rankings_table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		goto out; 
	}

	ret = populate_database_from_csv(rankings_csv, RANKINGS_TABLE_SIZE, db_id, rankings_table_id, &sc, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", rankings_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, rankings_table_id);
	printf("created rankings table with db ID:%d | table_id:%d\n",
			db_id, rankings_table_id);

	sc_udata = get_predef_schema(UVISITS_TABLE_ID);

	sgx_ret = ecall_create_table(eid, &ret, db_id, udata_table_name.c_str(), udata_table_name.length(), &sc_udata, &udata_table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d), table:%s\n", 
			ret, sgx_ret, udata_table_name.c_str());
		goto out; 
	}


	ret = populate_database_from_csv(uvisits_csv, UVISITS_TABLE_SIZE, db_id, udata_table_id, &sc_udata, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", uvisits_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, udata_table_id);
	printf("created uservisits table with ID:%d | table_id:%d\n",
			db_id, udata_table_id);

#if defined(TABLE_SCAN_TESTS)
	for (i = 0; i < 10; i++) {
		ecall_scan_table_dbg(eid, &ret, db_id, rankings_table_id);
	}

	for (i = 0; i < 10; i++) {
		ecall_scan_table_dbg(eid, &ret, db_id, udata_table_id);
	}
#endif

#if defined(TEST_BCACHE)
	/* Buffer cache testst */
	{	
		bcache_test(eid, db_id, rankings_table_id);
	}
#endif

#if defined(TEST_PROMOTE_COLUMN)
	/* Promote column tests */
	{
		int p_rankings_table_id;

		printf(TXT_FG_YELLOW "Promote column test" TXT_NORMAL ": promoting rankings table \n"); 

		ecall_promote_table_dbg(eid, &ret, db_id, rankings_table_id, 1, &p_rankings_table_id);
		ecall_flush_table(eid, &ret, db_id, p_rankings_table_id);
		printf("promoted rankings table\n");
		ecall_print_table_dbg(eid, &ret, db_id, rankings_table_id, 359990, 360020);
		ecall_print_table_dbg(eid, &ret, db_id, p_rankings_table_id, 359990, 360020);
	}
#endif

#if defined(TEST_RANKINGS_IN_A_SEPARATE_THREAD)
	// Run rankings test in a separate thread to get fine-grained profiling data
	{
		run_rankings_test(eid, db_id, rankings_table_id);
	}
#endif

#if defined(TEST_COLUMN_SORT_RANKINGS)	
	{
		/* Column sort tests */
		
		unsigned int column;

		// Rankings is 360000 
		//r = 16384;
		//s = 16; 
		column = 2;
		
		printf(TXT_FG_YELLOW "Column sort test" TXT_NORMAL ": sorting rankings table \n"); 
			
		unsigned long long start, end;
		start = RDTSC_START();
		int num_threads = 4;
		
		//ecall_column_sort_table_dbg(eid, &ret, db_id, rankings_table_id, column);
		column_sort_table_parallel(eid, db_id, rankings_table_id, column, num_threads);

		ecall_flush_table(eid, &ret, db_id, rankings_table_id);
		end = RDTSCP();

		printf("Sorting + flushing took %llu cycles\n", end - start);
		ecall_print_table_dbg(eid, &ret, db_id, rankings_table_id, 0, 23);

	}
#endif

#if defined(TEST_JOIN)
	{

		// Join tests

		c.num_conditions = 1; 
		c.table_left = rankings_table_id; 
		c.table_right = udata_table_id; 
		c.fields_left[0] = 0;
		c.fields_right[0] = 1;
		
		printf(TXT_FG_YELLOW "Sort join test" TXT_NORMAL ": joining rankings and udata tables \n"); 

#if defined(TEST_BIN_PACKING_JOIN)	
		ret = bin_packing_join(eid, db_id, &c, rankings_csv, uvisits_csv, &join_table_id);
		// Uncomment this to print the output table
		// ecall_print_table_dbg(eid, &ret, db_id, join_table_id, 0, 1<<20);
#else
		sgx_ret = ecall_join(eid, &ret, db_id, &c, &join_table_id);
#endif
		if (sgx_ret || ret) {
			ERR("join failed, err:%d (sgx ret:%d)\n", 
				ret, sgx_ret);
			goto out; 
		}

		ecall_flush_table(eid, &ret, db_id, join_table_id);
		printf("joined successfully\n");

	}
#endif
	ret = 0;
out:
	ecall_free_db(eid, &ret, db_id); 
	return ret;
}

int test_project_schema(sgx_enclave_id_t eid) {
    int ret;
    ecall_test_project_schema(eid, &ret);
}

int test_pad_schema(sgx_enclave_id_t eid) { 
    int ret;
    ecall_test_pad_schema(eid, &ret);
}

int test_project_row(sgx_enclave_id_t eid) {
    int ret;
    ecall_test_project_row(eid, &ret);
}

void test_barrier_fn(sgx_enclave_id_t eid, unsigned long count, int num_threads, int tid)
{
	int ret;
	ecall_barrier_test(eid, &ret, count, num_threads, tid);
}


void test_barriers(sgx_enclave_id_t eid, int num_threads, unsigned long count)
{
	std::vector<std::thread*> threads;
	assert ((num_threads & (num_threads - 1)) == 0);

	for (auto i = 0u; i < num_threads; i++)
		threads.push_back(new thread(test_barrier_fn, eid, count, num_threads, i));

	for (auto &t : threads)
		t->join();
}

void bitonic_sorter_fn(sgx_enclave_id_t eid, int db_id, int table_id, int field, int tid, int num_threads)
{
	int ret;
	ecall_bitonic_sort_table_parallel(eid, &ret, db_id, table_id, field, tid, num_threads);
}

void bitonic_sort_parallel(sgx_enclave_id_t eid, int db_id, int table_id, int field, int num_threads)
{
	std::vector<std::thread*> threads;
	assert ((num_threads & (num_threads - 1)) == 0);

	for (auto i = 0u; i < num_threads; i++)
		threads.push_back(new thread(bitonic_sorter_fn, eid, db_id, table_id, field, i, num_threads));

	for (auto &t : threads)
		t->join();
}

int test_bitonic_sort(sgx_enclave_id_t eid)
{
	schema_t sc;
	std::string db_name("random-integers");
	std::string table_name("rand_int");
	int i, db_id, table_id, join_table_id, ret;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;
	std::string rand_csv("rand.csv");

	printf(TXT_FG_YELLOW "Starting bitonic sort test" TXT_NORMAL "\n");

	sc = get_predef_schema(RAND_INT_TABLE_ID);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		goto out;
	}


	ret = populate_database_from_csv(rand_csv, RANDINT_TABLE_SIZE, db_id, table_id, &sc, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", rand_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, table_id);
	printf("created random table\n");

#define PRINT_SORTED_TABLE
	{
		int sorted_id;
		unsigned long long start, end;
		start = RDTSC_START();
		auto num_threads = 2u;
		//ecall_sort_table(eid, &ret, db_id, table_id, 0, &sorted_id);

		bitonic_sort_parallel(eid, db_id, table_id, 0, num_threads);
#ifdef CREATE_SORTED_TABLE
		ecall_flush_table(eid, &ret, db_id, sorted_id);
#endif
		ecall_flush_table(eid, &ret, db_id, table_id);
		end = RDTSCP();
		printf("Sorting random table (in-place) + flushing took %llu cycles\n", end - start);
#ifdef PRINT_SORTED_TABLE
		ecall_print_table_dbg(eid, &ret, db_id, table_id, 0, 16);
#endif
	}

	ret = 0;
out:
	ecall_free_db(eid, &ret, db_id); 
	return ret;
}


void column_sort_fn(sgx_enclave_id_t eid, int db_id, int table_id, int field, int tid, int num_threads)
{
	int ret;
	ecall_column_sort_table_parallel(eid, &ret, db_id, table_id, field, tid, num_threads);
}


void column_sort_table_parallel(sgx_enclave_id_t eid, int db_id, int table_id, int field, int num_threads)
{

	std::vector<std::thread*> threads;
	assert ((num_threads & (num_threads - 1)) == 0);

	for (auto i = 0u; i < num_threads; i++) {
		printf("%s, Spawning thread %d\n", __func__, i);
		threads.push_back(new thread(column_sort_fn, eid, db_id, table_id, field, i, num_threads));
	}
	for (auto &t : threads) {
		printf("%s, waiting for join\n", __func__);
		t->join();
		printf("%s, joined thread %p\n", __func__, t);
	}

	return;
}


int test_column_sort(sgx_enclave_id_t eid)
{
	schema_t sc;
	std::string db_name("columnsort-db");
	std::string table_name("columnsort");
	int i, db_id, table_id, ret;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;

	std::string rand_csv("rand.csv");

	printf(TXT_FG_YELLOW "Starting column sort test" TXT_NORMAL "\n"); 

	sc = get_predef_schema(RAND_INT_TABLE_ID);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		goto out;
	}


	ret = populate_database_from_csv(rand_csv, RANDINT_TABLE_SIZE, db_id, table_id, &sc, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", rand_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, table_id);
	
	printf("created columnsort table\n");

	/* Column sort tests */
	{		
		unsigned int column = 0; 
		
		unsigned long long start, end;
		start = RDTSC_START();
		auto num_threads = 4;
		//ecall_column_sort_table_dbg(eid, &ret, db_id, table_id, column);

		column_sort_table_parallel(eid, db_id, table_id, column, num_threads);
		ecall_flush_table(eid, &ret, db_id, table_id);
		end = RDTSCP();
	
		printf("Sorting columnsort table + flushing took %llu cycles\n", end - start);
		ecall_print_table_dbg(eid, &ret, db_id, table_id, 0, 23);
	}
	ret = 0;
out:
	ecall_free_db(eid, &ret, db_id); 
	return ret;
}

void quick_sorter_fn(sgx_enclave_id_t eid, int db_id, int table_id, int field, int tid, int num_threads)
{
	int ret;
	ecall_quicksort_table_parallel(eid, &ret, db_id, table_id, field, tid, num_threads);
}

void quick_sort_parallel(sgx_enclave_id_t eid, int db_id, int table_id, int field, int num_threads)
{
	std::vector<std::thread*> threads;
	assert ((num_threads & (num_threads - 1)) == 0);

	for (auto i = 0u; i < num_threads; i++)
		threads.push_back(new thread(quick_sorter_fn, eid, db_id, table_id, field, i, num_threads));

	for (auto &t : threads)
		t->join();
}

int test_quick_sort(sgx_enclave_id_t eid)
{
	schema_t sc;
	std::string db_name("qsort_test");
	std::string table_name("qsort_rankings");
	int i, db_id, table_id, join_table_id, ret;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;
	std::string rankings_csv("rankings.csv");

	printf(TXT_FG_YELLOW "Starting quick sort test" TXT_NORMAL "\n");

	sc = get_predef_schema(RANKINGS_TABLE_ID);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		goto out;
	}


	ret = populate_database_from_csv(rankings_csv, RANKINGS_TABLE_SIZE, db_id, table_id, &sc, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", rankings_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, table_id);

	printf("created %s table\n", table_name.c_str());

	{
		int sorted_id;
		unsigned long long start, end;
		start = RDTSC_START();
		auto num_threads = 2u;
		ecall_quicksort_table(eid, &ret, db_id, table_id, 1, &sorted_id);

		//quick_sort_parallel(eid, db_id, table_id, 0, num_threads);
#ifdef CREATE_SORTED_TABLE
		ecall_flush_table(eid, &ret, db_id, sorted_id);
#endif
		ecall_flush_table(eid, &ret, db_id, table_id);
		end = RDTSCP();
		printf("Quick Sorting table (in-place) + flushing took %llu cycles\n", end - start);
#ifdef PRINT_SORTED_TABLE
		ecall_print_table_dbg(eid, &ret, db_id, table_id, 0, 16);
#endif
	}

	ret = 0;
out:
	ecall_free_db(eid, &ret, db_id); 
	return ret;
}

int test_merge_sort_write(sgx_enclave_id_t eid)
{
	schema_t sc, sc_udata;
	data_base_t *db;
	std::string db_name("rankings-and-udata");
	std::string table_name("rankings");
	std::string udata_table_name("udata");
	int i, db_id, rankings_table_id, udata_table_id, join_table_id, ret; 
	join_condition_t c;
	std::string uvisits_csv("uservisits.csv");
	std::string rankings_csv("rankings.csv");

	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;

	sc = get_predef_schema(RANKINGS_TABLE_ID);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret; 
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &rankings_table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		goto out; 
	}


	ret = populate_database_from_csv(rankings_csv, RANKINGS_TABLE_SIZE, db_id, rankings_table_id, &sc, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", rankings_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, rankings_table_id);
	printf("created rankings table with db ID:%d | table_id:%d\n",
			db_id, rankings_table_id);

	sc_udata = get_predef_schema(UVISITS_TABLE_ID);

	sgx_ret = ecall_create_table(eid, &ret, db_id, udata_table_name.c_str(), udata_table_name.length(), &sc_udata, &udata_table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d), table:%s\n", 
			ret, sgx_ret, udata_table_name.c_str());
		goto out; 
	}


	ret = populate_database_from_csv(uvisits_csv, UVISITS_TABLE_SIZE, db_id, udata_table_id, &sc_udata, eid);

	if (ret) {
		ERR("populate db from %s error:%d\n", uvisits_csv.c_str(), ret);
		goto out;
	}

	ecall_flush_table(eid, &ret, db_id, udata_table_id);

	printf("created uservisits table with ID:%d | table_id:%d\n",
			db_id, udata_table_id);

	{

		int project_columns_left[3] = {0,1,2};
		int* ptr_project_columns_left = (int*)project_columns_left;
		int num_project_columns_left = sizeof(project_columns_left)/sizeof(project_columns_left[0]);
		int promote_columns_left[1] = {0};
		int* ptr_promote_columns_left = (int*)promote_columns_left;
		int num_pad_bytes_left = max(row_size(&sc),row_size(&sc_udata))-row_size(&sc);

		int project_columns_right[9] = {0,1,2,3,4,5,6,7,8};
		int* ptr_project_columns_right = (int*)project_columns_right;
		int num_project_columns_right = sizeof(project_columns_right)/sizeof(project_columns_right[0]);
		int promote_columns_right[1] = {1};
		int* ptr_promote_columns_right = (int*)promote_columns_right;
		// unsigned long -> int conversion
		int num_pad_bytes_right = max(row_size(&sc),row_size(&sc_udata))-row_size(&sc_udata);

		printf("set up input for the algorithm\n");

//#define PRINT_APPEND_WRITE_TABLE
#define	CREATE_APPEND_TABLE
/// Create multiple threads
		int	write_table_id;
		unsigned long long start, end;
		start = RDTSC_START();
		auto num_threads = 2u;

		ecall_merge_and_sort_and_write(eid, &ret, db_id,
			rankings_table_id,
			ptr_project_columns_left,
			num_project_columns_left,
			ptr_promote_columns_left,
			num_pad_bytes_left,
			udata_table_id,
			ptr_project_columns_right,
			num_project_columns_right,
			ptr_promote_columns_right,
			num_pad_bytes_right,
			&write_table_id);

		//merge_and_sort_and_write_parallel(eid, db_id, table_id, 0, num_threads);
#ifdef CREATE_APPEND_TABLE
		ecall_flush_table(eid, &ret, db_id, write_table_id);
#endif

		end = RDTSCP();
		printf("merge sort write table (in-place) + flushing took %llu cycles\n", end - start);

#ifdef PRINT_APPEND_WRITE_TABLE
		ecall_print_table_dbg(eid, &ret, db_id, write_table_id, 0, 16);
#endif
	}
	ret = 0;
out:
	ecall_free_db(eid, &ret, db_id); 
	return ret;	
}


