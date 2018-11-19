#include "db-tests.hpp"
#include "db.hpp"
#include <string>
#include <string.h>
#include <fstream>
#include <sstream>
#include "enclave_u.h" // Headers for untrusted part (autogenerated by edger8r)
#include <time.hpp>
#include <thread>
#include <cassert>
#include <vector>
#include <assert.h>

using namespace std;
#define OCALL_TEST_LENGTH 10000

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

	bcache_test_run_threads(eid, db_id, from_table_id, to_table_id);

	sgx_ret = ecall_bcache_test_cmp_read_write(eid, &ret, db_id, from_table_id, to_table_id);
	if (sgx_ret || ret) {
		ERR("error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	printf("%s:buffer cache test passed\n", __func__); 
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
	int i, db_id, rankings_table_id, udata_table_id, join_table_id, ret; 
	join_condition_t c;
	char line[MAX_ROW_SIZE]; 
	char data[MAX_ROW_SIZE];
	uint8_t *row;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;

	sc.num_fields = 4;
	sc.offsets[0] = 0;
	sc.sizes[0] = 1;
	sc.types[0] = CHARACTER;
	sc.offsets[1] = 1;
	sc.sizes[1] = 255;
	sc.types[1] = TINYTEXT;
	sc.offsets[2] = 256;
	sc.sizes[2] = 4;
	sc.types[2] = INTEGER;
	sc.offsets[3] = 260;
	sc.sizes[3] = 4;
	sc.types[3] = INTEGER;
	sc.row_data_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

	//row = (uint8_t*)malloc(sc.row_size);
	row = (uint8_t*)malloc(MAX_ROW_SIZE);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret; 
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &rankings_table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret; 
	}

	std::ifstream file("rankings.csv");

	row[0] = 'a';
	for(int i = 0; i < 360000; i++) { 
	//for(int i = 0; i < 10000; i++) { 

		memset(row, 'a', MAX_ROW_SIZE);
		file.getline(line, MAX_ROW_SIZE); //get the field

		std::istringstream ss(line);
		for(int j = 1; j < sc.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')) {
				ERR("something is wrong with data (skipping):%s\n", line);
				break;
			}
			if(sc.types[j] == INTEGER) {
				int d = 0;
				d = atoi(data);
				memcpy(&row[sc.offsets[j]], &d, 4);
			} else if (sc.types[j] == TINYTEXT) {
				strncpy((char*)&row[sc.offsets[j]], data, strlen(data) + 1);
			}
		}

		//DBG_ON(VERBOSE_INSERT, "insert row:%s\n", (char*)row); 
	
		sgx_ret = ecall_insert_row_dbg(eid, &ret, db_id, rankings_table_id, row);
		if (sgx_ret) {
			ERR("insert row:%d, err:%d (sgx ret:%d)\n", i, ret, sgx_ret);
			return ret; 
		}
		
	
	}

	ecall_flush_table(eid, &ret, db_id, rankings_table_id);
	printf("created rankings table with db ID:%d | table_id:%d\n",
			db_id, rankings_table_id);

	sc_udata.num_fields = 10;
	sc_udata.offsets[0] = 0;
	sc_udata.sizes[0] = 1;
	sc_udata.types[0] = CHARACTER;
	sc_udata.offsets[1] = 1;
	sc_udata.sizes[1] = 255;
	sc_udata.types[1] = TINYTEXT;
	sc_udata.offsets[2] = 256;
	sc_udata.sizes[2] = 255;
	sc_udata.types[2] = TINYTEXT;
	sc_udata.offsets[3] = 511;
	sc_udata.sizes[3] = 4;
	sc_udata.types[3] = INTEGER;
	sc_udata.offsets[4] = 515;
	sc_udata.sizes[4] = 4;
	sc_udata.types[4] = INTEGER;
	sc_udata.offsets[5] = 519;
	sc_udata.sizes[5] = 255;
	sc_udata.types[5] = TINYTEXT;
	sc_udata.offsets[6] = 774;
	sc_udata.sizes[6] = 255;
	sc_udata.types[6] = TINYTEXT;
	sc_udata.offsets[7] = 1029;
	sc_udata.sizes[7] = 255;
	sc_udata.types[7] = TINYTEXT;
	sc_udata.offsets[8] = 1284;
	sc_udata.sizes[8] = 255;
	sc_udata.types[8] = TINYTEXT;
	sc_udata.offsets[9] = 1539;
	sc_udata.sizes[9] = 4;
	sc_udata.types[9] = INTEGER;

	sc_udata.row_data_size = sc_udata.offsets[sc_udata.num_fields - 1] + sc_udata.sizes[sc_udata.num_fields - 1];

	sgx_ret = ecall_create_table(eid, &ret, db_id, udata_table_name.c_str(), udata_table_name.length(), &sc_udata, &udata_table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d), table:%s\n", 
			ret, sgx_ret, udata_table_name.c_str());
		return ret; 
	}

	std::ifstream file2("uservisits.csv");

	row[0] = 'a';
	for(int i = 0; i < 350000; i++){//TODO temp really 350000
	//for(int i = 0; i < 10000; i++){//TODO temp really 350000
	
		memset(row, 'a', MAX_ROW_SIZE);
		file2.getline(line, MAX_ROW_SIZE);//get the field

		std::istringstream ss(line);

		for(int j = 1; j < sc_udata.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')){
				break;
			}
			if(sc_udata.types[j] == INTEGER) {
				int d = 0;
				d = atoi(data);
				memcpy(&row[sc_udata.offsets[j]], &d, 4);
			} else if (sc_udata.types[j] == TINYTEXT) {
				strncpy((char*)&row[sc_udata.offsets[j]], data, strlen(data) + 1);
			}
		}

		sgx_ret = ecall_insert_row_dbg(eid, &ret, db_id, udata_table_id, row);
		if (sgx_ret || ret) {
			ERR("insert row:%d into %s, err:%d (sgx ret:%d)\n", 
				i, udata_table_name.c_str(), ret, sgx_ret);
			return ret; 
		}

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
		column = 1;
		
		printf(TXT_FG_YELLOW "Column sort test" TXT_NORMAL ": sorting rankings table \n"); 
			
		unsigned long long start, end;
		start = RDTSC_START();
		auto num_threads = 4;
		
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
		c.fields_left[0] = 1;
		c.fields_right[0] = 2;
		
		printf(TXT_FG_YELLOW "Sort join test" TXT_NORMAL ": joining rankings and udata tables \n"); 

	
		sgx_ret = ecall_join(eid, &ret, db_id, &c, &join_table_id);
		if (sgx_ret || ret) {
			ERR("join failed, err:%d (sgx ret:%d)\n", 
				ret, sgx_ret);
			return ret; 
		}

		ecall_flush_table(eid, &ret, db_id, join_table_id);
		printf("joined successfully\n");

	}
#endif
	return 0;

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
	ecall_sort_table_parallel(eid, &ret, db_id, table_id, field, tid, num_threads);
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
	char line[MAX_ROW_SIZE] = {0};
	char data[MAX_ROW_SIZE] = {0};
	uint8_t *row;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;

	printf(TXT_FG_YELLOW "Starting bitonic sort test" TXT_NORMAL "\n");

	sc.num_fields = 1;
	sc.offsets[0] = 0;
	sc.sizes[0] = 4;
	sc.types[0] = INTEGER;
	sc.row_data_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

	row = (uint8_t*)malloc(MAX_ROW_SIZE);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	std::ifstream file("rand.csv");

	for(auto i = 0u; i < 256; i++) {

		memset(row, 0x0, MAX_ROW_SIZE);
		file.getline(line, MAX_ROW_SIZE); //get the field
		std::istringstream ss(line);
		for(auto j = 0u; j < sc.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')) {
				ERR("something is wrong with data (skipping):%s\n", line);
				break;
			}
			if(sc.types[j] == INTEGER) {
				auto d = atoi(data);
				memcpy(&row[sc.offsets[j]], &d, 4);
			}
		}

		sgx_ret = ecall_insert_row_dbg(eid, &ret, db_id, table_id, row);
		if (sgx_ret) {
			ERR("insert row:%d, err:%d (sgx ret:%d)\n", i, ret, sgx_ret);
			return ret;
		}
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

	return 0;
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
	char line[MAX_ROW_SIZE] = {0};
	char data[MAX_ROW_SIZE] = {0};
	uint8_t *row;
	sgx_status_t sgx_ret = SGX_ERROR_UNEXPECTED;


	printf(TXT_FG_YELLOW "Starting column sort test" TXT_NORMAL "\n"); 

	sc.num_fields = 1;
	sc.offsets[0] = 0;
	sc.sizes[0] = 4;
	sc.types[0] = INTEGER;
	sc.row_data_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

	row = (uint8_t*)malloc(MAX_ROW_SIZE);

	sgx_ret = ecall_create_db(eid, &ret, db_name.c_str(), db_name.length(), &db_id);
	if (sgx_ret || ret) {
		ERR("create db error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	sgx_ret = ecall_create_table(eid, &ret, db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (sgx_ret || ret) {
		ERR("create table error:%d (sgx ret:%d)\n", ret, sgx_ret);
		return ret;
	}

	std::ifstream file("columnsort.csv");

	for(int i = 0; i < 16; i++) {

		memset(row, 0x0, MAX_ROW_SIZE);
		file.getline(line, MAX_ROW_SIZE); //get the field
		std::istringstream ss(line);
		for(int j = 0; j < sc.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')) {
				ERR("something is wrong with data (skipping):%s\n", line);
				break;
			}
			if(sc.types[j] == INTEGER) {
				int d = 0;
				d = atoi(data);
				//printf("%s, row %d | data %s : %d\n", __func__, i, data, atoi(data));
				memcpy(&row[sc.offsets[j]], &d, 4);
			}
		}

		sgx_ret = ecall_insert_row_dbg(eid, &ret, db_id, table_id, row);
		if (sgx_ret) {
			ERR("insert row:%d, err:%d (sgx ret:%d)\n", i, ret, sgx_ret);
			return ret;
		}
	}

	ecall_flush_table(eid, &ret, db_id, table_id);
	
	printf("created columnsort table\n");

	/* Column sort tests */
		
	unsigned int column;

	column = 0; 
		
	unsigned long long start, end;
	start = RDTSC_START();
	auto num_threads = 4;
	//ecall_column_sort_table_dbg(eid, &ret, db_id, table_id, column);

	column_sort_table_parallel(eid, db_id, table_id, column, num_threads);
	ecall_flush_table(eid, &ret, db_id, table_id);
	end = RDTSCP();
	
	printf("Sorting columnsort table + flushing took %llu cycles\n", end - start);
	ecall_print_table_dbg(eid, &ret, db_id, table_id, 0, 23);

	return 0;
}


