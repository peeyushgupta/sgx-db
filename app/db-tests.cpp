#include "db-tests.hpp"
#include "db.hpp"
#include <string>
#include <string.h>
#include <fstream>
#include <sstream>
#include "enclave_u.h" // Headers for untrusted part (autogenerated by edger8r)
#include <time.hpp>
#include <thread>

using namespace std;
#define OCALL_TEST_LENGTH 10000

/* Test timings for ocalls */
int test_null_ocalls(sgx_enclave_id_t eid) {

	unsigned long long start, end; 

	printf("Testing: null ecall for %d iterations\n", OCALL_TEST_LENGTH);
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
	printf("Creating threads\n");
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

	printf("%s:starting buffer cache test\n", __func__); 

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
	sc.row_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

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
	printf("created rankings table\n");

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

	sc_udata.row_size = sc_udata.offsets[sc_udata.num_fields - 1] + sc_udata.sizes[sc_udata.num_fields - 1];

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
	printf("created uservisits table\n");

#if defined(TABLE_SCAN_TESTS)
	for (i = 0; i < 10; i++) {
		ecall_scan_table_dbg(eid, &ret, db_id, rankings_table_id);
	}

	for (i = 0; i < 10; i++) {
		ecall_scan_table_dbg(eid, &ret, db_id, udata_table_id);
	}
#endif

	/* Buffer cache testst */
	{	
		bcache_test(eid, db_id, rankings_table_id);
	}

	/* Promote column tests */
	{
		int p_rankings_table_id;
		ecall_promote_table_dbg(eid, &ret, db_id, rankings_table_id, 1, &p_rankings_table_id);
		ecall_flush_table(eid, &ret, db_id, p_rankings_table_id);
		printf("promoted rankings table\n");
		ecall_print_table_dbg(eid, &ret, db_id, rankings_table_id, 359990, 360020);
		ecall_print_table_dbg(eid, &ret, db_id, p_rankings_table_id, 359990, 360020);
	}


	c.num_conditions = 1; 
	c.table_left = rankings_table_id; 
	c.table_right = udata_table_id; 
	c.fields_left[0] = 1;
	c.fields_right[0] = 2;

	sgx_ret = ecall_join(eid, &ret, db_id, &c, &join_table_id);
	if (sgx_ret || ret) {
		ERR("join failed, err:%d (sgx ret:%d)\n", 
			ret, sgx_ret);
		return ret; 
	}

	ecall_flush_table(eid, &ret, db_id, join_table_id);
	printf("joined successfully\n");
	return 0;

}

void bitonic_sorter_fn(sgx_enclave_id_t eid, int db_id, int table_id, int field, int tid) 
{
	int ret;
	ecall_sort_table_parallel(eid, &ret, db_id, table_id, field, tid);
}

void bitonic_sort_parallel(sgx_enclave_id_t eid, int db_id, int table_id, int field)
{
	thread t1(bitonic_sorter_fn, eid, db_id, table_id, field, 0);
	thread t2(bitonic_sorter_fn, eid, db_id, table_id, field, 1);
	t1.join();
	t2.join();
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

	sc.num_fields = 1;
	sc.offsets[0] = 0;
	sc.sizes[0] = 4;
	sc.types[0] = INTEGER;
	sc.row_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

	//row = (uint7_t*)malloc(sc.row_size);
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

	for(int i = 0; i < 8; i++) {

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
	printf("created random table\n");
#define PRINT_SORTED_TABLE
	{
		int sorted_id;
		unsigned long long start, end;
		start = RDTSC_START();
		//ecall_sort_table(eid, &ret, db_id, table_id, 0, &sorted_id);

		bitonic_sort_parallel(eid, db_id, table_id, 0);
#ifdef CREATE_SORTED_TABLE
		ecall_flush_table(eid, &ret, db_id, sorted_id);
#endif
		ecall_flush_table(eid, &ret, db_id, table_id);
		end = RDTSCP();
		printf("Sorting random table (in-place) + flushing took %llu cycles\n", end - start);
#ifdef PRINT_SORTED_TABLE
		ecall_print_table_dbg(eid, &ret, db_id, table_id, 0, 8);
#endif
	}

	return 0;
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

	sc.num_fields = 1;
	sc.offsets[0] = 0;
	sc.sizes[0] = 4;
	sc.types[0] = INTEGER;
	sc.row_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

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
		
	unsigned int r, s, column;

	r = 8;
	s = 2; 
	column = 0; 
		
	unsigned long long start, end;
	start = RDTSC_START();

	ecall_column_sort_table_dbg(eid, &ret, db_id, table_id, r, s, column);
	ecall_flush_table(eid, &ret, db_id, table_id);
	end = RDTSCP();
	printf("Sorting columnsort table + flushing took %llu cycles\n", end - start);
	ecall_print_table_dbg(eid, &ret, db_id, table_id, 0, 23);

	return 0;
}
