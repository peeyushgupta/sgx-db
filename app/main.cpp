#include "db-tests.hpp"
#include "dbg.hpp"
#include "apputil.hpp"
#include <cstdio>

/* Global EID shared by multiple threads */
sgx_enclave_id_t eid = 0;

# define MAX_PATH FILENAME_MAX
# define ENCLAVE_FILENAME "enclave.signed.so"

int main(){
	char token_path[MAX_PATH] = {'\0'};
	sgx_launch_token_t token = {0};
 	sgx_status_t ret = SGX_ERROR_UNEXPECTED;
 	int updated = 0;
 
	ret = sgx_create_enclave(ENCLAVE_FILENAME, 
				SGX_DEBUG_FLAG, 
				&token, 
				&updated, 
				&eid, NULL);
	if (ret != SGX_SUCCESS) {
		ERR("Failed to create SGX enclave:%d\n", ret);
		print_sgx_error(ret);
		return -1;
    	}

	DBG("Created enclave... starting DB tests\n");

#if defined(TEST_SPINLOCK)
	test_spinlock_inc(eid, 1000000);
#endif
#if defined(TEST_PROJECTIONS)
        test_project_schema(eid);
        test_pad_schema(eid);
        test_project_row(eid);
#endif
#if defined(TEST_BARRIERS)
	// 8 threads, 50 iterations
	test_barriers(eid, 8, 50);
#endif
#if defined(OCALL_ECALL_TESTS)
	test_null_ocalls(eid);
#endif

#if defined(TEST_THREADS)
	test_threads(eid);
#endif

#if defined(TEST_COLUMN_SORT_16)
	test_column_sort(eid);
#endif

#if defined(TEST_BITONIC)
	test_bitonic_sort(eid);
#endif

#if defined(TEST_QUICKSORT)
	test_quick_sort(eid);
#endif

	/* Launch a collection of tests inside that require
	   rankings and udata tables */
	test_rankings(eid);
	
	/* Destroy the enclave */
	sgx_destroy_enclave(eid);
 
	return 0;
}
