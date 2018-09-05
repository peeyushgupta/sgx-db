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

	test_rankings(eid); 


	/* Destroy the enclave */
	sgx_destroy_enclave(eid);
 
	return 0;
}
