#pragma once
#include "sgx_urts.h"

int test_null_ocalls(sgx_enclave_id_t eid); 
int test_rankings(sgx_enclave_id_t eid);
int test_threads(sgx_enclave_id_t eid);
int test_bitonic_sort(sgx_enclave_id_t eid);
