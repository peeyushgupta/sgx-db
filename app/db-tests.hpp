#pragma once
#include "sgx_urts.h"

void test_spinlock_inc(sgx_enclave_id_t eid, unsigned long count);
int test_null_ocalls(sgx_enclave_id_t eid); 
int test_rankings(sgx_enclave_id_t eid);
int test_threads(sgx_enclave_id_t eid);
int test_bitonic_sort(sgx_enclave_id_t eid);
int test_column_sort(sgx_enclave_id_t eid);

