#include "spinlock.hpp"
#include "util.hpp"
#include "time.hpp"
#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif


#define ECALL_TEST_LENGTH 10000

struct spinlock s_inc = {.locked = 0};
struct two_numbers {
	unsigned long a; 
	unsigned long b; 
};
struct two_numbers tn = {.a = 0, .b = 0};


int ecall_spinlock_inc(unsigned long count) {

	int passed = 0; 

	printf("%s: First test without lock to see if we get a race\n", __func__); 
	for (unsigned long i = 0; i < count; i++) {
		tn.a ++; 
		tn.b ++; 	
	}

	printf("%s: a:%lu, b:%lu (%s)\n", 
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
	printf("%s: a:%lu, b:%lu (%s)\n", 
		__func__, tn.a, tn.b, tn.a == tn.b? "passed" : "failed"); 
	release(&s_inc); 
	return passed; 
}

void ecall_null_ecall() {
	return; 
}

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
