#include "enclave_t.h"

#include "util.hpp"

#define CHECK_RET_STATUS(func) 				\
		do {					\
			if ((func) != SGX_SUCCESS)	\
				abort();		\
		} while(0)				\
/* 
 * printf: 
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */
void printf(const char *fmt, ...)
{
    char buf[BUFSIZ] = {'\0'};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    CHECK_RET_STATUS(ocall_print_string(buf));
}


unsigned long long RDTSC( void ) {
	unsigned long long tsc;
	CHECK_RET_STATUS(ocall_rdtsc(&tsc));
	return tsc;
};


