#include <time.hpp>

unsigned long long RDTSC_START ( void )
{

	unsigned cycles_low, cycles_high;

	asm volatile ( "CPUID\n\t"
				   "RDTSC\n\t"
				   "mov %%edx, %0\n\t"
				   "mov %%eax, %1\n\t"
				   : "=r" (cycles_high), "=r" (cycles_low)::
				   "%rax", "%rbx", "%rcx", "%rdx");

	return ((unsigned long long) cycles_high << 32) | cycles_low;
}

/**
 * CITE: http://www.intel.com/content/www/us/en/embedded/training/ia-32-ia-64-benchmark-code-execution-paper.html
 */
unsigned long long RDTSCP ( void )
{
	unsigned cycles_low, cycles_high;

	asm volatile( "RDTSCP\n\t"
				  "mov %%edx, %0\n\t"
				  "mov %%eax, %1\n\t"
				  "CPUID\n\t": "=r" (cycles_high), "=r" (cycles_low)::
				  "%rax", "%rbx", "%rcx", "%rdx");
	
	return ((unsigned long long) cycles_high << 32) | cycles_low;
}

/**
 * This function returns a time stamp 
 */
unsigned long long RDTSC( void )
{
	unsigned int low, high;

	asm volatile("rdtsc" : "=a" (low), "=d" (high));

	return low | ((unsigned long long)high) << 32;	
}

/**
 * This function returns the average time spent collecting timestamps.
 */
static inline
unsigned long long fipc_test_time_get_correction ( void )
{
	unsigned long long start;
	unsigned long long end;
	unsigned long long sum;
	unsigned long long i;

	for ( sum = 0, i = 0; i < 100000; ++i )
	{
		start = RDTSC_START();
		end   = RDTSCP();
		sum  += end - start;
	}

	return sum / i;
}


