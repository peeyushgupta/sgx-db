#pragma once 

const unsigned long FREQ = 2200;

const unsigned long long cycles_per_sec = FREQ*1000*1000;
const unsigned long long cycles_per_msec = FREQ*1000;
const unsigned long long cycles_per_usec = FREQ;

static inline
uint64_t RDTSC_START ( void )
{

	unsigned cycles_low, cycles_high;

	asm volatile ( "CPUID\n\t"
				   "RDTSC\n\t"
				   "mov %%edx, %0\n\t"
				   "mov %%eax, %1\n\t"
				   : "=r" (cycles_high), "=r" (cycles_low)::
				   "%rax", "%rbx", "%rcx", "%rdx");

	return ((uint64_t) cycles_high << 32) | cycles_low;
}

/**
 * CITE: http://www.intel.com/content/www/us/en/embedded/training/ia-32-ia-64-benchmark-code-execution-paper.html
 */
static inline
uint64_t RDTSCP ( void )
{
	unsigned cycles_low, cycles_high;

	asm volatile( "RDTSCP\n\t"
				  "mov %%edx, %0\n\t"
				  "mov %%eax, %1\n\t"
				  "CPUID\n\t": "=r" (cycles_high), "=r" (cycles_low)::
				  "%rax", "%rbx", "%rcx", "%rdx");
	
	return ((uint64_t) cycles_high << 32) | cycles_low;
}

/**
 * This function returns a time stamp 
 */
static inline
uint64_t RDTSC( void )
{
	unsigned int low, high;

	asm volatile("rdtsc" : "=a" (low), "=d" (high));

	return low | ((uint64_t)high) << 32;	
}
/**
 * This function returns the average time spent collecting timestamps.
 */
static inline
uint64_t fipc_test_time_get_correction ( void )
{
	register uint64_t start;
	register uint64_t end;
	register uint64_t sum;
	register uint64_t i;

	for ( sum = 0, i = 0; i < 100000; ++i )
	{
		start = RDTSC_START();
		end   = RDTSCP();
		sum  += end - start;
	}

	return sum / i;
}


/**
 * This function waits for atleast ticks clock cycles.
 */
static inline
void wait_ticks ( uint64_t ticks )
{
		uint64_t current_time;
		uint64_t time = RDTSC();
		time += ticks;
		do
		{
			current_time = RDTSC();
		}
		while ( current_time < time );
}


