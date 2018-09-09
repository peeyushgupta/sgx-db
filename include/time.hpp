#pragma once 

const unsigned long FREQ = 2200;

const unsigned long long cycles_per_sec = FREQ*1000*1000;
const unsigned long long cycles_per_msec = FREQ*1000;
const unsigned long long cycles_per_usec = FREQ;

unsigned long long RDTSC_START ( void );
unsigned long long RDTSCP ( void );
unsigned long long RDTSC( void );

/**
 * This function waits for atleast ticks clock cycles.
 */
static inline
void wait_ticks ( unsigned long long ticks )
{
		unsigned long long current_time;
		unsigned long long time = RDTSC();
		time += ticks;
		do
		{
			current_time = RDTSC();
		}
		while ( current_time < time );
}


