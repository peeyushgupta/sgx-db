#pragma once

#include <string>

// Mutual exclusion lock.
struct spinlock {
	unsigned int locked;  // Is the lock held?
	
	// For debugging:
	std::string name;           // Name of lock.
#if defined(DEBUG_SPINLOCKS)
	struct cpu *cpu;      // The cpu holding the lock.
	unsigned int pcs[10]; // The call stack (an array of program counters)
				// that locked the lock.
#endif
};

void initlock(struct spinlock *lk, std::string name);
void acquire(struct spinlock *lk);
void release(struct spinlock *lk);

typedef struct {
	volatile unsigned int count;  // How many threads arrived at the barrier
	volatile unsigned int global_sense;
} barrier_t;

void barrier_init(barrier_t *b);
void barrier_wait(barrier_t *b, volatile unsigned int *local_sense, unsigned int tid, unsigned int num_threads);
