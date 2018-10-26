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

