// Mutual exclusion spin locks.

#include "spinlock.hpp"
#include "x86.hpp"

void initlock(struct spinlock *lk, std::string name)
{
	lk->name = name;
	lk->locked = 0;
#if defined(DEBUG_SPINLOCKS)
	lk->cpu = 0;
#endif
}

// Acquire the lock.
// Loops (spins) until the lock is acquired.
// Holding a lock for a long time may cause
// other CPUs to waste time spinning to acquire it.
void acquire(struct spinlock *lk)
{
#if defined(DEBUG_SPINLOCKS)
	assert(!holding(lk));
#endif
	// The xchg is atomic.
	while(xchg(&lk->locked, 1) != 0)
		;

	// Tell the C compiler and the processor to not move loads or stores
	// past this point, to ensure that the critical section's memory
	// references happen after the lock is acquired.
	__sync_synchronize();

#if defined(DEBUG_SPINLOCKS)
	// Record info about lock acquisition for debugging.
	lk->cpu = mycpu();
	getcallerpcs(&lk, lk->pcs);
#endif
}

// Release the lock.
void release(struct spinlock *lk)
{

#if defined(DEBUG_SPINLOCKS)
	assert(holding(lk));

	lk->pcs[0] = 0;	
	lk->cpu = 0;
#endif

	// Tell the C compiler and the processor to not move loads or stores
	// past this point, to ensure that all the stores in the critical
	// section are visible to other cores before the lock is released.
	// Both the C compiler and the hardware may re-order loads and
	// stores; __sync_synchronize() tells them both not to.
	__sync_synchronize();

	// Release the lock, equivalent to lk->locked = 0.
	// This code can't use a C assignment, since it might
	// not be atomic. A real OS would use C atomics here.
	asm volatile("movl $0, %0" : "+m" (lk->locked) : );

}

#if defined(DEBUG_SPINLOCKS)

// Record the current call stack in pcs[] by following the %ebp chain.
void getcallerpcs(void *v, uint pcs[])
{
	uint *ebp;
 	int i;

	ebp = (uint*)v - 2;
	for(i = 0; i < 10; i++){
		if(ebp == 0 || ebp < (uint*)KERNBASE || ebp == (uint*)0xffffffff)
      			break;
		pcs[i] = ebp[1];     // saved %eip
		ebp = (uint*)ebp[0]; // saved %ebp
	}
	for(; i < 10; i++)
		pcs[i] = 0;
}

// Check whether this cpu is holding the lock.
int holding(struct spinlock *lock)
{
	return lock->locked && lock->cpu == mycpu();
}

#endif /* DEBUG_SPINLOCKS */
void barrier_init(volatile barrier_t *b) {
	b->count = 0; 
	b->seen = 0; 
	return;
}

void barrier_wait(volatile barrier_t *b, unsigned int num_threads) {
	__sync_fetch_and_add(&b->count, 1);
	while (b->count != num_threads)
		;
	__sync_fetch_and_add(&b->seen, 1); 
	return;
}

void barrier_reset(volatile barrier_t *b, unsigned int num_threads) {
	while (b->seen != num_threads)
		;
	b->count = 0; 
	b->seen = 0; 
	return;
}


