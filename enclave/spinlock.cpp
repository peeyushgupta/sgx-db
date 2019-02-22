// Mutual exclusion spin locks.

#include "spinlock.hpp"
#include "x86.hpp"
#include "util.hpp"
#include "dbg.hpp"

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

void barrier_init(barrier_t *b) {
	b->count = 0;
	b->global_sense = 0;
	return;
}

#ifdef NDEBUG	// if NDEBUG is defined, use release version
void barrier_wait(barrier_t *b, volatile unsigned int *local_sense, unsigned int tid, unsigned int num_threads) {
	// flip local_sense
	*local_sense = !(*local_sense);

	// XXX: add and fetch
	// equivalent to atomic ++b->count == num_threads
	if (__sync_add_and_fetch(&b->count, 1) == num_threads) {
		// reset the barrier
		b->count = 0;
		b->global_sense = *local_sense;
	} else {
		// Wait until global_sense is equal to local_sense of the waiting thread
		while (b->global_sense != *local_sense) ;
	}
	return;
}
#else
void barrier_wait(barrier_t *b, volatile unsigned int *local_sense, unsigned int tid, unsigned int num_threads) {
	// flip local_sense
	*local_sense = !(*local_sense);
	DBG("%s, tid %d , local_sense %d @ %p | global_sense %d | num_threads"
			" %d\n", __func__, tid, *local_sense, local_sense,
			b->global_sense, num_threads);

	// XXX: add and fetch
	// equivalent to atomic ++b->count == num_threads
	if (__sync_add_and_fetch(&b->count, 1) == num_threads) {
		// reset the barrier
		b->count = 0;
		b->global_sense = *local_sense;

		DBG("%s, tid %d resetting count = %d | global_sense %d\n",
				__func__, tid, b->count, b->global_sense);

	} else {
		DBG("%s, tid %d waiting at global_sense %d | local_sense %d\n",
				__func__, tid, b->global_sense, *local_sense);
		// Wait until global_sense is equal to local_sense of the waiting thread
		while (b->global_sense != *local_sense) ;

		DBG("%s, tid %d leaves barrier global_sense %d | local_sense %d\n",
				__func__, tid, b->global_sense,	*local_sense);
	}
	return;
}
#endif
