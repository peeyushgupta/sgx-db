#include "db.hpp"
#include "util.hpp"
#include "dbg.hpp"
#include "time.hpp"
#include "obli.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#include <cmath>

extern thread_local int thread_id;

const int ASCENDING  = 1;
const int DESCENDING = 0;

/** INLINE procedure exchange() : pair swap **/
inline int exchange(table_t *tbl, int i, int j, row_t *row_i, row_t *row_j, int tid) {
	row_t *row_tmp;
	row_t row_tmp_stack;
	row_tmp = &row_tmp_stack;

	memcpy(row_tmp, row_i, row_size(tbl));

	if(tbl->pinned_blocks) {
		memcpy(row_i, row_j, row_size(tbl));
		memcpy(row_j, row_tmp, row_size(tbl));
	} else {
		write_row_dbg(tbl, row_j, i);
		write_row_dbg(tbl, row_tmp, j);
	}
	return 0;
}

/** procedure compare() 
   The parameter dir indicates the sorting direction, ASCENDING 
   or DESCENDING; if (a[i] > a[j]) agrees with the direction, 
   then a[i] and a[j] are interchanged.
**/
int compare_and_exchange(table_t *tbl, int column, int i, int j, int dir, int tid) {
	int val_i, val_j;
	row_t *row_i, *row_j;
	data_block_t *b_i, *b_j;

	// FIXME: if tables are pinned, this stack allocation is not needed
#if defined(ALIGNMENT)
	__attribute__((aligned(ALIGNMENT))) 
#endif 
	row_t row_i_stack;
#if defined(ALIGNMENT)
	__attribute__((aligned(ALIGNMENT))) 
#endif	
	row_t row_j_stack;
	row_i = &row_i_stack;
	row_j = &row_j_stack;

	if(tbl->pinned_blocks) {
		data_block_t *b_i, *b_j; 
		get_pinned_row(tbl, i, &b_i, &row_i); 
		get_pinned_row(tbl, j, &b_j, &row_j);
	} else {
		read_row(tbl, i, row_i);
		read_row(tbl, j, row_j);
	}

#ifdef OBLI_XCHG
	bool cond = (dir == compare_rows(&tbl->sc, column, row_i, row_j));
	obli_cswap((u8*) row_i, (u8*) row_j, row_size(tbl), cond);

	// XXX: Is this required?
	if (!tbl->pinned_blocks) {
		write_row_dbg(tbl, row_i, i);
		write_row_dbg(tbl, row_j, j);
	}
#else
	if (dir == compare_rows(&tbl->sc, column, row_i, row_j)) {
		exchange(tbl, i, j, row_i, row_j, tid);
	}
#endif

#ifdef LOCAL_ALLOC
	free(row_i);
	free(row_j);
#endif
	return 0;
}

/** Procedure bitonicMerge() 
   It recursively sorts a bitonic sequence in ascending order, 
   if dir = ASCENDING, and in descending order otherwise. 
   The sequence to be sorted starts at index position lo,
   the parameter cbt is the number of elements to be sorted. 
 **/
void bitonicMerge(table_t *tbl, int lo, int cnt, int column, int dir, int tid) {
	if (cnt > 1) {
		int k = cnt / 2;
		int i;
		for (i = lo; i < lo + k; i++)
			compare_and_exchange(tbl, column, i, i + k, dir, tid);
		bitonicMerge(tbl, lo, k, column, dir, tid);
		bitonicMerge(tbl, lo + k, k, column, dir, tid);
	}
}



/** function recBitonicSort() 
    first produces a bitonic sequence by recursively sorting 
    its two halves in opposite sorting orders, and then
    calls bitonicMerge to make them in the same order 
 **/
void recBitonicSort(table_t *tbl, int lo, int cnt, int column, int dir, int tid) {
	if (cnt > 1) {
		int k = cnt / 2;
		recBitonicSort(tbl, lo, k, column, ASCENDING, tid);
		recBitonicSort(tbl, lo + k, k, column, DESCENDING, tid);
		bitonicMerge(tbl, lo, cnt, column, dir, tid);
	}
}


int bitonic_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl) {
	int ret = 0;

#ifdef CREATE_SORTED_TABLE
	std::string s_tbl_name;  
        schema_t p_sc;

	s_tbl_name = "s:" + tbl->name; 

	ret = create_table(db, s_tbl_name, &p_sc, p_tbl);
	if (ret) {
		ERR("create table:%d\n", ret);
		return ret; 
	}

	DBG("Created sorted table %s, id:%d\n", 
            s_tbl_name.c_str(), s_tbl->id); 
#endif
	recBitonicSort(tbl, 0, tbl->num_rows, column, ASCENDING, 0);

#ifdef CREATE_SORTED_TABLE
	bflush(*p_tbl);
#endif
	return ret; 
}

int ecall_sort_table(int db_id, int table_id, int field, int *sorted_id) {
	int ret; 
	data_base_t *db;
	table_t *table, *s_table;

	if (!(db = get_db(db_id)))
		return -1; 
	
	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = bitonic_sort_table(db, table, field, &s_table); 

#ifdef CREATE_SORTED_TABLE
	*sorted_id = s_table->id; 
#endif
	return ret; 
}

int print_table_dbg(table_t *table, int start, int end);

int bitonicSplit(table_t *tbl, int start_i, int start_j, int count, int column, int dir, int tid)
{
	for (int i = start_i, j = start_j; i < start_i + count; i++, j++) {
		compare_and_exchange(tbl, column, i, j, dir, tid);
	}
	return 0;
}

// XXX: Is there a better way to implement reusable barriers?
//std::atomic_uint stage1, stage2[32], stage3[8];
barrier_t stage0 = {.count = 0, .seen = 0};
barrier_t stage1 = {.count = 0, .seen = 0};
barrier_t stage2a = {.count = 0, .seen = 0};
barrier_t stage2b = {.count = 0, .seen = 0};
barrier_t stage3a = {.count = 0, .seen = 0};
barrier_t stage3b = {.count = 0, .seen = 0};
barrier_t stage4a = {.count = 0, .seen = 0};
barrier_t stage4b = {.count = 0, .seen = 0};


int sort_table_parallel(table_t *table, int column, int tid, int num_threads) {

	auto N = table->num_rows;
	assert (((N & (N - 1)) == 0));
	// printf("%s, num_rows %d | tid = %d\n", __func__, table->num_rows, tid);

	int num_parts = num_threads;
	const int num_stages = log2(num_threads);
	const int segment_length = (N / num_threads) >> 1;

#if defined(PIN_TABLE_BITONIC)
	// pin table
	if (tid == 0)
		pin_table(table);
#endif
	barrier_wait(&stage0, num_threads);

	if(tid == 0)
		barrier_reset(&stage0, num_threads);

	// stage 1: the whole data is split into shards for num_threads threads
	recBitonicSort(table, tid == 0 ? 0 : (tid * N) / num_threads, (N / num_threads),
			column, (tid % 2 == 0) ? ASCENDING : DESCENDING, tid);


	//stage1.fetch_add(1, std::memory_order_seq_cst);
	//while (stage1 != num_threads) ;

	barrier_wait(&stage1, num_threads);
	if (tid == 0)
		barrier_reset(&stage1, num_threads); 

	// num_stages: Number of stages of processing after stage 1 until num_threads
	// independent bitonic sequences are split. After that the last stage is to
	// sort those independent bitonic sequences into ascending/descending order
	for (auto i = 0; i < num_stages; i++) {
		// decide direction based on the bit
		auto dir = (tid & (1 << (i + 1))) == 0 ? ASCENDING : DESCENDING;
		auto j = 0u;

		// loop until we have num_threads independent bitonic sequences to work on
		do {
			// Injective function to get unique idx into our reusable barrier array
			auto idx = (2*i) + (3*j);
			auto sets = num_parts >> 1;
			auto threads_per_set = num_threads / sets;
			auto num_sets_passed =
				(tid == 0) ? 0 : static_cast<int>(tid / threads_per_set);
			auto si =
				(tid == 0) ?
					0 :
					(threads_per_set * num_sets_passed * segment_length)
					+ (tid * segment_length);
			auto sj = si + (threads_per_set * segment_length);
#ifndef NDEBUG
			printf(
				"[%d] i = %d performing split si %d | sj %d | count %d | num_parts %d | dir %d\n",
				tid, i, si, sj, segment_length, num_parts, dir);
#endif
			bitonicSplit(table, si, sj, segment_length, column, dir, tid);

			// when we do bitonic split, num_parts is doubled
			num_parts *= 2;
			//stage2[idx]++;
			// barrier
			//while (stage2[idx] != num_threads) ;
			barrier_wait(&stage2a, num_threads);
			if (tid == 0)
				barrier_reset(&stage2a, num_threads); 

			barrier_wait(&stage2b, num_threads);
			if (tid == 0)
				barrier_reset(&stage2b, num_threads); 

			++j;
		} while ((num_parts >> 1) != num_threads);

		recBitonicSort(table, tid == 0 ? 0 : (tid * N) / num_threads, (N / num_threads),
			column, dir, tid);
#ifndef NDEBUG
		printf("[%d] return after recursive sort num_parts %d\n", tid, num_parts);
#endif

		//++stage3[i];
		// synchronize all threads
		//while (stage3[i] != num_threads) ;
		barrier_wait(&stage3a, num_threads);
		if (tid == 0)
			barrier_reset(&stage3a, num_threads); 

		barrier_wait(&stage3b, num_threads);
		if (tid == 0)
			barrier_reset(&stage3b, num_threads); 


		// after every round of recursive sort, num_parts will reduce by this factor
		num_parts >>= (i + 2);
	}

#ifndef NDEBUG
	printf("[%d] stage3 sort start %d, count %d\n", tid,
		(tid * N) / num_threads, N / num_threads);
#endif
	// do a final round of sort where all threads will arrange it in ascending order
	recBitonicSort(table, tid == 0 ? 0 : (tid * N) / num_threads, (N / num_threads),
		column, ASCENDING, tid);

	barrier_wait(&stage4a, num_threads);
	if (tid == 0)
		barrier_reset(&stage4a, num_threads); 

	barrier_wait(&stage4b, num_threads);
	if (tid == 0)
		barrier_reset(&stage4b, num_threads); 

#if defined(PIN_TABLE_BITONIC)
	// pin table
	if (tid == 0)
		unpin_table_dirty(table);
#endif

#ifdef CREATE_SORTED_TABLE
	*sorted_id = s_table->id;
#endif
	return 0;
}

int ecall_sort_table_parallel(int db_id, int table_id, int column, int tid, int num_threads)
{
	int ret;
	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)))
		return -1; 

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2;

	table = db->tables[table_id];

	thread_id = tid; 

	return sort_table_parallel(table, column, tid, num_threads);
};
