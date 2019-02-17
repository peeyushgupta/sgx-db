#include "db.hpp"
#include "util.hpp"
#include "dbg.hpp"
#include "time.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#include "mbusafecrt.h"
#include "dbg_buffer.hpp"

// Column sort uses either quicksort or bitonic sort for sorting each column
#include "bitonic_sort.hpp"
#include "quick_sort.hpp"

#define COLUMNSORT_VERBOSE 0
#define COLUMNSORT_VERBOSE_L2 0

extern thread_local int thread_id;
/* 
   - num_records -- number of records in the table
   - rec_size -- size of projected record
   - bcache_rec_size -- size of the buffer cache record

   - r * rec_size + s * bcache_rec_size < sgx_mem_size
   - r * s >= num_records
   - r is a power of 2 for bitonic to work
   - r % s = 0 -- r is divisible by s
   - r > 2 * (s - 1)^2
*/

int column_sort_pick_params(unsigned long num_records, 
				unsigned long rec_size, 
				unsigned long bcache_rec_size, 
				unsigned long sgx_mem_size, 
				unsigned long *r_out, 
				unsigned long *s_out) 
{

	bool all_good = false;
	unsigned long r, s;  

	DBG_ON(COLUMNSORT_VERBOSE, 
		"Searching for r and s for num_records=%d, rec_size=%d, bcache_rec_size=%d, sgx_mem_size=%d\n", num_records, rec_size, bcache_rec_size, sgx_mem_size);

	/* Initial version of this algorithm will try to minimize r */
	r = 1; 
	do {
		/* Increase r, start over */
		r = r * 2;

		/* Choose s */
		for (s = num_records / r; s < r; s ++) {
		
 			DBG_ON(COLUMNSORT_VERBOSE_L2, 
				"trying r=%d and s=%d\n", r, s);
			if (s == 0)
				continue; 
	
			if( r % s != 0) {
				DBG_ON(COLUMNSORT_VERBOSE_L2, 
				"r (%d) is not divisible by s (%d)\n", r, s);
				continue;
			}	

			if ( r < 2*(s - 1)*(s - 1)) {
				DBG_ON(COLUMNSORT_VERBOSE, 
					"r (%d) is < 2*(s - 1)^2 (%d), where r=%d, s=%d\n", 
					r, 2*(s - 1)*(s - 1), r, s);  
				continue; 
			}
		
			if (r * rec_size + s * bcache_rec_size > sgx_mem_size) {
				DBG_ON(COLUMNSORT_VERBOSE, 
					"r * rec_size + s * bcache_rec_size < sgx_mem_size, where r=%d, rec_size %d, s=%d, rec_size:%d, bcache_rec_size:%d, sgx_mem_size:%d\n", 
					r, rec_size, s, rec_size, bcache_rec_size, sgx_mem_size);  
				continue; 
			}

			all_good = true;
			break;  
		};

 	
	} while (!all_good); 

	DBG_ON(COLUMNSORT_VERBOSE, 
		"r=%d, s=%d\n", r, s); 
	*r_out = r; 
	*s_out = s; 

	return 0;
}

int reassemble_column_tables(table_t** s_tables, table_t *table, row_t *row, int s, int r, int tid, int num_threads)
{
	unsigned int row_num = 0; 
	int ret; 
	
	/* In a parallel setup all work is done by tid 0 */
	if (tid != 0) 
		return 0; 

	/* Write sorted table back  */
	for (unsigned int i = 0; i < s; i ++) {

		for (unsigned int j = 0; j < r; j ++) {

			/* Read row from s table */
			ret = read_row(s_tables[i], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, s_tables[i]->name.c_str());
				return -1;
			}

			/* Add row to the st table */
			ret = write_row_dbg(table, row, row_num);
			if(ret) {
				ERR("failed to insert row %d of sorted table %s\n",
					row_num, table->name.c_str());
				return -2;
			}
			row_num ++;
		}
	}
	return 0; 
}

int compare_tables(table_t *left_tbl, table_t *right_tbl, int tid, int num_threads) {

	row_t *right_row, *left_row; 
	int ret;  

	/* In a parallel setup all work is done by tid 0 */
	if (tid != 0) 
		return 0; 

	left_row = (row_t*) malloc(row_size(left_tbl));
	if(!left_row)
		return -ENOMEM;

	right_row = (row_t*) malloc(row_size(right_tbl));
	if(!right_row)
		return -ENOMEM;

	if(left_tbl->num_rows != left_tbl->num_rows) {
		ERR("tables have different size: (%s,%d) != (%s, %d)\n",
			left_tbl->name.c_str(), left_tbl->num_rows, 
			right_tbl->name.c_str(), right_tbl->num_rows);
		return -3;
	};

	for (unsigned long i = 0; i < left_tbl->num_rows; i ++) {

		// Read old row
		ret = read_row(left_tbl, i, left_row);
		if(ret) {
			ERR("failed to read row %d of table %s\n",
				i, left_tbl->name.c_str());
			goto cleanup;
		}

				
		/* Add row to the promoted table */
		ret = read_row(right_tbl, i, right_row);
		if(ret) {
			ERR("failed to insert row %d of promoted table %s\n",
				i, right_tbl->name.c_str());
			goto cleanup;
		}

		ret = memcmp(left_row, right_row, row_size(left_tbl)); 
		if (ret) {
			ERR("tables have different rows: (%s, row:%d) != (%s, row:%d)\n",
			left_tbl->name.c_str(), i, 
			right_tbl->name.c_str(), i);
			print_row(&left_tbl->sc, left_row); 
			print_row(&right_tbl->sc, right_row);
			bcache_info_printf(left_tbl);			
		}
	}

	INFO("passed comparison: (%s) == (%s)\n",
		left_tbl->name.c_str(), right_tbl->name.c_str());


	ret = 0;
cleanup: 
	if (left_row)
		free(left_row); 

	if (right_row)
		free(right_row); 


	return ret; 
};

int compare_tables(table_t *left, table_t *right) {
	return compare_tables(left, right, 0, 1);
}


barrier_t b1 = {.count = 0, .seen = 0}; 
barrier_t b2 = {.count = 0, .seen = 0}; 

/* r -- number of rows
   s -- number of columns
*/

// Globals
table_t **s_tables, **st_tables, *tmp_table;
unsigned long r, s;

int column_sort_table_parallel(data_base_t *db, table_t *table, int column, int tid, int num_threads) {
	int ret;
	std::string tmp_tbl_name;  
	row_t *row;
	unsigned long row_num;  
	unsigned long shift, unshift;

#if defined(COLUMNSORT_COMPARE_TABLES)
	table_t *tmp_table; 
#endif

#if defined(REPORT_COLUMNSORT_STATS)
	unsigned long long start, end; 
	unsigned long long cycles;
	double secs;
	bcache_stats_t bstats;
	dbg_buffer *dbuf;
#endif
	
	row = (row_t*) malloc(row_size(table));
	if(!row) {
		ERR("failed to alloc row\n"); 
		goto cleanup;
	}

	if(tid == 0) {
		dbuf = new dbg_buffer(20);
		ret = column_sort_pick_params(table->num_rows, table->sc.row_data_size, 
				DATA_BLOCK_SIZE, 
				(1 << 20) * 80, 
				&r, &s);
		if (ret) {
			ERR("Can't pick r and s for %s\n", table->name.c_str());
			return -1;  
		}

		if( r % s != 0) {
			ERR("r (%d) is not divisible by s (%d)\n", r, s);
			return -1;
		}

		if ( r < 2*(s - 1)*(s - 1)) {
			ERR("r (%d) is < 2*(s - 1)^2 (%d), where r=%d, s=%d\n", 
				r, 2*(s - 1)*(s - 1), r, s);  
			return -1; 
		}

		s_tables = (table_t **)malloc(s * sizeof(table_t *)); 
		if(!s_tables) {
			ERR("failed to allocate s_tables\n");
			goto cleanup; 
		}

		st_tables = (table_t **)malloc(s * sizeof(table_t *)); 
		if(!st_tables) {
			ERR("failed to allocate s_tables\n");
			goto cleanup; 
		}

#if defined(REPORT_COLUMNSORT_STATS)
		start = RDTSC();
		//bcache_info_printf(table);  
		bcache_stats_read_and_reset(&db->bcache, &bstats);
#endif

		/* Create s temporary column tables */
		for (int i = 0; i < s; i++) {
	
			tmp_tbl_name = "s" + std::to_string(i) + ":" + table->name ; 

			ret = create_table(db, tmp_tbl_name, &table->sc, &s_tables[i]);
			if (ret) {
				ERR("create table:%d\n", ret);
				goto cleanup; 
			}
	
			DBG_ON(COLUMNSORT_VERBOSE_L2, "Created tmp table %s, id:%d\n", 
            			tmp_tbl_name.c_str(), s_tables[i]->id); 
		}

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Created temp tables in %llu cycles (%f sec)\n",
			cycles, secs);

		start = RDTSC();
#endif


		/* Create another set of s transposed column tables */
		for (int i = 0; i < s; i++) {
	
			tmp_tbl_name = "st" + std::to_string(i) + ":" + table->name ; 

			ret = create_table(db, tmp_tbl_name, &table->sc, &st_tables[i]);
			if (ret) {
				ERR("create table:%d\n", ret);
				goto cleanup; 
			}

			DBG_ON(COLUMNSORT_VERBOSE_L2, "Created tmp table %s, id:%d\n", 
            			tmp_tbl_name.c_str(), st_tables[i]->id); 
		}
	
#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Created another set of transposed tables in %llu cycles (%f sec)\n",
			cycles, secs);

		start = RDTSC();
#endif


	
		row_num = 0; 

		/* Rewrite the table as s column tables  */
		for (unsigned int i = 0; i < s; i ++) {

			for (unsigned int j = 0; j < r; j ++) {

				// Read old row
				ret = read_row(table, row_num, row);
				if(ret) {
					ERR("failed to read row %d of table %s\n",
						row_num, table->name.c_str());
					goto cleanup;
				}

				
				/* Add row to the s table */
				ret = insert_row_dbg(s_tables[i], row);
				if(ret) {
					ERR("failed to insert row %d of column table %s\n",
						row_num, s_tables[i]->name.c_str());
					goto cleanup;
				}
				row_num ++;
			}
		}
	
#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Rewrite the table as s column tables in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 
#endif


#if defined(COLUMNSORT_COMPARE_TABLES)
		/* Create a temporary table to compare against the original one  */
		tmp_tbl_name = "tmp:" + table->name ; 
			
		ret = create_table(db, tmp_tbl_name, &table->sc, &tmp_table);
		if (ret) {
			ERR("create table:%d\n", ret);
			goto cleanup; 
		}
	
		DBG_ON(COLUMNSORT_VERBOSE_L2, "Created tmp table %s, id:%d\n", 
            		tmp_tbl_name.c_str(), tmp_table->id); 

		printf("Column tables\n");
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
		}
#endif
	}

	// wait here until tid=0 sets up 's' value
	barrier_wait(&b1, num_threads);
	if(tid == 0)
		barrier_reset(&b1, num_threads);

	barrier_wait(&b2, num_threads);
	if(tid == 0)
		barrier_reset(&b2, num_threads);

#if defined(REPORT_COLUMNSORT_STATS)
	start = RDTSC(); 
#endif

	/* All threads sort table in parallel */
	for (unsigned int i = 0; i < s; i++) {
#if defined(PIN_TABLE)
		if(tid == 0) {
			pin_table(s_tables[i]); 
		}
#endif
		barrier_wait(&b1, num_threads);
		if(tid == 0) 
			barrier_reset(&b1, num_threads); 

#if !defined(SKIP_BITONIC)
		ret = sort_table_parallel(s_tables[i], column, tid, num_threads);
#endif
		barrier_wait(&b2, num_threads); 	
		if(tid == 0) {
			barrier_reset(&b2, num_threads); 
#if defined(PIN_TABLE)
			unpin_table_dirty(s_tables[i]); 
#endif
		}
	}

	if(tid == 0) {
#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 1: Sorted column tables in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 

#endif

		DBG_ON(COLUMNSORT_VERBOSE, "Step 1: Sorted column tables\n");
#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
		}
#endif

#if defined(REPORT_COLUMNSORT_STATS)
		start = RDTSC(); 
	}
#endif

	/* All threas transpose tables in parallel */
	/* Each thread takes an s table and writes it into an st table */
	
	/* Just in case tid==0 is still unpining table s_tables, wait for it */
	barrier_wait(&b1, num_threads);
	if(tid == 0) 
		barrier_reset(&b1, num_threads); 
	
	/* Transpose s column tables into s transposed tables  */
	for (unsigned int i = 0 + tid; i < s; i += num_threads) {

		for (unsigned int j = 0; j < r; j ++) {
			unsigned long seq; 

			// Read old row
			ret = read_row(s_tables[i], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					j, s_tables[i]->name.c_str());
				goto cleanup;
			}

			seq = i * r + j; 
				
			/* Add row to the st table */
			//DBG_ON(COLUMNSORT_VERBOSE,
			//	"tid (%d): insert row %d of s_tables[%d] into row:%d of st_tables[%d]"
			//	"r:%d, s:%d, j%d, r/s:%d, j/s:%d, row_size:%d\n", 
			//	tid, j, i, tid * (r/s) + j / s, j % s, 
			//	r, s, j, r/s, j/s, row_size(s_tables[i])); 

			DBG_ON(COLUMNSORT_VERBOSE,
				"tid (%d): insert row %d of s_tables[%d] into row:%d of st_tables[%d]"
				"r:%d, s:%d, j%d, seq:%d, row_size:%d\n", 
				tid, j, i, seq/s, seq % s, 
				r, s, j, seq, row_size(s_tables[i])); 

#if defined(COLUMNSORT_APPENDS)
			ret = insert_row_dbg(st_tables[seq % s], row);
			if(ret) {
				ERR("failed to insert row %d of transposed column table %s\n",
					j, st_tables[j % s]->name.c_str());
				goto cleanup;
			}

#else			
			ret = write_row_dbg(st_tables[seq % s], row, seq / s);
			if(ret) {
				ERR("failed to insert row %d of transposed column table %s\n",
					j, st_tables[j % s]->name.c_str());
				goto cleanup;
			}
#endif
		}
	}

	barrier_wait(&b2, num_threads); 	
	if(tid == 0) 
		barrier_reset(&b2, num_threads); 
	if (tid == 0) {
#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 2: Transposed column tables in %llu cycles (%f sec)\n",
			cycles, secs);
		
		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 
#endif


		DBG_ON(COLUMNSORT_VERBOSE, "Step 2: Transposed column tables\n");
#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
		}
#endif
	} /* tid == 0 */

#if defined(REPORT_COLUMNSORT_STATS)
	start = RDTSC(); 
#endif

	for (unsigned int i = 0; i < s; i++) {
		//bitonic_sort_table(db, st_tables[i], column, &tmp_table);
#if defined(PIN_TABLE)
		if(tid == 0) {
			pin_table(st_tables[i]); 
		}
#endif
		barrier_wait(&b1, num_threads);
		if(tid == 0) { 
			barrier_reset(&b1, num_threads);
			// Clean s tables so we can do insert_row again
			for (unsigned int i = 0; i < s; i++) {
				s_tables[i]->num_rows = 0; 
			}
 
		}
#if !defined(SKIP_BITONIC)
		ret = sort_table_parallel(st_tables[i], column, tid, num_threads);
#endif
		barrier_wait(&b2, num_threads); 	
		if(tid == 0) { 
			barrier_reset(&b2, num_threads); 
#if defined(PIN_TABLE)
			unpin_table_dirty(st_tables[i]);
#endif
		}
	}

	if (tid == 0) {

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 3: Sorted transposed column tables in %llu cycles (%f sec)\n",
			cycles, secs);
		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 
#endif

		DBG_ON(COLUMNSORT_VERBOSE, 
			"Step 3: Sorted transposed column tables\n");
#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
		}
#endif
		row_num = 0; 

#if defined(REPORT_COLUMNSORT_STATS)
		start = RDTSC(); 
#endif
	}
	
	/* Untranspose st transposed column tables into s tables  */
	for (unsigned int i = tid; i < r; i += num_threads) {

		for (unsigned int j = 0; j < s; j ++) {
			unsigned long seq; 

			/* Read row from st table */
			ret = read_row(st_tables[j], i, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					i, st_tables[j]->name.c_str());
				goto cleanup;
			}

			seq = i * s + j; 
		
			//DBG_ON(COLUMNSORT_VERBOSE,
			//	"tid (%d): insert row %d of st_tables[%d] into row:%d of st_tables[%d]"
			//	"r:%d, s:%d, j%d, seq:%d, row_size:%d\n", 
			//	tid, i, j, seq/s, seq % s, 
			//	r, s, j, seq, row_size(s_tables[i])); 

			/* Add row to the s table */
			//ret = write_row_dbg(s_tables[row_num / r], row, row_num % r);

			ret = insert_row_dbg(s_tables[seq / r], row);
			if(ret) {
				ERR("failed to insert row %d of untransposed column table %s\n",
					seq / r, s_tables[i]->name.c_str());
				goto cleanup;
			}

			row_num ++; 
		}
	}

	if(tid == 0) {

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 4: Untransposed column tables in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 
#endif

		DBG_ON(COLUMNSORT_VERBOSE,
			"Step 4: Untransposed column tables\n");
#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
		}
#endif
	} /* tid == 0 */

#if defined(REPORT_COLUMNSORT_STATS)
	start = RDTSC(); 
#endif

	for (unsigned int i = 0; i < s; i++) {
	//	bitonic_sort_table(db, s_tables[i], column, &tmp_table);
#if defined(PIN_TABLE)
		if(tid == 0) {
			pin_table(s_tables[i]); 
		}
#endif
		barrier_wait(&b1, num_threads);
		if(tid == 0) 
			barrier_reset(&b1, num_threads); 
#if !defined(SKIP_BITONIC)
		ret = sort_table_parallel(s_tables[i], column, tid, num_threads);
#endif
		barrier_wait(&b2, num_threads); 	
		if(tid == 0) {
			barrier_reset(&b2, num_threads); 
#if defined(PIN_TABLE)
			unpin_table_dirty(s_tables[i]); 
#endif
		}
	}
#if defined(COLUMNSORT_COMPARE_TABLES)
 	ret = reassemble_column_tables(s_tables, tmp_table, row, s, r, tid, num_threads); 
	if (ret) 
		goto cleanup; 

 	ret = compare_tables(table, tmp_table, tid, num_threads); 
	if (ret) {
		print_table_dbg(table, 0, 16);
 		print_table_dbg(tmp_table, 0, 16);
	}
#endif
	if (tid == 0 && 1) {

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 5: Sorted untransposed column tables in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 

#endif

		DBG_ON(COLUMNSORT_VERBOSE, 
			"Step 5: Sorted untransposed column tables\n");

#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
		}
#endif

		shift = r / 2 ;
		row_num = 0;  

#if defined(REPORT_COLUMNSORT_STATS)
		start = RDTSC(); 
#endif

		/* Shift s tables into st tables  */
		for (unsigned int i = 0; i < s; i ++) {

			for (unsigned int j = 0; j < r; j ++) {

				/* Read row from s table */
				ret = read_row(s_tables[i], j, row);
				if(ret) {
					ERR("failed to read row %d of table %s\n",
						row_num, s_tables[i]->name.c_str());
					goto cleanup;
				}

				/* Add row to the st table */
				//DBG_ON(COLUMNSORT_VERBOSE,
				//	"insert row %d of shifted table (%s) at row %d, row_num:%d, shitf:%d\n", 
				//	j, st_tables[((row_num + shift) / r) % s]->name.c_str(), 
				//	(row_num + shift) % r, row_num, shift); 


				
				/* Add row to the st table */
				ret = write_row_dbg(st_tables[((row_num + shift) / r) % s], 
							row, (row_num + shift) % r);
				if(ret) {
					ERR("failed to insert row %d of shifted column table %s\n",
						row, st_tables[i]->name.c_str());
					goto cleanup;
				}
				row_num ++;
			}
		}

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 6: Shifted column tables in %llu cycles (%f sec)\n",
			cycles, secs);
		
		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 

#endif

		DBG_ON(COLUMNSORT_VERBOSE, 
			"Step 6: Shifted column tables\n");

#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
		}
#endif
	} /* tid == 0 */

#if defined(REPORT_COLUMNSORT_STATS)
	start = RDTSC(); 
#endif

	for (unsigned int i = 0; i < s; i++) {
		//bitonic_sort_table(db, st_tables[i], column, &tmp_table);
#if defined(PIN_TABLE)
		if(tid == 0) {
			pin_table(st_tables[i]); 
		}
#endif
		barrier_wait(&b1, num_threads);
		if(tid == 0) 
			barrier_reset(&b1, num_threads); 
#if !defined(SKIP_BITONIC)
		ret = sort_table_parallel(st_tables[i], column, tid, num_threads);
#endif		
		barrier_wait(&b2, num_threads); 	
		if(tid == 0) {
			barrier_reset(&b2, num_threads); 
#if defined(PIN_TABLE)
			unpin_table_dirty(st_tables[i]);
#endif
		}

	}

	if (tid == 0 && 1) {

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 7: Sorted shifted column tables in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 

#endif
		DBG_ON(COLUMNSORT_VERBOSE, 
			"Step 7: Sorted shifted column tables\n");

#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(st_tables[i], 0, st_tables[i]->num_rows);
		}
#endif

#if defined(REPORT_COLUMNSORT_STATS)
		start = RDTSC(); 
#endif

		row_num = 0;  
	
		/* Unshift st tables into s tables  */

		/* In Shantanu's implementation (no +/-infinity) the first column
	           is special --- it's sorted so instead of shifting we have to 
		   splice it: first half of the column stays in it's place, the 
		   second half (the max elements) goes to the last column */

		unshift = r - r / 2; 
		/* Read half of the row from the st[0] table and write it into 
                   s[0] table -- this part doesn't move */	
		for (unsigned int j = 0; j < unshift; j ++) {

			/* Read row from st table */
			ret = read_row(st_tables[0], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, st_tables[0]->name.c_str());
				goto cleanup;
			}

			/* Add row to the s table */
			ret = write_row_dbg(s_tables[0], row, j);
			if(ret) {
				ERR("failed to insert row %d of unshifted column table %s\n",
					row, s_tables[0]->name.c_str());
				goto cleanup;
			}
			row_num ++;
		}

		/* Read the second half of the st[0] table and write it to the 
                   very end of s[s-1] table */
		for (unsigned int j = unshift; j < s; j ++) {

			/* Read row from st table */
			ret = read_row(st_tables[0], j, row);
			if(ret) {
				ERR("failed to read row %d of table %s\n",
					row_num, st_tables[0]->name.c_str());
				goto cleanup;
			}

			/* Add row to the s table */
			ret = write_row_dbg(s_tables[s - 1], row, j);
			if(ret) {
				ERR("failed to insert row %d of unshifted column table %s\n",
					row, s_tables[s - 1]->name.c_str());
				goto cleanup;
			}
			row_num ++;
		}

		/* Now shift the rest of the table 
		   we start from st[1] table and shift it into the
		   end of s[0], and so on
		*/

		for (unsigned int i = 1; i < s; i ++) {

			for (unsigned int j = 0; j < r; j ++) {
				unsigned int serial; 
	
				/* Read row from st table */
				ret = read_row(st_tables[i], j, row);
				if(ret) {
					ERR("failed to read row %d of table %s\n",
						row_num, st_tables[i]->name.c_str());
					goto cleanup;
				}

				/* Add row to the st table */
				//DBG_ON(COLUMNSORT_VERBOSE_L2,
				//	"insert row %d of unshifted table (%s) at row %d, row_num:%d, shitf:%d\n", 
				//	j, s_tables[((row_num + (r * s) - shift) / r) % s]->name.c_str(), 
				//	(row_num + (r * s) - shift) % r, 
				//	row_num, shift); 
	
				serial = (i * r) + j; 
				serial -= shift; 
				
				DBG_ON(COLUMNSORT_VERBOSE_L2,
					"insert row %d of st[%d] into s[%d], row %d, shitf:%d\n", 
					j, i, serial / r, serial % r, shift); 

				/* Add row to the s table */
				ret = write_row_dbg(s_tables[serial / r], 
							row, serial % r);
				if(ret) {
					ERR("failed to insert row %d of unshifted column table %s\n",
						row, s_tables[i]->name.c_str());
					goto cleanup;
				}
				row_num ++;
			}
		}

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Step 8: Unshifted column tables in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 

#endif
		DBG_ON(COLUMNSORT_VERBOSE, 
			"Step 8: Unshifted column tables\n");

#if defined(COLUMNSORT_DBG)
		for (unsigned int i = 0; i < s; i++) {
			print_table_dbg(s_tables[i], 0, s_tables[i]->num_rows);
		}
#endif

#if defined(REPORT_COLUMNSORT_STATS)
		start = RDTSC(); 
#endif

#if defined(COLUMNSORT_COMPARE_TABLES)
	 	ret = reassemble_column_tables(s_tables, tmp_table, row, s, r, tid, num_threads); 
		if (ret) 
			goto cleanup; 

	 	ret = compare_tables(table, tmp_table, tid, num_threads); 
		if (ret) {
 			print_table_dbg(table, 0, 16);
 			print_table_dbg(tmp_table, 0, 16);
			goto cleanup; 
		}
#endif


		row_num = 0;  

	 	ret = reassemble_column_tables(s_tables, table, row, s, r, tid, num_threads);
		if (ret) 
			goto cleanup; 

#if defined(REPORT_COLUMNSORT_STATS)
		end = RDTSC();
		cycles = end - start;
		secs = (cycles / cycles_per_sec);

		dbuf->insert("Wrote sorted temporary tables back in %llu cycles (%f sec)\n",
			cycles, secs);

		bcache_stats_read_and_reset(&db->bcache, &bstats);
		bcache_stats_printf(&bstats); 

#endif

		DBG_ON(COLUMNSORT_VERBOSE, 
			"Sorted table\n");

#if defined(COLUMNSORT_DBG)
		print_table_dbg(table, 0, table->num_rows);
#endif
	} /* tid == 0 */


	ret = 0;
cleanup: 
	if (row) {
		free(row); 
	}

	if (tid == 0) {
		if (s_tables) {
			for (unsigned int i = 0; i < s; i++) {
				if (s_tables[i]) {
					bflush(s_tables[i]);
					delete_table(db, s_tables[i]);
				}
			}
			free(s_tables);
			s_tables = NULL; 
		};

		if (st_tables) {
			for (unsigned int i = 0; i < s; i++) {
				if (st_tables[i]) {
					bflush(st_tables[i]);
					delete_table(db, st_tables[i]);
					
				}
			}
			free(st_tables);
			st_tables = NULL; 
		};
	};
	if (tid == 0) {
		dbuf->flush();
		delete dbuf;
	}
	return ret; 
};

int column_sort_table(data_base_t *db, table_t *table, int column) {
	
	return column_sort_table_parallel(db, table, column, 0, 1); 
}

int ecall_column_sort_table_parallel(int db_id, int table_id, int column, int tid, int num_threads)
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
	return column_sort_table_parallel(db, table, column, tid, num_threads); 

};

