#include "db.hpp"
#include "util.hpp"
#include "dbg.hpp"
#include "time.hpp"
#include "sort_helper.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#include <cerrno>
#include "quick_sort.hpp"

extern thread_local int thread_id;

int ecall_quicksort_table(int db_id, int table_id, int field, int *sorted_id) {
	int ret; 
	data_base_t *db;
	table_t *table, *s_table;

	if (!(db = get_db(db_id)))
		return -1;

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = quick_sort_table(db, table, field, &s_table); 

#ifdef CREATE_SORTED_TABLE
	*sorted_id = s_table->id; 
#endif
	return ret; 
}

int quicksort_table_parallel(table_t *table, int column, int tid, int num_threads) {
	int ret = 0; 

	/*
	if (!(db = get_db(db_id)))
		return -1;

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2; 

	table = db->tables[table_id];

	ret = quick_sort_table_parallel(db, table, field, &s_table); 

#ifdef CREATE_SORTED_TABLE
	*sorted_id = s_table->id; 
#endif*/
	return ret;
}

int ecall_quicksort_table_parallel(int db_id, int table_id, int column, int tid, int num_threads)
{
	data_base_t *db;
	table_t *table;

	if (!(db = get_db(db_id)))
		return -1;

	if ((table_id > (MAX_TABLES - 1)) || !db->tables[table_id])
		return -2;

	table = db->tables[table_id];

	thread_id = tid; 

	return quicksort_table_parallel(table, column, tid, num_threads);
};

row_t *row = NULL, *start_row = NULL, *end_row = NULL, *temp_row = NULL;

int allocate_memory_for_quicksort(table_t *tbl) {

    if (!tbl->pinned_blocks) {
        row = (row_t *) malloc(row_size(tbl));
        if (!row)
            goto fail1;

        start_row = (row_t *) malloc(row_size(tbl));
        if (!start_row)
            goto fail2;

        end_row = (row_t *) malloc(row_size(tbl));
        if (!end_row)
            goto fail3;
    }

	temp_row = (row_t *) malloc(row_size(tbl));
	if(!temp_row)
		goto fail4;

	return 0;

fail4:
	free(end_row);
fail3:
	free(start_row);
fail2:
	free(row);
fail1:
	return -ENOMEM;
}

void deallocate_memory_for_quicksort(table_t *tbl) {
	if (!tbl->pinned_blocks) {
        if (row)
            free(row);

        if (start_row)
            free(start_row);

        if (end_row)
            free(end_row);
    }
	if (temp_row)
		free(temp_row);
}


int quick_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl)
{
	int ret = 0;

#if defined(REPORT_QSORT_STATS)
	unsigned long long start, end;
	unsigned long long cycles;
	double secs;
#endif

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
	
	ret = allocate_memory_for_quicksort(tbl);
	if (ret) {
		ERR("memory allocation failed: %d\n", ret);
		return ret;
	}

#if defined(REPORT_QSORT_STATS)
	start = RDTSC();
#endif
	// index runs from 0 to (num_rows - 1)
	quickSort(tbl, column, 0, tbl->num_rows - 1);

#if defined(REPORT_QSORT_STATS)
	end = RDTSC();
	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO(" In-place quicksort took %llu cycles (%f sec)\n", cycles, secs);
#endif

#ifdef CREATE_SORTED_TABLE
	bflush(*p_tbl);
#endif
	//print_table_dbg(tbl, 0, tbl->num_rows);

    ret = verify_sorted_output(tbl, 0, tbl->num_rows, column);
	INFO(" %s, verify_sorted_output returned %d\n", __func__, ret);

	if (ret) {
		ERR("============================\n");
		ERR("%s: SORTED OUTPUT INCORRECT \n");
		ERR("============================\n");
	}
	deallocate_memory_for_quicksort(tbl);
	return ret; 	
}

int quick_sort_table_parallel(data_base_t *db, table_t *tbl, int column, table_t **p_tbl)
{
	int ret = 0;

#if defined(REPORT_QSORT_STATS)
	unsigned long long start, end;
	unsigned long long cycles;
	double secs;
#endif

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

	ret = allocate_memory_for_quicksort(tbl);
	if (ret) {
		ERR("memory allocation failed: %d\n", ret);
		return ret;
	}

#if defined(REPORT_QSORT_STATS)
	start = RDTSC();
#endif
	// index runs from 0 to (num_rows - 1)
//#pragma omp parallel
//#pragma omp single
	quickSort_parallel(tbl, column, 0, tbl->num_rows - 1);
	//#pragma omp taskwait

#if defined(REPORT_QSORT_STATS)
	end = RDTSC();
	cycles = end - start;
	secs = (cycles / cycles_per_sec);

	INFO("In-place quicksort took %llu cycles (%f sec)\n", cycles, secs);
#endif

#ifdef CREATE_SORTED_TABLE
	bflush(*p_tbl);
#endif
	//print_table_dbg(tbl, 0, tbl->num_rows);

	ret = verify_sorted_output(tbl, 0, tbl->num_rows, column);

#if defined(REPORT_QSORT_STATS)
	INFO("%s, verify_sorted_output returned %d\n", __func__, ret);
#endif

	if (ret) {
		ERR("============================\n");
		ERR("%s: SORTED OUTPUT INCORRECT \n");
		ERR("============================\n");
	}
	deallocate_memory_for_quicksort(tbl);
	return ret; 	
}
 
void quickSort(table_t *tbl, int column, int start, int end) {

#if defined(REPORT_QSORT_STATS)
	unsigned long long start_sec, end_sec;
	unsigned long long cycles;
	double secs;
#endif

	if (start >= end) {
		return;
	}

#if defined(REPORT_QSORT_STATS)
	start_sec = RDTSC();
#endif	
	int pivot = partition(tbl, column, start, end);
	if (pivot == -1) {
		ERR("Sorting failed\n");
		return;
	}
#if defined(REPORT_QSORT_STATS)
	end_sec = RDTSC();
	cycles = end_sec - start_sec;
	secs = (cycles / cycles_per_sec);

	INFO(" Partitioning from %d to %d took %llu cycles (%f sec)\n", start, end, cycles, secs);
#endif

#if defined(REPORT_QSORT_STATS)
	start_sec = RDTSC();
#endif
	quickSort(tbl, column, start, pivot);
#if defined(REPORT_QSORT_STATS)
	end_sec = RDTSC();
	cycles = end_sec - start_sec;
	secs = (cycles / cycles_per_sec);

	INFO(" #1 quickSort from %d to %d took %llu cycles (%f sec)\n", start, pivot, cycles, secs);
#endif

#if defined(REPORT_QSORT_STATS)
	start_sec = RDTSC();
#endif
	quickSort(tbl, column, pivot + 1, end);
#if defined(REPORT_QSORT_STATS)
	end_sec = RDTSC();
	cycles = end_sec - start_sec;
	secs = (cycles / cycles_per_sec);

	INFO(" #2 quickSort from %d to %d took %llu cycles (%f sec)\n", pivot+1, end, cycles, secs);
#endif
}

void quickSort_parallel(table_t *tbl, int column, int start, int end) {

	if (start >= end) {
		return;
	}
	
	int pivot = partition(tbl, column, start, end);
	if (pivot == -1) {
		ERR("Sorting failed\n");
		return;
	}
	//omp_set_nested(1);
	//#pragma omp parallel sections num_threads(16)
	{
		//#pragma omp task
		//#pragma omp section	
		quickSort(tbl, column, start, pivot);
		//#pragma omp task
		//#pragma omp section
		quickSort(tbl, column, pivot + 1, end);
	}
}



int partition(table_t *tbl, int column, int start, int end) {

    if (start == end)
        return start;

	int mid = start + (end - start) / 2;
    int i;
    int j;
    void *pivot_data;

    if (start+1 == end) {
        i = start - 1;
        j = end + 1;
        pivot_data = get_element(tbl, mid, &row, column);
    } else {
        get_element(tbl, start, &start_row, column);
        get_element(tbl, mid, &end_row, column);
        exchange(tbl, start, mid, start_row, end_row, 0);
        pivot_data = get_element(tbl, start, &row, column);
        i = start;
        j = end + 1;
    }

    if (!pivot_data)
        return -1;

    while (true) {
		switch (tbl->sc.types[column]) {
			case BOOLEAN: {
				bool pivot = *((bool*)pivot_data);
				bool start_val, end_val;

				do {
					i++;
				} while ((start_val = *((bool*)get_element(tbl, i, &start_row, column))) < pivot && i<=j);

				do {
					j--;
				} while ((end_val = *((bool*)get_element(tbl, j, &end_row, column))) > pivot);

                if (i >= j) {
                    if (end > start+1) {
                        get_element(tbl, start, &start_row, column);
                        exchange(tbl, start, j, start_row, end_row, 0);
                    }
                    return j;
                }

				break;
			}
			case INTEGER: {
				int pivot = *((int*)pivot_data);
				int start_val, end_val;

				do {
					i++;
				} while ((start_val = *((int*)get_element(tbl, i, &start_row, column))) < pivot && i<=j);

				do {
					j--;
				} while ((end_val = *((int*)get_element(tbl, j, &end_row, column))) > pivot);

                if (i >= j) {
                    if (end > start+1) {
                        get_element(tbl, start, &start_row, column);
                        exchange(tbl, start, j, start_row, end_row, 0);
                    }
                    return j;
                }
				break;

			}

			case TINYTEXT: {
				char *pivot = (char*)pivot_data;
				char *start_val, *end_val;

				do {
					i++;
					start_val = (char*)get_element(tbl, i, &start_row, column);
				} while (strncmp(start_val, pivot, MAX_ROW_SIZE) < 0 && i<=j);

				do {
					j--;
					end_val = (char*)get_element(tbl, j, &end_row, column);
				} while (strncmp(end_val, pivot, MAX_ROW_SIZE) > 0);

                if (i >= j) {
                    if (end > start+1) {
                        get_element(tbl, start, &start_row, column);
                        exchange(tbl, start, j, start_row, end_row, 0);
                    }
                    return j;
                }
				break;
			}
			default:
				break;
		}

		// Perform swap by writing start_row to jth position and
		// end_row to ith position
		exchange(tbl, i, j, start_row, end_row, 0);
	}
}
