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
	return 0;
}

int ecall_quicksort_table_parallel(int db_id, int table_id, int column, int tid, int num_threads)
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

	return quicksort_table_parallel(table, column, tid, num_threads);
};

row_t *row = NULL, *start_row = NULL, *end_row = NULL, *temp_row = NULL;

int allocate_memory_for_quicksort(table_t *tbl) {
	int ret;

	row = (row_t *) malloc(row_size(tbl));
	if(!row)
		goto fail1;

	start_row = (row_t *) malloc(row_size(tbl));
	if(!start_row)
		goto fail2;
	
	end_row = (row_t *) malloc(row_size(tbl));
	if(!end_row)
		goto fail3;

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

void deallocate_memory_for_quicksort() {
	if (row)
		free(row); 
	
	if (start_row)
		free(start_row); 

	if (end_row)
		free(end_row);

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

	INFO("In-place quicksort took %llu cycles (%f sec)\n", cycles, secs);
#endif

#ifdef CREATE_SORTED_TABLE
	bflush(*p_tbl);
#endif
	//print_table_dbg(tbl, 0, tbl->num_rows);

	ret = verify_sorted_output(tbl, 0, tbl->num_rows, column);

	INFO("%s, verify_sorted_output returned %d\n", __func__, ret);

	if (ret) {
		ERR("============================\n");
		ERR("%s: SORTED OUTPUT INCORRECT \n");
		ERR("============================\n");
	}
	deallocate_memory_for_quicksort();
	return ret; 	
}
 
void quickSort(table_t *tbl, int column, int start, int end) {

	if (start >= end) {
		return;
	}

	int pivot = partition(tbl, column, start, end);
	if (pivot == -1) {
		ERR("Sorting failed\n");
		return;
	}
		
	quickSort(tbl, column, start, pivot);
	quickSort(tbl, column, pivot + 1, end);
}

int partition(table_t *tbl, int column, int start, int end) {

	int ret = 0;

	int mid = start + (end - start) / 2;
	int i = start - 1;
	int j = end + 1;
	void *pivot_data = get_element(tbl, mid, row, column);

	if (!pivot_data)
		return -1;

	while (true) {
		switch (tbl->sc.types[column]) {
			case BOOLEAN: {
				bool pivot = *((bool*)pivot_data);
				bool start_val, end_val;

				do {
					i++;
				} while ((start_val = *((bool*)get_element(tbl, i, start_row, column))) < pivot);

				do {
					j--;
				} while ((end_val = *((bool*)get_element(tbl, j, end_row, column))) > pivot);

				if (i >= j)
					return j;

				break;
			}
			case INTEGER: {
				int pivot = *((int*)pivot_data);
				int start_val, end_val;

				do {
					i++;
				} while ((start_val = *((int*)get_element(tbl, i, start_row, column))) < pivot);

				do {
					j--;
				} while ((end_val = *((int*)get_element(tbl, j, end_row, column))) > pivot);

				if (i >= j)
					return j;

				break;

			}

			case TINYTEXT: {
				char *pivot = (char*)pivot_data;
				char *start_val, *end_val;

				do {
					i++;
					start_val = (char*)get_element(tbl, i, start_row, column);
				} while (strncmp(start_val, pivot, MAX_ROW_SIZE) < 0);

				do {
					j--;
					end_val = (char*)get_element(tbl, j, end_row, column);
				} while (strncmp(end_val, pivot, MAX_ROW_SIZE) > 0);

				if (i >= j)
					return j;

				break;
			}
			default:
				break;
		}

		// if we break from switch, we should swap
		memcpy(temp_row, start_row, row_size(tbl));
		memcpy(start_row, end_row, row_size(tbl));
		memcpy(end_row, temp_row, row_size(tbl));

		// write swapped rows i and j
		ret = write_row_dbg(tbl, start_row, i);
		if (ret) {
			ERR("failed to insert row %d of table %s\n",
				i, tbl->name.c_str());
			return -1;
		}

		ret = write_row_dbg(tbl, end_row, j);
		if (ret) {
			ERR("failed to insert row %d of table %s\n",
				j, tbl->name.c_str());
			return -1;
		}
	}
}
