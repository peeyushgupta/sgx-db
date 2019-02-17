#ifndef _COLUMN_SORT_HPP
#define _COLUMN_SORT_HPP

int column_sort_pick_params(unsigned long num_records,
				unsigned long rec_size,
				unsigned long bcache_rec_size,
				unsigned long sgx_mem_size,
				unsigned long *r,
				unsigned long *s);

int column_sort_table(data_base_t *db, table_t *table, int column);

int compare_tables(table_t *left, table_t *right, int tid, int num_threads);
int compare_tables(table_t *left, table_t *right);

#endif // _COLUMN_SORT_HPP
