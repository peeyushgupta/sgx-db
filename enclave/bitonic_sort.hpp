#ifndef _BITONIC_SORT_HPP
#define _BITONIC_SORT_HPP

inline int exchange(table_t *tbl, int i, int j, row_t *row_i, row_t *row_j);
int compare(table_t *tbl, int column, int i, int j, int dir);
void bitonicMerge(table_t *tbl, int lo, int cnt, int column, int dir);
void recBitonicSort(table_t *tbl, int lo, int cnt, int column, int dir, int tid);
int bitonic_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl);
int bitonic_sort_table_parallel(table_t *tbl, int column, int tid, int num_threads);

#endif // _BITONIC_SORT_HPP
