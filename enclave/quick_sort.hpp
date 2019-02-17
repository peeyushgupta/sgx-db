#ifndef _QUICK_SORT_HPP
#define _QUICK_SORT_HPP

int quick_sort_table(data_base_t *db, table_t *tbl, int column, table_t **p_tbl);
void quickSort(table_t *tbl, int column, int start, int end);
int partition(table_t *tbl, int column, int start, int end);

#endif // _QUICK_SORT_HPP
