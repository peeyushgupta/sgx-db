#ifndef _SORT_HELPER_H
#define _SORT_HELPER_H

void *get_element(table_t *tbl, int row_num, row_t *row_buf, int column);
int verify_sorted_output(table_t *tbl, int start, int end, int column);
inline int exchange(table_t *tbl, int i, int j, row_t *row_i, row_t *row_j, int tid);

#endif // _SORT_HELPER_H
