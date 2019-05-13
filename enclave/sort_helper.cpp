#include "db.hpp"
#include "util.hpp"
#include "dbg.hpp"
#include "time.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#include <cerrno>

void *get_element(table_t *tbl, int row_num, row_t **row_buf, int column)
{
	int ret;
	void *element;

	if(tbl->pinned_blocks) {
		data_block_t *b_i;
		get_pinned_row(tbl, row_num, &b_i, row_buf);
	} else {
		ret = read_row(tbl, row_num, *row_buf);

		if(ret) {
			ERR("failed to read row %d of table %s\n",
				row_num, tbl->name.c_str());
			return NULL;
		}
	}

	element = get_column(&tbl->sc, column, *row_buf);
	return element;
}

int verify_sorted_output(table_t *tbl, int start, int end, int column)
{
	int ret = 0;
	unsigned long i;
	row_t *row_i, *row_j;

	if (end > tbl->num_rows) {
		end = tbl->num_rows;
	}
	// alloc two rows
	if (!tbl->pinned_blocks) {
        row_i = (row_t *) malloc(row_size(tbl));
        if (!row_i) {
            ERR("can't allocate memory for the row\n");
            return -ENOMEM;
        }

        row_j = (row_t *) malloc(row_size(tbl));
        if (!row_j) {
            ERR("can't allocate memory for the row\n");
            return -ENOMEM;
        }
    }


	// end is tbl->num_rows; we check until end - 2
	for (i = start; i < end - 2; i++) {
		void *element_i = get_element(tbl, i, &row_i, column);
		void *element_j = get_element(tbl, i + 1, &row_j, column);
		if (!element_i || !element_j) {
			ERR("%s, failed\n", __func__);
			ret = 1;
			goto exit;
		}

		if (tbl->sc.types[column] == INTEGER) {
			ret |= (*((int*)element_i) > *((int *)element_j));
		} else if (tbl->sc.types[column] == TINYTEXT) {
			int ans = 0;
		    ans = (strncmp((char*) element_i, (char*) element_j, MAX_ROW_SIZE) > 0);
			ret=ret + ans;
		}
	}

	if (!tbl->pinned_blocks) {
        free(row_i);
        free(row_j);
    }
exit:
	return ret;
}

// INLINE procedure exchange() : pair swap
int exchange(table_t *tbl, int i, int j, row_t *row_i, row_t *row_j, int tid) {
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
