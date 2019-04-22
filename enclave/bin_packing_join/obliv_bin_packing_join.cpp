#include "obliv_bin_packing_join.hpp"

#include <cassert>
#include <cstring>
#include <vector>

#include "bitonic_sort.hpp"
#include "obli.hpp"

int obliv_fill_bins(data_base_t *db, table_t *data_table, int column, const int rows_per_dblk,
                    table_t *bin_info_table, const int start_dblk, const int end_dblk,
                    const int num_bins, const int rows_per_cell, schema_t *bin_sc,
                    std::vector<table_t *> *bins) {
    ERR("Unimplemented");

    return 0;
}

int obli_fill_bins_per_dblk(table_t *data_table, int column, int *data_row_num,
                            const int rows_per_dblk, const int dblk_cnt, table_t *bin_info_table,
                            int *info_row_num, const int rows_per_cell, schema_t *bin_sc,
                            std::vector<table_t *> *bins) {
    // Create a backup for data_row_num since we will use it multiple times
    const int initial_data_row_num = *data_row_num;
    // Load bin information
    // This map stores the information about a value should be placed in
    // which cell.
    const int num_bins = bins->size();
    int byte_read_info = 0;
    // For each cell
    for (int cell_num = 0; cell_num < num_bins; ++cell_num) {
        std::vector<std::string> keys;
        // Load each value in `bin_info_table` into `keys`
        for (int i = 0; i < rows_per_cell; ++i) {
            row_t row;
#if !defined(NDEBUG)
            if (*info_row_num >= bin_info_table->num_rows) {
                ERR("Reach the end of data table but still have more rows "
                    "to read\n");
                return -1;
            }
#endif
            if (read_row(bin_info_table, (*info_row_num)++, &row)) {
                ERR("Failed to read row");
                return -1;
            }

            std::string value((char *)get_column(&(bin_info_table->sc), 0, &row));
            if (value.empty()) {
                continue;
            }

            try {
                byte_read_info += value.length();
                keys.push_back(value);
            } catch (const std::bad_alloc &) {
                ERR("Not enough memory: dblk %d, i %d, row_num %d, byte_info "
                    "%d\n",
                    dblk_cnt, i, data_row_num, byte_read_info);
                return -1;
            }
        }

        // Scan the entire datablock(expensive) and load data
        *data_row_num = initial_data_row_num;
        int initial_bin_row_num = (*bins)[cell_num]->num_rows;
        int byte_read_bin = 0;
        typedef std::vector<row_t> temp_bin_t;
        std::vector<temp_bin_t> temp_bins(num_bins);
        for (int i = 0; i < rows_per_dblk; ++i) {
            row_t data_row;
            if (*data_row_num >= data_table->num_rows) {
                // TODO: make sure that there's no more info_bin to read in the
                // outer loop
                break;
            }

            if (read_row(data_table, (*data_row_num)++, &data_row)) {
                ERR("Failed to read row");
                return -1;
            }
            std::string value((char *)get_column(&(data_table->sc), column, &data_row));
            // If we don't want to load this value, make data_row empty
            // TODO: set header.is_fake instead. it's way cheaper
            bool should_not_load = false;
            for (const auto &key : keys) {
                should_not_load |= (key == value);
            }
            row_t fake_row;
            memset(&fake_row, 0x0, sizeof(fake_row));
            fake_row.header.fake = true;
            obli_cswap((u8 *)&data_row, (u8 *)&fake_row, sizeof(row_t), should_not_load);

            insert_row_dbg(bins->at(cell_num), &data_row);
        }
        bitonic_sort_table(db, bins->at(cell_num), column, /* output talbe */);
    }

    *data_row_num = initial_data_row_num + rows_per_dblk;
    return 0;
}