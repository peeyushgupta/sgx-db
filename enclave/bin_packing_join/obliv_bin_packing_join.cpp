#include "obliv_bin_packing_join.hpp"

#include <cassert>
#include <vector>

#include "obli.hpp"

int obliv_fill_bins(data_base_t *db, table_t *data_table, int column,
                    const int rows_per_dblk, table_t *bin_info_table,
                    const int start_dblk, const int end_dblk,
                    const int num_bins, const int rows_per_cell,
                    schema_t *bin_sc, std::vector<table_t *> *bins) {
    ERR("Unimplemented");

    return 0;
}

int obli_fill_bins_per_dblk(table_t *data_table, int column, int *data_row_num,
                            const int rows_per_dblk, const int dblk_cnt,
                            table_t *bin_info_table, int *info_row_num,
                            const int rows_per_cell, schema_t *bin_sc,
                            std::vector<table_t *> *bins) {
    // Load bin information
    // This map stores the information about a value should be placed in
    // which cell.
    const int num_bins = bins->size();
    int byte_read_info = 0;
    std::vector<std::string> keys;
    // For each cell
    for (int cell_num = 0; cell_num < num_bins; ++cell_num) {
        // Load each value in `bin_info_table` into
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

            std::string value(
                (char *)get_column(&(bin_info_table->sc), 0, &row));
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
    }

    // Scan the data table and fill bin according to the bin information
    int byte_read_bin = 0;
    typedef std::vector<row_t> temp_bin_t;
    std::vector<temp_bin_t> temp_bins(num_bins);
    for (int i = 0; i < rows_per_dblk; ++i) {
        row_t row;
        if (*data_row_num >= data_table->num_rows) {
            // TODO: make sure that there's no more info_bin to read in the
            // outer loop if (data_table_exhausted) {
            //     ERR("Reach the end of data table but still have more rows "
            //         "to read. Data table: dblk %d, curr_row %d, "
            //         "rows_per_dblk. Info table: total rows %d, curr_row "
            //         "%d %d\n",
            //         dblk_cnt, i, rows_per_dblk, bin_info_table->num_rows,
            //         row_num, 1);
            //     return -1;
            // }
            break;
        }

        if (read_row(data_table, (*data_row_num)++, &row)) {
            ERR("Failed to read row");
            return -1;
        }
        std::string value((char *)get_column(&(data_table->sc), column, &row));
#if !defined(NDEBUG)
        const auto it = placement.find(value);
        if (it == placement.end()) {
            ERR("Failed to find bin info for '%s'\n", value.c_str());
            return -1;
        }
        if (it->second < 0 || it->second >= temp_bins.size()) {
            ERR("Bin index out of bound: %d\n", it->second);
            return -1;
        }
#endif
        const auto &key = placement[value];
        auto &bin = temp_bins[key];
        try {
            bin.push_back(row);
            byte_read_bin += sizeof(row);
        } catch (const std::bad_alloc &) {
            ERR("Not enough memory: dblk %d, i %d, row_num %d, byte_info "
                "%d, byte_bin %d\n",
                dblk_cnt, i, data_row_num, byte_read_info, byte_read_bin);
            return -1;
        }
    }

    // Flush temp_bins to bins
    row_t empty_row;
    memset(&empty_row, 0x0, sizeof(empty_row));
    empty_row.header.fake = true;
    for (int i = 0; i < temp_bins.size(); ++i) {
        temp_bin_t *temp_bin = &temp_bins[i];
        for (int j = 0; j < rows_per_cell; j++) {
            row_t *row;
            if (j < temp_bin->size()) {
                row = &(*temp_bin)[j];
            } else {
                row = &empty_row;
            }
            insert_row_dbg((*bins)[i], row);
        }
    }

    return 0;
}