#include "obli_bin_packing_join.hpp"

#include <cassert>
#include <cstring>
#include <vector>

#include "bitonic_sort.hpp"
#include "db.hpp"
#include "dbg.hpp"
// #include "obli.hpp"
#include "time.hpp"
#include "util.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

int nearest_pow_2(int x) { return 1 << (sizeof(x) * 8 - __builtin_clz(x - 1)); }

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
    int rtn = 123456;
    // Create a backup for data_row_num since we will use it multiple times
    const int initial_data_row_num = *data_row_num;
    // Load bin information
    // This map stores the information about a value should be placed in
    // which cell.
    const int num_bins = bins->size();
    int byte_read_info = 0;
    // For each cell
    for (int cell_num = 0; cell_num < num_bins; ++cell_num) {
        DBG("filling dblk #%d, cell#%d\n", dblk_cnt, cell_num);
        table_t *bin = bins->at(cell_num);
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

        // Create temp_bin
        table_t *temp_bin;
        std::string temp_bin_name = bin->name + "_obli_phase3_temp_bin";
        rtn = create_table(bin->db, temp_bin_name, &bin->sc, &temp_bin);
        if (rtn) {
            ERR("Failed to create temp_bin");
            return -7;
        }

        // Scan the entire datablock(expensive) and load data into temp_bin
        *data_row_num = initial_data_row_num;
        int initial_bin_row_num = (*bins)[cell_num]->num_rows;
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
            std::string value(
                (char *)get_column(&(data_table->sc), column, &data_row));
            // If we don't want to load this value, make data_row empty
            // TODO: set header.is_fake instead. it's way cheaper
            bool should_not_load = false;
            for (const auto &key : keys) {
                should_not_load |= (key == value);
            }
            row_t fake_row;
            memset(&fake_row, 0x0, sizeof(fake_row));
            fake_row.header.fake = true;
            // TODO: add the oblivious code bad
            if (should_not_load) {
                memcpy(&data_row, &fake_row, sizeof(data_row));
            }

            // obli_cswap((u8*)&data_row, (u8*)&fake_row, sizeof(row_t),
            // should_not_load);

            insert_row_dbg(temp_bin, &data_row);
        }

        DBG("Keys: \n");
        for (const auto& key : keys) {
            DBG("%s\n", key.c_str());
        }
        DBG("\n");

        // Append fake row to make
        row_t fake_row;
        memset(&fake_row, 0x0, sizeof(fake_row));
        fake_row.header.fake = true;
        int n = nearest_pow_2(temp_bin->num_rows);
        assert(__builtin_popcount(n) == 1);
        for (int i = temp_bin->num_rows; i < n; ++i) {
            insert_row_dbg(temp_bin, &fake_row);
        }
        // print_table_dbg(temp_bin, 0, 100);

        // Sort temp bin
        table_t *sorted_temp_bin = nullptr;
        bitonic_sort_table(temp_bin->db, temp_bin, column, &sorted_temp_bin);
#if !defined(NDEBUG)
        if (sorted_temp_bin != nullptr) {
            ERR("A sorted_temp_bin is created unexpectedly.\n");
            return -1;
        }
#endif
        print_table_dbg(temp_bin, 8000, 8100);
        delete_table(temp_bin->db, temp_bin);
        assert(false);
        return -1;
    }

    *data_row_num = initial_data_row_num + rows_per_dblk;
    return 0;
}