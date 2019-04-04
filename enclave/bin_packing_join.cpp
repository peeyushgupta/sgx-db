#include "bin_packing_join.hpp"

#include <cassert>
#include <unordered_map>

#include "db.hpp"
#include "util.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

// Perform phase3 and phase4 of the bin_packing_join
// `bin_info_tid`: table id of the table that stores the information about the
// bins.
// `midpoints`: where the table_left and table_right seperate.
// `num_rows_per_bin`: number of rows in eacn input/output bin.
int ecall_bin_pack_join(int db_id, join_condition_t *join_cond,
                        int *join_tbl_id, int num_rows_per_out_bin,
                        int bin_info_tbl_id, int midpoint, int num_bins,
                        int rows_per_cell) {
    int rtn;
    data_base_t *db = get_db(db_id);
    table_t *bin_info_btl;
    if (!db) {
        ERR("Failed to get database");
        return -1;
    }
    if ((bin_info_tbl_id > (MAX_TABLES - 1)) || !db->tables[bin_info_tbl_id]) {
        ERR("Failed to get table");
        return -2;
    }
    bin_info_btl = db->tables[bin_info_tbl_id];
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("Bin information with %lu rows is received.\n",
         bin_info_btl->num_rows);
#endif

    std::vector<table_t *> lhs_bins, rhs_bin;
    // rtn = fill_bins(bin_info_btl, num_bins, num_rows_per_bin, &bins)
    // if (rtn)) {
    //     return -3;
    // }

    return 0;
}

int fill_bins(data_base_t *db, table_t * data_table, table_t *bin_info_table, int num_bins,
              int num_rows_per_bin, schema_t *bin_sc,
              std::vector<table_t *> *bins) {
    for (int i = 0; i < num_bins; ++i) {
        std::string bin_name = "join:bp:bin_";
        bin_name += std::to_string(i);
        table_t *tmp;
        create_table(db, bin_name, bin_sc, &tmp);
        bins->push_back(tmp);
    }
    assert(bins->size() == num_bins);

    // Load one datablock of bin information into the memory at a time
    // TODO: parallelize this in per dblk level
    int dblk_size = num_bins * num_rows_per_bin;

    int dblk_cnt = 0;
    // For each datablock
    for (int row_num = 0; row_num < bin_info_table->num_rows; ++dblk_cnt) {
        // Load bin information
        // This map stores the information about a value should be placed in
        // which cell.
        std::unordered_map<std::string, int> placement;
        // For each cell
        for (int cell_num = 0; cell_num < num_bins; ++cell_num) {
            // For each value
            for (int i = 0; i < num_rows_per_bin; ++i) {
                row_t row;
                if (read_row(bin_info_table, row_num++, &row)) {
                    ERR("Failed to read row");
                    return -1;
                }
                std::string value((char*)get_column(&(bin_info_table->sc), 0, &row));
                const auto it = placement.find(value);
                if (it != placement.end()) {
                    ERR("Value %s already existed in another bin %d\n", value.c_str(), it->second);
                }
                placement[value] = cell_num;
            }
        }

        // Scan the data table and fill bin according to the bin information
        for (int data_row_num = 0; data_row_num < NULL /* we want the dblk_size of the actual table */ ; ++data_row_num) {

        }
    }

    return 0;
}