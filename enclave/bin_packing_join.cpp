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
// `rows_per_dblk`: number of rows in each input bin
// `num_rows_per_bin`: number of rows in eacn output bin.
int ecall_bin_pack_join(int db_id, join_condition_t *join_cond,
                        int rows_per_dblk, int num_rows_per_out_bin,
                        int bin_info_tbl_id, int midpoint, int num_bins,
                        int rows_per_cell, int *join_tbl_id,
                        int rows_per_out_bin) {
    int rtn = 0;
    data_base_t *db = get_db(db_id);
    table_t *bin_info_btl;
    table_t *lhs_tbl;
    table_t *rhs_tbl;
    if (!db) {
        ERR("Failed to get database");
        return -1;
    }
    if ((bin_info_tbl_id > (MAX_TABLES - 1)) || !db->tables[bin_info_tbl_id]) {
        ERR("Failed to get info table");
        return -2;
    }
    bin_info_btl = db->tables[bin_info_tbl_id];
    int lhs_tbl_id = join_cond->table_left;
    if ((lhs_tbl_id > (MAX_TABLES - 1)) || !db->tables[lhs_tbl_id]) {
        ERR("Failed to get lhs table");
        return -2;
    }
    lhs_tbl = db->tables[lhs_tbl_id];
    int rhs_tbl_id = join_cond->table_right;
    if ((rhs_tbl_id > (MAX_TABLES - 1)) || !db->tables[rhs_tbl_id]) {
        ERR("Failed to get lhs table");
        return -2;
    }
    rhs_tbl = db->tables[rhs_tbl_id];
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("Bin information with %lu rows is received.\n",
         bin_info_btl->num_rows);
#endif

    std::vector<table_t *> lhs_bins, rhs_bin;
    DBG("num rows %d\n", bin_info_btl->num_rows);
    rtn = fill_bins(db, lhs_tbl, join_cond->fields_left[0], rows_per_dblk,
                    bin_info_btl, num_bins, rows_per_cell, &(lhs_tbl->sc),
                    &lhs_bins);
    if (rtn) {
        ERR("Failed to fill lhs bins");
        return -2;
    }

    return rtn;
}

int fill_bins(data_base_t *db, table_t *data_table, int column,
              const int rows_per_dblk, table_t *bin_info_table,
              const int num_bins, const int rows_per_cell, schema_t *bin_sc,
              std::vector<table_t *> *bins) {
    return 0;
    for (int i = 0; i < num_bins; ++i) {
        std::string bin_name = "join:bp:bin_";
        bin_name += std::to_string(i);
        table_t *tmp;
        create_table(db, bin_name, bin_sc, &tmp);
        bins->push_back(tmp);
    }
    assert(bins->size() == num_bins);

    // Load one datablock of bin information into the memory at a time.
    // For each datablock, data will be stored in temp_bins before we flush them
    // to the actual bins for security measure.
    // TODO: parallelize this in per dblk level.
    int dblk_cnt = 0;
    int data_row_num = 0;
    typedef std::vector<row_t> temp_bin_t;
    std::vector<temp_bin_t> temp_bins(num_bins);
    // For each datablock
    DBG("%d\n", bin_info_table->num_rows);
    for (int row_num = 0; row_num < bin_info_table->num_rows; ++dblk_cnt) {
        // Load bin information
        // This map stores the information about a value should be placed in
        // which cell.
        DBG("info to map\n");
        std::unordered_map<std::string, int> placement;
        DBG("here u gos\n");
        // For each cell
        for (int cell_num = 0; cell_num < num_bins; ++cell_num) {
            // For each value
            // DBG("outer\n");
            for (int i = 0; i < rows_per_cell; ++i) {
                DBG("inner\n");
                row_t row;
                DBG("read row\n");
                assert(row_num < bin_info_table->num_rows);
                if (read_row(bin_info_table, row_num++, &row)) {
                    ERR("Failed to read row");
                    return -1;
                }
                DBG("read value\n");
                
                // TODO: we need to fake this
                if (row.header.fake) {
                    continue;
                }
                std::string value(
                    (char *)get_column(&(bin_info_table->sc), 0, &row));
                DBG("dblk %d, row %d, cell %d, i %d, fake %d, value %s\n", dblk_cnt, row_num, cell_num, i, row.header.fake, value.c_str());
#ifndef NDEBUG
                const auto it = placement.find(value);
                assert(it == placement.end());
#endif
                placement[value] = cell_num;
                DBG("done with ya %d\n", cell_num);
            }
        }

        DBG("fill bin\n");
        /*
        // Scan the data table and fill bin according to the bin information
        for (int i = 0; i < rows_per_dblk; ++i) {
            row_t row;
            assert(data_row_num < data_table->num_rows);
            DBG("Read row %d\n", data_table->num_rows);
            if (read_row(data_table, data_row_num++, &row)) {
                ERR("Failed to read row");
                return -1;
            }
            DBG("Read value %d %d\n", column, data_table->sc.types[column]);
            std::string value(
                (char *)get_column(&(data_table->sc), column, &row));
            DBG("value %s\n", value.c_str());
#ifndef NDEBUG
            const auto it = placement.find(value);
            assert(it != placement.end());
            assert(it->second >= 0);
            assert(it->second < temp_bins.size());
#endif
            temp_bins[placement[value]].emplace_back(std::move(row));
        }

        DBG("flush bin\n");
        // Flush temp_bins to bins
        for (int i = 0; i < temp_bins.size(); ++i) {
            temp_bin_t &temp_bin = temp_bins[i];
            assert(temp_bin.size() == rows_per_dblk / num_bins);
            for (row_t &row : temp_bin) {
                insert_row_dbg((*bins)[i], &row);
            }
        }
        */
    }

    return 0;
}