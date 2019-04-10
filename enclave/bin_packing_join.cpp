#include "bin_packing_join.hpp"

#include <bitset>
#include <cstring>
#include <unordered_map>

#include "db.hpp"
#include "time.hpp"
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
        ERR("Failed to get lhs table\n");
        return -2;
    }
    lhs_tbl = db->tables[lhs_tbl_id];
    int rhs_tbl_id = join_cond->table_right;
    if ((rhs_tbl_id > (MAX_TABLES - 1)) || !db->tables[rhs_tbl_id]) {
        ERR("Failed to get rhs table\n");
        return -2;
    }
    rhs_tbl = db->tables[rhs_tbl_id];
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("Bin information with %lu rows is received.\n",
         bin_info_btl->num_rows);
#endif

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    unsigned long long start, end;
    unsigned long long cycles;
    double secs;
#endif

    std::vector<table_t *> lhs_bins, rhs_bins;
    rtn = fill_bins(db, lhs_tbl, join_cond->fields_left[0], rows_per_dblk,
                    bin_info_btl, 0, midpoint, num_bins, rows_per_cell,
                    &(lhs_tbl->sc), &lhs_bins);
    if (rtn) {
        ERR("Failed to fill lhs bins\n");
        return rtn;
    }

    rtn = fill_bins(db, rhs_tbl, join_cond->fields_right[0], rows_per_dblk,
                    bin_info_btl, midpoint, -1, num_bins, rows_per_cell,
                    &(rhs_tbl->sc), &rhs_bins);
    if (rtn) {
        ERR("Failed to fill lhs bins\n");
        return rtn;
    }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    end = RDTSC();
    cycles = end - start;
    secs = (cycles / cycles_per_sec);
    INFO("Phase 3: fill bins took %llu cycles (%f sec)\n", cycles, secs);
#endif

    schema_t join_sc;
    join_schema(&join_sc, &lhs_tbl->sc, &rhs_tbl->sc);
    std::string join_tbl_name = "join:bp:result";
    table_t *join_tbl;
    if (create_table(db, join_tbl_name, &join_sc, &join_tbl)) {
        ERR("Failed to create table %s\n", join_tbl_name.c_str());
        return -1;
    }
    for (int i = 0; i < num_bins; ++i) {
        rtn = join_bins(lhs_bins[i], join_cond->fields_left[0], rhs_bins[i],
                        join_cond->fields_right[0], &join_sc, join_tbl,
                        num_rows_per_out_bin);
        if (rtn) {
            ERR("Failed to join bin %d out of %d\n", i, num_bins);
            return rtn;
        }
    }
    *join_tbl_id = join_tbl->id;

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    end = RDTSC();
    cycles = end - start;
    secs = (cycles / cycles_per_sec);
    INFO("Phase 4: join bins took %llu cycles (%f sec)\n", cycles, secs);
#endif

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("%d rows of joined rows is generated.\n", join_tbl->num_rows);
#endif

    return rtn;
}

int fill_bins(data_base_t *db, table_t *data_table, int column,
              const int rows_per_dblk, table_t *bin_info_table,
              const int start_dblk, const int end_dblk, const int num_bins,
              const int rows_per_cell, schema_t *bin_sc,
              std::vector<table_t *> *bins) {
    for (int i = 0; i < num_bins; ++i) {
        std::string bin_name = "join:bp:bin_";
        bin_name += std::to_string(i);
        table_t *tmp;
        if (create_table(db, bin_name, bin_sc, &tmp)) {
            ERR("Failed to create table %s\n", bin_name.c_str());
            return -1;
        }
        bins->push_back(tmp);
    }

    // Load one datablock of bin information into the memory at a time.
    // For each datablock, data will be stored in temp_bins before we flush them
    // to the actual bins for security measure.
    // TODO: parallelize this in per dblk level.
    int dblk_cnt = 0;
    int data_row_num = 0;
    const int start_row = start_dblk * num_bins * rows_per_cell;
    const int end_row = end_dblk < 0 ? bin_info_table->num_rows
                                     : end_dblk * num_bins * rows_per_cell;
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("Reading bin info table from row %d to %d\n", start_row, end_row);
#endif
#if !defined(NDEBUG)
    bool data_table_exhausted = false;
#endif
    // For each datablock
    for (int row_num = start_row; row_num < end_row; ++dblk_cnt) {
        // Load bin information
        // This map stores the information about a value should be placed in
        // which cell.
        std::unordered_map<std::string, int> placement;
        int byte_read_info = 0;
        // For each cell
        for (int cell_num = 0; cell_num < num_bins; ++cell_num) {
            // For each value
            // DBG("outer\n");
            for (int i = 0; i < rows_per_cell; ++i) {
                row_t row;
#if !defined(NDEBUG)
                if (row_num >= bin_info_table->num_rows) {
                    ERR("Reach the end of data table but still have more rows "
                        "to read\n");
                    return -1;
                }

#endif

                if (read_row(bin_info_table, row_num++, &row)) {
                    ERR("Failed to read row");
                    return -1;
                }

                if (row.header.fake) {
                    continue;
                }
                std::string value(
                    (char *)get_column(&(bin_info_table->sc), 0, &row));
                byte_read_info += value.length();
#if !defined(NDEBUG)
                const auto &it = placement.find(value);
                if (it != placement.end()) {
                    ERR("Value \"%s\" is appeared in the datablock %d twice at "
                        "cell %d row %d\n",
                        value.c_str(), dblk_cnt, cell_num, i);
                    return -1;
                }
#endif
                if (!value.empty()) {
                    placement[value] = cell_num;
                }
            }
        }

        int byte_read_bin = 0;
        typedef std::vector<row_t> temp_bin_t;
        std::vector<temp_bin_t> temp_bins(num_bins);
        // Scan the data table and fill bin according to the bin information
        for (int i = 0; i < rows_per_dblk; ++i) {
            row_t row;

            if (data_row_num >= data_table->num_rows) {
#if !defined(NDEBUG)
                if (data_table_exhausted) {
                    ERR("Reach the end of data table but still have more rows "
                        "to read. Data table: dblk %d, curr_row %d, "
                        "rows_per_dblk. Info table: total rows %d, curr_row "
                        "%d %d\n",
                        dblk_cnt, i, rows_per_dblk, bin_info_table->num_rows,
                        row_num, 1);
                    return -1;
                }
                data_table_exhausted = true;
#endif
                break;
            }

            if (read_row(data_table, data_row_num++, &row)) {
                ERR("Failed to read row");
                return -1;
            }
            std::string value(
                (char *)get_column(&(data_table->sc), column, &row));
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

        row_t empty_row;
        memset(&empty_row, 0x0, sizeof(empty_row));
        // Flush temp_bins to bins
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
    }

    return 0;
}

int join_bins(table_t *lhs_tbl, const int lhs_column, table_t *rhs_tbl,
              const int rhs_column, schema_t *join_sc, table_t *join_tbl,
              const int num_rows_per_out_bin) {
    // Fill in the `mapping`
    // `mapping` map a joining value to rows in the `lhs_tbl`
    std::unordered_map<std::string, std::vector<row_t>> mapping;
    schema_t *lhs_sc = &lhs_tbl->sc;
    for (int row_num = 0; row_num < lhs_tbl->num_rows; ++row_num) {
        row_t row;
        if (read_row(lhs_tbl, row_num, &row)) {
            ERR("Failed to read row");
            return -1;
        }
        std::string value((char *)get_column(lhs_sc, lhs_column, &row));
        if (value.empty()) {
            continue;
        }
        try {
            mapping[value].push_back(row);
        } catch (const std::bad_alloc &) {
            ERR("Not enough memory: table_name %s, row_num %d, total_rows "
                "%d\n",
                lhs_tbl->name.c_str(), row_num, lhs_tbl->num_rows);
            return -1;
        }
    }

    schema_t *rhs_sc = &rhs_tbl->sc;
    int num_rows_inserted = 0;
    for (int row_num = 0; row_num < rhs_tbl->num_rows; ++row_num) {
        row_t row;
        if (read_row(rhs_tbl, row_num, &row)) {
            ERR("Failed to read row");
            return -1;
        }
        std::string value((char *)get_column(rhs_sc, rhs_column, &row));
        if (value.empty()) {
            continue;
        }

        row_t join_row;
        for (row_t &lhs_row : mapping[value]) {
            join_rows(&join_row, join_sc->row_data_size, &lhs_row,
                      lhs_sc->row_data_size, &row, rhs_sc->row_data_size, 0);
            insert_row_dbg(join_tbl, &join_row);
        }
    }
    row_t empty_row;
    memset(&empty_row, 0x0, sizeof(empty_row));
    for (; num_rows_inserted < num_rows_per_out_bin; ++num_rows_inserted) {
        insert_row_dbg(join_tbl, &empty_row);
    }

    return 0;
}