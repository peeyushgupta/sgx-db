#include "bin_packing_join.hpp"

#include <cassert>

#include "db.hpp"
#include "util.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

// Perform phase3 and phase4 of the bin_packing_join
// `bin_info_tid`: table id of the table that stores the information about the
// bins `midpoints`: where the table_left and table_right seperate
// `num_rows_per_bin`: number of rows in eacn input/output bin
int ecall_bin_pack_join(int db_id, join_condition_t *c, int *join_tbl_id,
                        int bin_info_tbl_id, int midpoint, int num_bins,
                        int num_rows_per_bin) {
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

    std::vector<table_t *> lhs_bins, rhs_bin;
    // if (fill_bins(bin_info_btl, num_bins, num_rows_per_bin, &bins)) {
    //     return -3;
    // }

    return 0;
}

int fill_bins(data_base_t *db, table_t *bin_info_table, int num_bins,
              int num_rows_per_bin, schema_t *bin_sc, std::vector<table_t *> *bins) {
    for (int i = 0; i < num_bins; ++i) {
        std::string bin_name = "join:bp:bin";
        bin_name += std::to_string(i);
        table_t *tmp;
        create_table(db, bin_name, bin_sc, &tmp);
        bins->push_back(tmp);
    }
    assert(bins->size() == num_bins);

    int dblk_size = num_bins * num_rows_per_bin;
    // TODO: parallelize this in per dblk level
    
    return 0;
}