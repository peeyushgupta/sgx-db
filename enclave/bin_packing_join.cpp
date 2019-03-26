#include "bin_packing_join.hpp"

#include "db.hpp"
#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

// Perform phase3 and phase4 of the bin_packing_join
// `bin_info_tid`: table id of the table that stores the information about the bins
// `midpoints`: where the table_left and table_right seperate
// `num_rows_per_bin`: number of rows in eacn input/output bin
int ecall_bin_pack_join(int db_id, join_condition_t *c, int *join_tbl_id,
                        int bin_info_tid, int midpoint, int num_bins,
                        int num_rows_per_bin) {
    return -1;
}