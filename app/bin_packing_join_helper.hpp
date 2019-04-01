// Implementation of the bin-packing based join algorithm
// Multi table/column join is not supported currently
// Concurrent calls to `bin_packing_join` is also not supported
// The first two phases, metadata collection and bin information creation, are
// assumed to be done in client side.
#pragma once

#include "db.hpp"
#include <sgx_eid.h>
#include <string>
#include <unordered_map>
#include <vector>

int bin_packing_join(sgx_enclave_id_t eid, int db_id,
                     join_condition_t *join_cond, const std::string &csv_left,
                     const std::string &csv_right, int *out_tbl_id);

// A vector of <size, values>
typedef std::vector<std::pair<int, std::vector<std::pair<std::string, int>>>>
    bin_t;
// The type of a value in the metadata
// <value, occurances, datablocks_that_it_occurs>
struct metadata_value_t {
    int count;
    int max_cnt; // the max count of this value among different datablocks
    std::vector<std::pair<int, int>> dblks; // <dblk_num, count>
};
typedef std::unordered_map<std::string, metadata_value_t> metadata_t;

// Bin Packing Phase 1: metadata collection
// Collect information about each datablock
// Assuming only two table are joining
// Assuming only one joining column
// Assuming the corresponding tables are already created in the enclave.
int collect_metadata(const std::string &filename, int column,
                     const size_t rows_per_dblk, int *dblk_count,
                     metadata_t *metadata);

// Bin Packing Phase 2: bin information collection
// Figure out which value goes into which cell based on the metadata
// Possible optimization: remove empty attributes; only merge adjency bins
int bin_info_collection(const int dblk_count, const metadata_t &metadata,
                        std::vector<bin_t> *bins);

// Bin Packing Phase 2.1: calculate number of rows per bin in the output
int out_bin_info_collection(const std::vector<bin_t> &bins, int midpoint,
                            int *num_rows_per_out_bin);

// Bin Packing Phase 2.5: bin information to table
// Convert the bin information that we collected in Phase 2 into a table so we
// can read it within the enclave in Phase 3
int bin_info_to_table(sgx_enclave_id_t eid, int db_id,
                      const std::vector<bin_t> &bins,
                      const std::string &tbl_name, int *rows_per_cell,
                      int *bin_info_tbl_id);

int verifyBins(const std::vector<bin_t> &bins);