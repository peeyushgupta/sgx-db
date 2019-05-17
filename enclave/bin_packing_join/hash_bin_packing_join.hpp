// Just a little bit different from the standard bin_packing_join.
// hash_bin_packing_join hashes its key so that the memory usage is reduce on
// large key size;

#pragma once

#include "db.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace bin_packing_join {
namespace hash_bin_packing_join {
typedef std::size_t hash_size_t;    // The ID of each unique key
// [cell_t<size_of_cell, [value_t<value, occurances>]>]
typedef std::vector<std::pair<int, std::vector<std::pair<hash_size_t, int>>>>
    bin_t;

// The type of a value in the metadata
// <value, occurances, datablocks_that_it_occurs>
struct metadata_value_t {
    int count;
    int max_cnt; // the max count of this value among different datablocks
    std::vector<std::pair<int, int>> dblks; // <dblk_num, count>
};
typedef std::unordered_map<hash_size_t, metadata_value_t> metadata_t;

// Bin Packing Phase 1: metadata collection
// Collect information about each datablock
// Assuming only two table are joining
// Assuming only one joining column
// Assuming the corresponding tables are already created in the enclave.
int collect_metadata(table_t* table, int column,
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
} // namespace hash_bin_packing_join
} // namespace bin_packing_join
