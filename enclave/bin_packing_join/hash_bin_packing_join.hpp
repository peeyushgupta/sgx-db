// Just a little bit different from the standard bin_packing_join.
// hash_bin_packing_join hashes its key so that the memory usage is reduce on
// large key size;

#pragma once

#include "db.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace bin_packing_join::hash_bin_packing_join {
typedef std::size_t hash_size_t;    // The ID of each unique key
// [cell_t<size_of_cell, [value_t<value, occurances>]>]
typedef std::vector<std::pair<int, std::vector<std::pair<hash_size_t, int>>>>
    bin_info_t;

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
                        std::vector<bin_info_t> *bins);

// Bin Packing Phase 2.1: calculate number of rows per bin in the output
int out_bin_info_collection(const std::vector<bin_info_t> &bins, int midpoint,
                            int *num_rows_per_out_bin);

// Bin Packing Phase 3: fill bins
// Fill the bins with the actual value based on what we figured out in Phase 2.
// `db`: the database to store temporary tables .
// `data_table`: the table that stores the actual data.
// `rows_per_dblk`: number of rows per datablock in `data_table`.
// `bin_info_table`: the table that stores the bin information that we
//      collection in Phase 2.
// `start_dblk`: the first datablock we need to read
//      in `bin_info_table`
// `end_dblk`: the last datablock we need to read in
// `bin_info_table` `num_bins`: number of bins.
// `num_rows_per_bins`: number of rows in a bin in the `bin_info_table`.
// `bin_sc`: The schema of the bins. We use this to create the actual bins.
// `column_offset`: Where the two tables are sperated in the `bin_sc`.
// `bins`: The actual bins, which is the output of Phase 3 and the input of
// Phase 4.
// TODO: parallelize this
// TODO: get `bin_sc` from table
int fill_bins(data_base_t *db, table_t *data_table, int column,
              const int rows_per_dblk, const std::vector<bin_info_t> &info_bins,
              const int start_dblk, const int end_dblk, const int num_bins,
              const int rows_per_cell, schema_t *bin_sc,
              std::vector<table_t *> *bins);
// Helper for fill_bins
// Fill bin in a datablock. The information of the bin is located at
// `row_num`: to keep track of which row are we on in the `data_table.
//  Increment as we scan through the `data_table`.
int fill_bins_per_dblk(table_t *data_table, int column, int *data_row_num,
                       const int rows_per_dblk, const int dblk_cnt,
                       table_t *bin_info_table, int *info_row_num,
                       const int rows_per_cell, schema_t *bin_sc,
                       std::vector<table_t *> *bins);

// Bin Packing Phase 4: join each individual bin
// For each individual bin, do a hash join, do padding(while pretending not),
// and write the results out
int join_bins(table_t *lhs_tbl, const int lhs_column, table_t *rhs_tbl,
              const int rhs_column, schema_t *join_sc, table_t *join_tbl,
              const int num_rows_per_out_bin, int bin_id);
} // namespace bin_packing_join::hash_bin_packing_join
