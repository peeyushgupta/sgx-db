#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include <vector>

#include "db.hpp"

// Bin Packing Phase 3: fill bins
// Fill the bins with the actual value based on what we figured out in Phase 2.
// `db`: the database to store temporary tables .
// `data_table`: the table that stores the actual data.
// `rows_per_dblk`: number of rows per datablock in `data_table`.
// `bin_info_table`: the table that stores the bin information that we
//      collection in Phase 2. `start_dblk`: the first datablock we need to read
//      in
// `bin_info_table` `end_dblk`: the last datablock we need to read in
// `bin_info_table` `num_bins`: number of bins. `num_rows_per_bins`: number of
//      rows in a bin in the `bin_info_table`. `bin_sc`: The schema of the bins.
//      We use this to create the actual bins. `column_offset`: Where the two
//      tables are sperated in the `bin_sc`.
// `bins`: The actual bins, which is the output of
// Phase 3 and the input of Phase 4.
// TODO: parallelize this
// TODO: get `bin_sc` from table
int fill_bins(data_base_t *db, table_t *data_table, int column,
              const int rows_per_dblk, table_t *bin_info_table,
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
#endif // E_BIN_PACKING_JOIN_HPP