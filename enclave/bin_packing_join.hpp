#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include <vector>

#include "db.hpp"

// Bin Packing Phase 3: fill bins
// Fill the bins with the actual value based on what we figured out in Phase 2.
// `db`: the database to store temporary tables .
// `data_table`: the table that stores the actual data.
// `rows_per_dblk`: number of rows per datablock in `data_table`.
// `bin_info_table`: the table that stores the bin information that we collection in Phase 2.
// `num_bins`: number of bins.
// `num_rows_per_bins`: number of rows in a bin in the `bin_info_table`.
// `bin_sc`: The schema of the bins. We use this to create the actual bins.
// `column_offset`: Where the two tables are sperated in the `bin_sc`.
// `bins`: The actual bins, which is the output of Phase 3 and the input of Phase 4.
// TODO: parallelize this
int fill_bins(data_base_t *db, table_t *data_table, int column,
              const int rows_per_dblk, table_t *bin_info_table,
              const int start_dblk, const int end_dblk, const int num_bins,
              const int rows_per_cell, schema_t *bin_sc,
              std::vector<table_t *> *bins);
// Helper for fill_bins
// Fill one bin. The information of the bin is located at
// bin_info_table[begin:end)
int fill_bin(table_t *bin_info_table, int begin, int end, int num_bins,
             int num_rows_per_bin, table_t *bin);

// Bin Packing Phase 4: join each individual bin
// For each individual bin, do a hash join, do padding(while pretending not),
// and write the results out
int join_bins();

#endif // E_BIN_PACKING_JOIN_HPP