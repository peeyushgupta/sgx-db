#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include <vector>

// Bin Packing Phase 3: fill bins
// Fill the bins with the actual value based on what we figured out in Phase 2
// TODO: parallelize this
int fill_bins(table_t *bin_info_table, int num_bins,
                        int num_rows_per_bin, std::vector<table_t *> *bins);
// Helper for fill_bins
// Fill one bin. The information of the bin is located at bin_info_table[begin:end)
int fill_bin(table_t *bin_info_table, int begin, int end, int num_bins,
                        int num_rows_per_bin, table_t *bin);

// Bin Packing Phase 4: join each individual bin
// For each individual bin, do a hash join, do padding(while pretending not),
// and write the results out
int join_bins();

#endif // E_BIN_PACKING_JOIN_HPP