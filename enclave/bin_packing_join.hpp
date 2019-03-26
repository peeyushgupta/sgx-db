#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include <vector>

// Bin Packing Phase 3: fill bins
// Fill the bins with the actual value based on what we figured out in Phase 2
// TODO: parallize this
int fill_bins(table_t *bin_info_table, std::vector<table_t *> *bins);

// Bin Packing Phase 4: join each individual bin
// For each individual bin, do a hash join, do padding(while pretending not),
// and write the results out
int join_bins();

#endif // E_BIN_PACKING_JOIN_HPP