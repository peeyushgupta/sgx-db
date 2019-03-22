// Implementation of the bin-packing based join algorithm
// The first two phases, metadata collection and bin information creation, are
// assumed to be done in client side.

#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include "db.hpp"
#include <string>
#include <unordered_map>
#include <vector>

int bin_packing_join(int db_id, join_condition_t *join_cond,
                     const std::string &csv_left, const std::string &csv_right,
                     int *out_tbl_id);

typedef std::vector<std::vector<std::string>> bin_t;
// The type of a value in the metadata
// <value, occurances, datablocks_that_it_occurs>
struct metadata_value_t {
    int count;
    std::vector<std::pair<int, int>> dblks; // <dblk_num, count>
};
typedef std::unordered_map<std::string, metadata_value_t> metadata_t;

// Assuming only two table are joining
// Assuming only one joining column
// Assuming the corresponding tables are already created in the enclave.
int collect_metadata(const std::string& filename, int column,
                     const size_t rows_per_dblk, int *dblk_count,
                     metadata_t *metadata);

// Possible optimization: 
int pack_bins(const int dblk_count, const metadata_t &metadata,
              std::vector<bin_t> *bins);

int verifyBins(const std::vector<bin_t> &bins);

#endif // E_BIN_PACKING_JOIN_HPP