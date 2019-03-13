#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include "db.hpp"
#include <string>
#include <unordered_map>
#include <vector>

typedef std::size_t hash_value_t;
typedef std::vector<std::vector<hash_value_t>> bin_t;
// The type of a value in the metadata
// <value, occurances, datablocks_that_it_occurs>
struct metadata_value_t {
    hash_value_t value;
    int count;
    std::vector<std::pair<int, int>> dblks; // <dblk_num, count>
};
typedef std::unordered_map<hash_value_t, metadata_value_t> metadata_t;

// Collect metadata without sorting it
int collect_metadata(
    int db_id, int table_id, int column, const size_t dblk_size,
    std::unordered_map<hash_value_t, int> *total_occurances,
    metadata_t *metadata);

int pack_bins(
    std::unordered_map<hash_value_t, int> *total_occurances,
    const metadata_t &metadata,
    std::vector<bin_t> *bins);

#endif // E_BIN_PACKING_JOIN_HPP