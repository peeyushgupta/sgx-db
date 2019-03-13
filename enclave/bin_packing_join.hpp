#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include "db.hpp"
#include <string>
#include <unordered_map>
#include <vector>

typedef std::size_t hash_size_t;
typedef std::vector<std::vector<hash_size_t>> bin_t;

// Move it outside of the enclave?
int collect_metadata(
    int db_id, int table_id, int column, const size_t dblk_size,
    std::unordered_map<hash_size_t, int> *total_occurances,
    std::vector<std::vector<std::pair<hash_size_t, int>>> *metadatas);

int pack_bins(
    std::unordered_map<hash_size_t, int> *total_occurances,
    const std::vector<std::vector<std::pair<hash_size_t, int>>> &metadatas,
    std::vector<bin_t> *bins);

#endif // E_BIN_PACKING_JOIN_HPP