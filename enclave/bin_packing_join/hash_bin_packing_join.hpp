// Just a little bit different from the standard bin_packing_join.
// hash_bin_packing_join hashes its key so that the memory usage is reduce on
// large key size;

#pragma once

#include "db.hpp"
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace bin_packing_join {
namespace hash_bin_packing_join {
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
} // namespace hash_bin_packing_join
} // namespace bin_packing_join
