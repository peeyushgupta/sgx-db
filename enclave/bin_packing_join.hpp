#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include "db.hpp"
#include <string>
#include <unordered_map>
#include <vector>

// Move it outside of the enclave?
int collect_metadata(int db_id, join_condition *join_cond,
                     std::unordered_map<std::string, int> *total_occurances,
                     std::vector<table_t *> *metadatas);

int pack_bin(const std::unordered_map<std::string, int> &total_occurances,
             const std::vector<table_t *> &metadatas,
             std::vector<std::vector<std::string>> *bins);

#endif // E_BIN_PACKING_JOIN_HPP