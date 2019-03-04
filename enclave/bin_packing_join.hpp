#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include "db.hpp"
#include <string>
#include <unordered_map>
#include <vector>

// Move it outside of the enclave?
int collect_metadata(int db_id, int table_id, int column,
                     schema_t metadata_schema,
                     std::unordered_map<std::string, int> *total_occurances,
                     std::vector<table_t *> *metadatas);

int pack_bins(std::unordered_map<std::string, int> *total_occurances,
             const std::vector<table_t *> &metadatas,
             std::vector<std::vector<std::string>> *bins,
             schema_t metadata_schema);

#endif // E_BIN_PACKING_JOIN_HPP