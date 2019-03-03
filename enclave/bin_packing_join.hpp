#ifndef E_BIN_PACKING_JOIN_HPP
#define E_BIN_PACKING_JOIN_HPP

#include <string>
#include <unordered_map>
#include <vector>

struct join_condition;


// Move it outside of the enclave?
int collect_metadata(int db_id, join_condition* join_cond, std::vector<std::unordered_map<std::string, int>>* metadata);

#endif // E_BIN_PACKING_JOIN_HPP