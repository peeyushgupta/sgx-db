#include "hash_bin_packing_join.hpp"

#include <algorithm>
#include <cerrno>
#include <sgx_tcrypto.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db.hpp"
#include "dbg.hpp"
#include "sort_helper.hpp"
#include "time.hpp"
#include "util.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

using namespace bin_packing_join;
using namespace bin_packing_join::hash_bin_packing_join;

#define RETURN_IF_FAILED(x)                                                    \
    {                                                                          \
        if (x)                                                                 \
            return x;                                                          \
    }

#ifdef MAX_HEAP_SIZE
const size_t max_heap_size = MAX_HEAP_SIZE;
#else
const size_t max_heap_size = 7E7;
#endif
const size_t usable_heap_size = max_heap_size / 2;

// Assuming only two table are joining
// Assuming only one joining column
// TODO(tianjiao): support multi column joining
int ecall_bin_packing_join(int db_id, join_condition_t *join_cond,
                           int *join_tbl_id) {
    int rtn = 0;
    data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    const int left_table_id = join_cond->table_left;
    if ((left_table_id > (MAX_TABLES - 1)) || !db->tables[left_table_id]) {
        return -2;
    }
    const int right_table_id = join_cond->table_right;
    if ((right_table_id > (MAX_TABLES - 1)) || !db->tables[right_table_id]) {
        return -2;
    }

    // Determine size of data blocks
    const size_t dblk_size = usable_heap_size /
                             (db->tables[left_table_id]->num_rows +
                              db->tables[right_table_id]->num_rows) /
                             MAX_ROW_SIZE;
    std::vector<std::vector<std::pair<hash_size_t, int>>> metadatas_left,
        metadatas_right;

    do {
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        unsigned long long start, end;
        unsigned long long cycles;
        double secs;
        start = RDTSC();
#endif

        // Number of occurance of each joining attribute across all datablocks
        std::unordered_map<hash_size_t, int> total_occurances;

        // if (collect_metadata(db_id, join_cond->table_left,
        //                      join_cond->fields_left[0], metadata_schema,
        //                      &total_occurances, &metadatas_left)) {
        //     ERR("Failed to collect metadata");
        //     rtn = -1;
        //     break;
        // }

        if (hash_bin_packing_join::collect_metadata(
                db_id, join_cond->table_right, join_cond->fields_right[0],
                dblk_size, &total_occurances, &metadatas_right)) {
            ERR("Failed to collect metadata");
            rtn = -1;
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();

        cycles = end - start;
        secs = (cycles / cycles_per_sec);

        INFO("Collecting metadata took %llu cycles (%f sec)\n", cycles, secs);
#endif

        std::vector<std::vector<std::vector<hash_size_t>>> bins;
        if (hash_bin_packing_join::pack_bins(&total_occurances, metadatas_right,
                                             &bins)) {
            ERR("Failed to pack bin\n");
            rtn = -1;
            break;
        }

    } while (0);

    // Clean up

    return rtn;
}

int collect_metadata(
    int db_id, int table_id, int column, const size_t dblk_size,
    std::unordered_map<hash_size_t, int> *total_occurances,
    std::vector<std::vector<std::pair<hash_size_t, int>>> *metadatas) {
    sgx_status_t sgx_status = SGX_ERROR_UNEXPECTED;
    // Check params
    data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    table_t *table = db->tables[table_id];
    const int rows_per_dblk = dblk_size / row_size(table);
    schema_t *schema = &(table->sc);

    int row_num = 0;
    while (row_num < table->num_rows) {
        std::unordered_map<hash_size_t, int> counter;
        for (int i = 0; i < rows_per_dblk; ++i) {
            row_t row;
            if (read_row(table, row_num++, &row)) {
                return -3;
            }

            std::string val;
            if (schema->types[column] != TINYTEXT) {
                val = std::string((char *)get_column(schema, column, &row),
                                  schema->sizes[column]);
            } else {
                val = std::string((char *)get_column(schema, column, &row));
            }
            // Probably there's more secured/better way to get hash
            sgx_sha1_hash_t sh1_hash;
            sgx_status =
                sgx_sha1_msg((uint8_t *)val.c_str(), val.size(), &sh1_hash);
            if (sgx_status) {
                ERR("Failed to hash %s. Error code: %d\n", val.c_str(),
                    sgx_status);
                return sgx_status;
            }
            // This should take the 8 least significat bits of the hash.
            // https://en.cppreference.com/w/cpp/language/implicit_conversion#Integral_conversions
            const hash_size_t hash = (hash_size_t)sh1_hash;
            counter[hash]++;
            (*total_occurances)[hash]++;
        }

        std::vector<std::pair<hash_size_t, int>> sorted_counter;
        for (const auto &it : counter) {
            sorted_counter.emplace_back(std::move(it));
        }
        std::sort(
            sorted_counter.begin(), sorted_counter.end(),
            [](const auto &a, const auto &b) { return a.second > b.second; });
        metadatas->push_back(std::move(sorted_counter));
    }
    return 0;
}

int pack_bins(
    std::unordered_map<hash_size_t, int> *total_occurances,
    const std::vector<std::vector<std::pair<hash_size_t, int>>> &metadatas,
    std::vector<bin_t> *bins) {
    std::unordered_map<hash_size_t, int> last_seen; // <val, bin>
    for (auto &metadata : metadatas) {
        std::unordered_map<hash_size_t, int> curr_seen;
        for (const auto &kv : metadata) {
            // Get metadata
            const hash_size_t val = kv.first;
            const int count = kv.second;
            total_occurances->at(val) -= count;
#ifndef NDEBUG
            if (total_occurances->at(val) < 0) {
                ERR("Total occurance %d of %s is smaller than its current "
                    "occurance %d\n",
                    (*total_occurances)[val] + count, val, count);
                return -1;
            }

            if (total_occurances->at(val) == 0) {
                total_occurances->erase(val);
            }
#endif

            // Fit it into the cell
            // Metadata is pre-sorted
        }

        last_seen.swap(curr_seen);
    }

#ifndef NDEBUG
    if (!total_occurances->empty()) {
        ERR("There are %d joining attributes that are not packed into a bin\n",
            total_occurances->size());
        return -1;
    }
#endif

    return 0;
}