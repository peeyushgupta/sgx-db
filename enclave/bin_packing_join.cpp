#include "bin_packing_join.hpp"

#include <algorithm>
#include <cerrno>
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
                              db->tables[right_table_id]->num_rows);
    metadata_t metadatas_left, metadatas_right;

    do {
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        unsigned long long start, end;
        unsigned long long cycles;
        double secs;
        start = RDTSC();
#endif

        // Number of occurance of each joining attribute across all datablocks
        // TODO: get rid of this
        std::unordered_map<hash_value_t, int> total_occurances;

        // if (collect_metadata(db_id, join_cond->table_left,
        //                      join_cond->fields_left[0], metadata_schema,
        //                      &total_occurances, &metadatas_left)) {
        //     ERR("Failed to collect metadata");
        //     rtn = -1;
        //     break;
        // }

        if (collect_metadata(db_id, join_cond->table_right,
                             join_cond->fields_right[0], dblk_size, &total_occurances,
                             &metadatas_right)) {
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

        std::vector<std::vector<std::vector<hash_value_t>>> bins;
        if (pack_bins(&total_occurances, metadatas_right, &bins)) {
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
    std::unordered_map<hash_value_t, int> *total_occurances,
    metadata_t *metadata) {

    // Check params
    data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    table_t *table = db->tables[table_id];
    const int rows_per_dblk = dblk_size /* / row_size(table) */;
    schema_t *schema = &(table->sc);

    
    // For each datablock
    
    for (int row_num = 0, dblk_num = 0; row_num < table->num_rows; dblk_num++) {
        // For each row
        std::unordered_map<hash_value_t, int> counter;
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
            // TODO: use SGX libs to get secured hash
            const hash_value_t hash = std::hash<std::string>{}(val);
            counter[hash]++;
        }
        // Populate metadata
        for (const auto& pair : counter) {
            auto it = metadata->find(pair.first);
            if (it == metadata->end()) {
                const auto res = metadata->emplace(pair.first, (metadata_value_t){pair.first, 0, {}});
                if (res.second == false) {
                    ERR("Failed to insert pair into metadata");
                    return -1;
                }
                it = res.first;
            }
            it->second.count += pair.second;
            it->second.dblks.emplace_back(dblk_num, pair.second);
        }
    }
    return 0;
}

int pack_bins(std::unordered_map<hash_value_t, int> *total_occurances,
              const metadata_t &metadatas,
              std::vector<bin_t> *bins) {
    // TODO: get rid of this
    return 0;
    std::unordered_map<hash_value_t, int> last_seen; // <val, bin>
    for (auto &metadata : metadatas) {
        std::unordered_map<hash_value_t, int> curr_seen;
        row_t row;
        for (const auto &kv : metadata.second.dblks) {
            // Get metadata
            const int dblk_num = kv.first;
            const int count = kv.second;
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

    return 0;
}
