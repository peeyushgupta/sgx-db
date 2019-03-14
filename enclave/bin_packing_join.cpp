#include "bin_packing_join.hpp"

#include <algorithm>
#include <cassert>
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
const size_t max_heap_size = 7E7; // 70MB
#endif
// TODO: make it full size
const size_t usable_heap_size = max_heap_size / 4;

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
    const size_t dblk_size = usable_heap_size;
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
        int dblk_cnt = 0;

        // if (collect_metadata(db_id, join_cond->table_left,
        //                      join_cond->fields_left[0], metadata_schema,
        //                      &total_occurances, &metadatas_left)) {
        //     ERR("Failed to collect metadata");
        //     rtn = -1;
        //     break;
        // }

        if (collect_metadata(db_id, join_cond->table_right,
                             join_cond->fields_right[0], dblk_size, &dblk_cnt,
                             &total_occurances, &metadatas_right)) {
            ERR("Failed to collect metadata");
            rtn = -1;
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();

        cycles = end - start;
        secs = (cycles / cycles_per_sec);

        INFO("Collecting metadata took %llu cycles (%f sec)\n", cycles, secs);
        start = RDTSC();
#endif

        std::vector<std::vector<std::vector<hash_value_t>>> bins;
        if (pack_bins(&total_occurances, dblk_cnt, metadatas_right, &bins)) {
            ERR("Failed to pack bin\n");
            rtn = -1;
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();

        cycles = end - start;
        secs = (cycles / cycles_per_sec);

        INFO("Pack bins took %llu cycles (%f sec)\n", cycles, secs);
        start = RDTSC();
#endif

    } while (0);

    // Clean up

    return rtn;
}

int collect_metadata(int db_id, int table_id, int column,
                     const size_t dblk_size, int *dblk_count,
                     std::unordered_map<hash_value_t, int> *total_occurances,
                     metadata_t *metadata) {

    // Check params
    data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    table_t *table = db->tables[table_id];
    // TODO: fix this
    const int rows_per_dblk = dblk_size / MAX_ROW_SIZE;
    if (rows_per_dblk <= 0) {
        ERR("Usable SGX memory is smaller than one row\n");
    }
    schema_t *schema = &(table->sc);

    // For each datablock

    for (int row_num = 0; row_num < table->num_rows; (*dblk_count)++) {
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
        for (const auto &pair : counter) {
            auto it = metadata->find(pair.first);
            if (it == metadata->end()) {
                const auto res = metadata->emplace(
                    pair.first, (metadata_value_t){pair.first, 0, {}});
                if (res.second == false) {
                    ERR("Failed to insert pair into metadata");
                    return -1;
                }
                it = res.first;
            }
            it->second.count += pair.second;
            it->second.dblks.emplace_back(*dblk_count, pair.second);
        }
    }
    return 0;
}

// A vector of <size, values>
typedef std::vector<std::pair<int, std::vector<hash_value_t>>> temp_bin_t;
// Attempt to merge `b` into `a`. Nothing happens and return false if failed.
bool mergeBins(temp_bin_t *a, const temp_bin_t *b, const int cell_size) {
    assert(a->size() == b->size());
    for (int i = 0; i < a->size(); ++i) {
        if ((*a)[i].first + (*b)[i].first > cell_size) {
            return false;
        }
    }

    for (int i = 0; i < a->size(); ++i) {
        (*a)[i].first += (*b)[i].first;

        std::copy((*b)[i].second.begin(), (*b)[i].second.end(),
                  std::back_inserter((*a)[i].second));
    }
    return true;
}

// Sort metadata and pack bins
int pack_bins(std::unordered_map<hash_value_t, int> *total_occurances,
              const int dblk_count, const metadata_t &metadata,
              std::vector<bin_t> *bins) {
    // TODO: The size of the joining value may not equal to MAX_ROW_SIZE
    const int cell_size = usable_heap_size / dblk_count / MAX_ROW_SIZE;
    if (cell_size <= 0) {
        ERR("Too many datablocks created.\n");
    }
    std::vector<metadata_value_t> sorted_meta;
    // TODO: can be optimized using std::move once the algo is stablized
    std::transform(metadata.cbegin(), metadata.cend(),
                   std::back_inserter(sorted_meta),
                   [](const auto &it) { return it.second; });

    // Do we really need this?
    std::sort(sorted_meta.begin(), sorted_meta.end(),
              [](const auto &a, const auto &b) { return a.count > b.count;
              });

    // Sort the count of each value in a cell in addtion to the `bin_t`

    std::vector<temp_bin_t> res;
    temp_bin_t last_bin(dblk_count);

    for (const auto &it : metadata) {
        auto &dblks = it.second.dblks;
        temp_bin_t bin(dblk_count);
        for (const auto &pair : dblks) {
            bin[pair.first].first += pair.second;
            bin[pair.first].second.push_back(it.second.value);
        }
        bool merged =
            std::any_of(res.begin(), res.end(), [&bin, cell_size](auto &b) {
                return mergeBins(&b, &bin, cell_size);
            });
        if (!merged) {
            res.emplace_back(std::move(bin));
        }
    }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("%d bins created with %d cells each bin and %d values each cell\n",
         res.size(), dblk_count, cell_size);
#endif

    return 0;
}
