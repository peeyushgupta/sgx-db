#include "hash_bin_packing_join.hpp"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
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

#if !defined(MAX_HEAP_SIZE)
// 70MB
#define MAX_HEAP_SIZE 7E7
#endif
const size_t usable_heap_size = (MAX_HEAP_SIZE - DATA_BLKS_PER_DB * DATA_BLOCK_SIZE) * 0.8;

// Assuming only two table are joining
// Assuming only one joining column
// TODO(tianjiao): support multi column joining
int ecall_hash_bin_packing_join(int db_id, join_condition_t *join_cond,
                           int *join_tbl_id) {
    using namespace bin_packing_join::hash_bin_packing_join;
    // Validate input
    int rtn = 0;
    data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    const int left_table_id = join_cond->table_left;
    if ((left_table_id > (MAX_TABLES - 1)) || !db->tables[left_table_id]) {
        return -2;
    }
    table_t *left_table = db->tables[left_table_id];
    const int right_table_id = join_cond->table_right;
    if ((right_table_id > (MAX_TABLES - 1)) || !db->tables[right_table_id]) {
        return -2;
    }
    table_t *right_table = db->tables[right_table_id];


    // Determine size of data blocks
    const size_t dblk_size = usable_heap_size;
    const size_t rows_per_dblk = dblk_size / MAX_ROW_SIZE;
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("Datablock size: %lu bytes.\n", dblk_size);
    INFO("Rows per datablock: %lu rows.\n", rows_per_dblk);
#endif
    if (rows_per_dblk <= 0) {
        ERR("Usable SGX memory is smaller than one row\n");
    }

    metadata_t metadata;
    do {
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        unsigned long long start, end, total_start, total_end;
        unsigned long long cycles;
        double secs;
        total_start = start = RDTSC();
#endif

        int dblk_cnt = 0;
        rtn = collect_metadata(left_table, join_cond->fields_left[0],
                               rows_per_dblk, &dblk_cnt, &metadata);
        if (rtn) {
            ERR("Failed to collect metadata\n");
            break;
        }
        int midpoint = dblk_cnt;

        rtn = collect_metadata(right_table, join_cond->fields_right[0],
                               rows_per_dblk, &dblk_cnt, &metadata);
        if (rtn) {
            ERR("Failed to collect metadata\n");
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();

        cycles = end - start;
        secs = (cycles / cycles_per_sec);

        INFO("Phase 1: collecting metadata took %llu cycles (%f sec)\n", cycles,
             secs);
        start = RDTSC();
#endif

        std::vector<bin_t> bins;
        rtn = bin_info_collection(dblk_cnt, metadata, &bins);
        if (rtn) {
            ERR("Failed to pack bin\n");
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();

        cycles = end - start;
        secs = (cycles / cycles_per_sec);

        INFO("Phase 2: bin information collection took %llu cycles (%f sec)\n",
             cycles, secs);
        start = RDTSC();
#endif

        int num_rows_per_out_bin;
        rtn = out_bin_info_collection(bins, midpoint, &num_rows_per_out_bin);
        if (rtn) {
            ERR("Failed to collect output bin information.\n");
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();

        cycles = end - start;
        secs = (cycles / cycles_per_sec);

        INFO("Phase 2.1: output-bin information collection took %llu cycles "
             "(%f sec)\n",
             cycles, secs);
        start = RDTSC();
#endif

        // Perform Phase 3


#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        total_end = RDTSC();

        cycles = total_end - total_start;
        secs = (cycles / cycles_per_sec);

        INFO("Finished: Bin-Packing-Based Merge Join takes %llu cycles (%f sec)\n", cycles,
             secs);
#endif

    } while (0);

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    if (rtn == 0) {
        // ecall_print_table_dbg(eid, &rtn, db_id, *out_tbl_id, 0, 1 << 20);
    }
#endif
    // Clean up
    return rtn;
}

namespace bin_packing_join::hash_bin_packing_join{

int collect_metadata(table_t* table, int column,
                     const size_t rows_per_dblk, int *dblk_count,
                     metadata_t *metadata) {
    sgx_status_t sgx_status = SGX_ERROR_UNEXPECTED;
    int original_dblk_cnt = *dblk_count;
    int row_num;
    for (row_num = 0; row_num < table->num_rows; ++(*dblk_count)) {
        std::unordered_map<hash_size_t, int> counter;
        for (int i = 0; i < rows_per_dblk; ++i) {
            row_t row;
            if (read_row(table, row_num++, &row)) {
                ERR("Failed to read row\n");
            }
            char* value = (char *)get_column(&table->sc, column, &row);
            int value_len = strlen(value);
            sgx_sha1_hash_t sh1_hash;
            sgx_status =
                sgx_sha1_msg((uint8_t *)value, value_len, &sh1_hash);
            if (sgx_status) {
                ERR("Failed to hash %s. Error code: %d\n", value,
                    sgx_status);
                return sgx_status;
            }
            // This should take the 8 least significat bits of the hash.
            // https://en.cppreference.com/w/cpp/language/implicit_conversion#Integral_conversions
            const hash_size_t hashed_value = (hash_size_t)sh1_hash;
            counter[hashed_value]++;   // TODO: using string_view?
            row_num++;
        }

        // Populate the the metadata after reading each datablock.
        for (const auto &[key, occurances] : counter) {
            metadata_value_t *metadata_value = &((*metadata)[key]);
            metadata_value->count += occurances;
            metadata_value->max_cnt =
                std::max(metadata_value->max_cnt, occurances);
            metadata_value->dblks.emplace_back(*dblk_count, occurances);
        }
    }

    // Flush the last counter
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("%d rows read and %d datablocks of metadata collected for %s.\n",
         row_num, *dblk_count - original_dblk_cnt, table->name.c_str());
#endif

    return 0;
}

// Attempt to merge `b` into `a`. Nothing happens and return false if failed.
bool mergeBins(bin_t *a, const bin_t *b, const int cell_size) {
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
int bin_info_collection(const int dblk_count, const metadata_t &metadata,
                        std::vector<bin_t> *bins) {
    const int cell_size = usable_heap_size / dblk_count / MAX_ROW_SIZE;
    if (cell_size <= 0) {
        ERR("Too many datablocks created.\n");
        return -1;
    }
    std::vector<metadata_value_t> sorted_meta;
    // TODO: can be optimized using std::move once the algo is stablized
    std::transform(metadata.cbegin(), metadata.cend(),
                   std::back_inserter(sorted_meta),
                   [](const auto &it) { return it.second; });

    // Do we really need this?
    std::sort(sorted_meta.begin(), sorted_meta.end(),
              [](const auto &a, const auto &b) {
                  return std::tie(a.max_cnt, a.count) >
                         std::tie(b.max_cnt, b.count);
              });

    std::vector<bin_t> res;
    bin_t last_bin(dblk_count);

    for (const auto &it : metadata) {
        auto &dblks = it.second.dblks;
        bin_t bin(dblk_count);
        for (const auto &pair : dblks) {
            bin[pair.first].first += pair.second;
            bin[pair.first].second.emplace_back(it.first, pair.second);
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
    INFO("%lu bins created with %d cells each bin and %d values each cell. "
         "Expect to have bin info table with %lu rows.\n",
         res.size(), dblk_count, cell_size,
         res.size() * dblk_count * cell_size);

#if defined(REPORT_BIN_PACKING_JOIN_PRINT_BIN)
    for (const bin_t &bin : res) {
        for (const auto &cell : bin) {
            printf("%3d ", cell.first);
        }
        printf("\n");
    }
#endif

#endif

    // TODO: remove this line once the algo is stable
    bins->swap(res);
    return 0;
}

int out_bin_info_collection(const std::vector<bin_t> &bins, int midpoint,
                            int *num_rows_per_out_bin) {
    if (bins.empty()) {
        ERR("Bin can't be empty");
        return -1;
    }
    *num_rows_per_out_bin = 0;

    for (const bin_t &bin : bins) {
        int sum = 0;
        std::unordered_map<hash_size_t, int> lhs, rhs;
        for (int i = 0; i < midpoint; ++i) {
            const auto &cell = bin[i].second;
            for (const auto &value : cell) {
                lhs.emplace(value);
            }
        }
        for (int i = midpoint; i < bin.size(); ++i) {
            const auto &cell = bin[i].second;
            for (const auto &value : cell) {
                rhs.emplace(value);
            }
        }

        for (const auto &value : rhs) {
            sum += value.second * lhs[value.first];
        }
        *num_rows_per_out_bin = std::max(*num_rows_per_out_bin, sum);
    }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("There will be %d rows in each output bin.\n", *num_rows_per_out_bin);
#endif

    if (*num_rows_per_out_bin <= 0) {
        ERR("num_rows_per_out_bin %d should be greater than 0.\n",
            *num_rows_per_out_bin);
        return -1;
    }

    return 0;
}

} // namespace hash_bin_packing_join