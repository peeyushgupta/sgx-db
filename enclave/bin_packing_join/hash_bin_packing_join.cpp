#include "hash_bin_packing_join.hpp"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bin_packing_join.hpp"
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
// TODO: set this to 0.8
const size_t usable_heap_size =
    (MAX_HEAP_SIZE - DATA_BLKS_PER_DB * DATA_BLOCK_SIZE) * 0.8;

// Helpers
namespace {
using namespace bin_packing_join::hash_bin_packing_join;
static inline hash_size_t get_hash(char *data, uint32_t len) {
    sgx_sha1_hash_t sh1_hash;
    sgx_status_t sgx_status = sgx_sha1_msg((uint8_t *)data, len, &sh1_hash);
    if (sgx_status) {
        data[len] = '\n';
        ERR("Failed to hash %s. Error code: %d\n", data, sgx_status);
        assert(false);
        return 0;
    }
    // This should take the 8 least significat bits of the hash.
    // https://en.cppreference.com/w/cpp/language/implicit_conversion#Integral_conversions
    hash_size_t rtn = sh1_hash[0];
    return rtn;
}
static inline hash_size_t get_hash(char *str) {
    int value_len = strlen(str);
    return get_hash(str, value_len);
}
} // namespace

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
    std::vector<table_t *> lhs_bins, rhs_bins;
    table_t *join_tbl;
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
        INFO("%d datablocks of metadata is collected in total\n", metadata.size());

        end = RDTSC();
        cycles = end - start;
        secs = (cycles / cycles_per_sec);
        INFO("Phase 1: collecting metadata took %llu cycles (%f sec)\n", cycles,
             secs);
        start = RDTSC();
#endif

        std::vector<bin_info_t> bins;
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

        INFO("Finished: Bin-Packing-Based Merge Join takes %llu cycles (%f "
             "sec)\n",
             cycles, secs);
#endif
        int rows_per_cell = 0;
        for (const bin_info_t &bin : bins) {
            for (const auto &cell : bin) {
                rows_per_cell = std::max(rows_per_cell, cell.first);
            }
        }

        lhs_bins = rhs_bins =
            std::vector<table_t *>(bins.size(), nullptr);
        rtn =
            fill_bins(db, left_table, join_cond->fields_left[0], rows_per_dblk,
                      bins, 0, midpoint, rows_per_cell, &lhs_bins);
        if (rtn) {
            ERR("Failed to fill lhs bins\n");
            break;
        }

        // ERR: contents in `lhs_bins` are corrupted after this call;
        rtn = fill_bins(db, right_table, join_cond->fields_right[0],
                        rows_per_dblk, bins, midpoint, dblk_cnt, rows_per_cell,
                        &rhs_bins);
        if (rtn) {
            ERR("Failed to fill lhs bins\n");
            break;
        }

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();
        cycles = end - start;
        secs = (cycles / cycles_per_sec);
        start = end;
        INFO("Phase 3: fill bins took %llu cycles (%f sec)\n", cycles, secs);
#endif

        schema_t join_sc;
        join_schema(&join_sc, &left_table->sc, &right_table->sc);
        std::string join_tbl_name = "join:bp:result";
        rtn = create_table(db, join_tbl_name, &join_sc, &join_tbl);
        if (rtn) {
            ERR("Failed to create table %s\n", join_tbl_name.c_str());
            break;
        }
        for (int i = 0; i < bins.size(); ++i) {
            rtn = join_bins(lhs_bins[i], join_cond->fields_left[0], rhs_bins[i],
                            join_cond->fields_right[0], &join_sc, join_tbl,
                            num_rows_per_out_bin, i);
            if (rtn) {
                ERR("Failed to join bin %d out of %d\n", i, bins.size());
                break;
            }
        }
        *join_tbl_id = join_tbl->id;

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        end = RDTSC();
        cycles = end - start;
        secs = (cycles / cycles_per_sec);
        start = end;
        INFO("Phase 4: join bins took %llu cycles (%f sec)\n", cycles, secs);
#endif
    } while (false);

#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    if (rtn == 0) {
        // ecall_print_table_dbg(eid, &rtn, db_id, *out_tbl_id, 0, 1 << 20);
        INFO("%u rows of joined rows is generated.\n", join_tbl->num_rows.load());
    }
#endif

    // Clean up
    for (const auto &tbl : lhs_bins) {
        if (tbl != nullptr) {
            delete_table(db, tbl);
        }
    }
    lhs_bins.clear();

    for (const auto &tbl : rhs_bins) {
        if (tbl != nullptr) {
            delete_table(db, tbl);
        }
    }
    rhs_bins.clear();
    return rtn;
}

namespace bin_packing_join::hash_bin_packing_join {

int collect_metadata(table_t *table, int column, const size_t rows_per_dblk,
                     int *dblk_count, metadata_t *metadata) {
    INFO("Collection metadata on table %s with %d rows\n", table->name.c_str(),
         table->num_rows.load());
    int original_dblk_cnt = *dblk_count;
    int row_num;
    for (row_num = 0; row_num < table->num_rows; ++(*dblk_count)) {
        std::unordered_map<hash_size_t, int> counter;
        for (int i = 0; i < rows_per_dblk; ++i) {
            if (row_num >= table->num_rows) {
                break;
            }
            row_t row;
            if (read_row(table, row_num++, &row)) {
                ERR("Failed to read row\n");
            }
            char *value = (char *)get_column(&table->sc, column, &row);
            counter[get_hash(value)]++;
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
bool mergeBins(bin_info_t *a, const bin_info_t *b, const int cell_size) {
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
                        std::vector<bin_info_t> *bins) {
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

    std::vector<bin_info_t> res;
    bin_info_t last_bin(dblk_count);

    for (const auto &it : metadata) {
        auto &dblks = it.second.dblks;
        bin_info_t bin(dblk_count);
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
    for (const bin_info_t &bin : res) {
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

int out_bin_info_collection(const std::vector<bin_info_t> &bins, int midpoint,
                            int *num_rows_per_out_bin) {
    if (bins.empty()) {
        ERR("Bin can't be empty");
        return -1;
    }
    *num_rows_per_out_bin = 0;

    for (const bin_info_t &bin : bins) {
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

int fill_bins(data_base_t *db, table_t *data_table, int column,
              const int rows_per_dblk, const std::vector<bin_info_t> &info_bins,
              const int start_dblk, const int end_dblk, const int rows_per_cell,
              std::vector<table_t *> *bins) {
    schema_t *bin_sc = &data_table->sc;
    for (int i = 0; i < bins->size(); ++i) {
        std::string bin_name = "join:bp:bin_" + data_table->name + "_";
        bin_name += std::to_string(i);
        table_t *tmp;
        if (create_table(db, bin_name, bin_sc, &tmp)) {
            ERR("Failed to create table %s\n", bin_name.c_str());
            return -1;
        }
        (*bins)[i] = tmp;
    }

    // Load one datablock of bin information into the memory at a time.
    // For each datablock, data will be stored in temp_bins before we flush them
    // to the actual bins for security measure.
    // TODO: parallelize this in per dblk level.
    int data_row_num = 0;
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
    INFO("Reading bin info table from dblk %d to %d\n", start_dblk, end_dblk);
#endif
    // For each datablock
    for (int dblk_num = start_dblk; dblk_num < end_dblk; ++dblk_num) {
        int rtn =
            fill_bins_per_dblk(data_table, column, &data_row_num, rows_per_dblk,
                               dblk_num, info_bins, rows_per_cell, bins);
        if (rtn) {
            ERR("Failed to fill bin\n");
            return rtn;
        }
    }

    return 0;
}

int fill_bins_per_dblk(table_t *data_table, int column, int *data_row_num,
                       const int rows_per_dblk, const int dblk_num,
                       const std::vector<bin_info_t> &info_bins,
                       const int rows_per_cell,
                       const std::vector<table_t *> *bins) {
                           
    // Load bin information
    // This map stores the information about a value should be placed in
    // which cell.
    const int num_bins = bins->size();
    std::unordered_map<hash_size_t, int> placement;
    // For each cell
    for (int cell_num = 0; cell_num < info_bins.size(); ++cell_num) {
        const auto &[_, info_cell] = info_bins[cell_num][dblk_num];
        UNUSED(_);
        // For each value
        for (const auto &[value, _] : info_cell) {
            UNUSED(_);
            placement[value] = cell_num;
        }
    }

    // Scan the data table and fill bin according to the bin information
    int byte_read_bin = 0;
    typedef std::vector<row_t> temp_bin_t;
    std::vector<temp_bin_t> temp_bins(num_bins);
    for (int i = 0; i < rows_per_dblk; ++i) {
        row_t row;
        if (*data_row_num >= data_table->num_rows) {
            // TODO: make sure that there's no more info_bin to read in the
            // outer loop if (data_table_exhausted) {
            //     ERR("Reach the end of data table but still have more rows "
            //         "to read. Data table: dblk %d, curr_row %d, "
            //         "rows_per_dblk. Info table: total rows %d, curr_row "
            //         "%d %d\n",
            //         dblk_cnt, i, rows_per_dblk, bin_info_table->num_rows,
            //         row_num, 1);
            //     return -1;
            // }
            break;
        }

        if (read_row(data_table, (*data_row_num)++, &row)) {
            ERR("Failed to read row");
            return -1;
        }
        char *value = (char *)get_column(&(data_table->sc), column, &row);
#if !defined(NDEBUG)
        const auto it = placement.find(get_hash(value));
        if (it == placement.end()) {
            ERR("Failed to find bin info\n");
            return -1;
        }
        if (it->second < 0 || it->second >= temp_bins.size()) {
            ERR("Bin index out of bound: %d\n", it->second);
            return -1;
        }
#endif
        const auto &cell_to_be_placed_in = it->second;
        auto &bin = temp_bins[cell_to_be_placed_in];
        try {
            bin.push_back(row);
            byte_read_bin += sizeof(row);
        } catch (const std::bad_alloc &) {
            ERR("Not enough memory: dblk %d, i %d, row_num %d, byte_bin %d\n",
                dblk_num, i, *data_row_num, byte_read_bin);
            return -1;
        }
    }

    // Flush temp_bins to bins
    row_t empty_row;
    memset(&empty_row, 0x0, sizeof(empty_row));
    empty_row.header.fake = true;
    for (int i = 0; i < temp_bins.size(); ++i) {
        temp_bin_t *temp_bin = &temp_bins[i];
        for (int j = 0; j < rows_per_cell; j++) {
            row_t *row;
            if (j < temp_bin->size()) {
                row = &(*temp_bin)[j];
            } else {
                row = &empty_row;
            }
            insert_row_dbg((*bins)[i], row);
        }
    }

    return 0;
}

int join_bins(table_t *lhs_tbl, const int lhs_column, table_t *rhs_tbl,
              const int rhs_column, schema_t *join_sc, table_t *join_tbl,
              const int num_rows_per_out_bin, int bin_id) {
    return bin_packing_join::external_bin_packing_join::join_bins(
        lhs_tbl, lhs_column, rhs_tbl, rhs_column, join_sc, join_tbl,
        num_rows_per_out_bin, bin_id);
}

} // namespace bin_packing_join::hash_bin_packing_join