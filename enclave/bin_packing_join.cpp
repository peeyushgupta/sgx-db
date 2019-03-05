#include "bin_packing_join.hpp"

#include <algorithm>
#include <cerrno>
#include <unordered_map>
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

    // Schema for each temp table we create for metadata
    // It currently only support one joining attrivute
    // <attribute_1, attribut_2, ... , attribute_n, count>
    schema_t *joining_sc = &(db->tables[left_table_id]->sc);
    const int column = join_cond->fields_left[0];
    const int joining_attr_size = joining_sc->sizes[column];
    schema_t metadata_schema;
    metadata_schema.num_fields = 2;
    metadata_schema.offsets[0] = 0;
    metadata_schema.sizes[0] = joining_attr_size;
    metadata_schema.types[0] = joining_sc->types[column];
    metadata_schema.offsets[1] = joining_attr_size;
    metadata_schema.sizes[1] = 4;
    metadata_schema.types[1] = INTEGER;
    metadata_schema.row_data_size =
        metadata_schema.offsets[metadata_schema.num_fields - 1] +
        metadata_schema.sizes[metadata_schema.num_fields - 1];

    // Each table stores the metadata
    std::vector<table_t *> metadatas_left;
    std::vector<table_t *> metadatas_right;

    do {
#if defined(REPORT_BIN_PACKING_JOIN_STATS)
        unsigned long long start, end;
        unsigned long long cycles;
        double secs;
        start = RDTSC();
#endif

        // Number of occurance of each joining attribute across all datablocks
        std::unordered_map<std::string, int> total_occurances;

        // if (collect_metadata(db_id, join_cond->table_left,
        //                      join_cond->fields_left[0], metadata_schema,
        //                      &total_occurances, &metadatas_left)) {
        //     ERR("Failed to collect metadata");
        //     rtn = -1;
        //     break;
        // }

        if (collect_metadata(db_id, join_cond->table_right,
                             join_cond->fields_right[0], metadata_schema,
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
#endif

        std::vector<std::vector<std::string>> bins;
        if (pack_bins(&total_occurances, metadatas_right, &bins,
                      metadata_schema)) {
            ERR("Failed to pack bin\n");
            rtn = -1;
            break;
        }

    } while (0);

    // Clean up
    for (auto &table : metadatas_left) {
        delete_table(db, table);
    }
    for (auto &table : metadatas_right) {
        delete_table(db, table);
    }

    return rtn;
}

int collect_metadata(int db_id, int table_id, int column,
                     schema_t metadata_schema,
                     std::unordered_map<std::string, int> *total_occurances,
                     std::vector<table_t *> *metadatas) {

    // Determine size of data blocks
#ifdef MAX_HEAP_SIZE
    const size_t max_heap_size = MAX_HEAP_SIZE;
#else
    const size_t max_heap_size = 7E7;
#endif
    const size_t dblk_size = max_heap_size / 4;

    // Check params
    data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    table_t *table = db->tables[table_id];
    const int rows_per_dblk = dblk_size / row_size(table);
    schema_t *schema = &(table->sc);

    int row_num = 0;
    // Due to memory limitation, we only do half of the table
    // TODO(tianjiao): increase to full size
    while (row_num < table->num_rows / 2) {
        std::unordered_map<std::string, int> counter;
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
            counter[val]++;
            (*total_occurances)[val]++;
        }
        // Create temp table: bin_packing_join_metadata_collection_<i>
        table_t *tmp_table;
        std::string tmp_table_name =
            "bin_packing_join_mdc_" + std::to_string(metadatas->size());
        RETURN_IF_FAILED(
            create_table(db, tmp_table_name, &metadata_schema, &tmp_table));
        metadatas->push_back(tmp_table);

        std::vector<std::pair<std::string, int>> sorted_counter;
        for (const auto &it : counter) {
            sorted_counter.emplace_back(std::move(it));
        }
        std::sort(
            sorted_counter.begin(), sorted_counter.end(),
            [](const auto &a, const auto &b) { return a.second > b.second; });

        int i = 0;
        row_t row;
        row.header.fake = false;
        row.header.from = tmp_table->id;
        for (const auto &it : sorted_counter) {
            // Populate row
            write_column(&metadata_schema, 0, &row, it.first.c_str());
            write_column(&metadata_schema, 1, &row, &it.second);
            insert_row_dbg(tmp_table, &row);
        }
    }
    return 0;
}

int pack_bins(std::unordered_map<std::string, int> *total_occurances,
              const std::vector<table_t *> &metadatas,
              std::vector<std::vector<std::string>> *bins,
              schema_t metadata_schema) {
    std::unordered_map<std::string, int> last_seen;
    for (auto &metadata : metadatas) {
        std::unordered_map<std::string, int> curr_seen;
        row_t row;
        for (int row_num = 0; row_num < metadata->num_rows; row_num++) {
            // Get metadata
            RETURN_IF_FAILED(read_row(metadata, row_num, &row));
            std::string val;
            if (metadata_schema.types[0] != TINYTEXT) {
                val = std::string((char *)get_column(&metadata_schema, 0, &row),
                                  metadata_schema.sizes[0]);
            } else {
                val =
                    std::string((char *)get_column(&metadata_schema, 0, &row));
            }
            int count = *(int *)get_column(&metadata_schema, 1, &row);
            total_occurances->at(val) -= count;
            if (total_occurances->at(val) < 0) {
                ERR("Total occurance %d of %s is smaller than its current "
                    "occurance %d\n",
                    (*total_occurances)[val] + count, val, count);
                return -1;
            }
            if (total_occurances->at(val) == 0) {
                total_occurances->erase(val);
            }

            // Fit it into the cell
            // Metadata is pre-sorted
        }

        last_seen.swap(curr_seen);
    }

    if (!total_occurances->empty()) {
        ERR("There are %d joining attributes that are not packed into a bin\n",
            total_occurances->size());
        return -1;
    }

    return 0;
}
