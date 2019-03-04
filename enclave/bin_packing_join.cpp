#include "bin_packing_join.hpp"

#include <cerrno>
#include <unordered_map>
#include <vector>
#include <map>

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
    const int joining_attr_size = joining_sc->sizes[join_cond->fields_left[0]];
    schema_t metadata_schema;
    metadata_schema.num_fields = 2;
    metadata_schema.offsets[0] = 0;
    metadata_schema.sizes[0] = joining_attr_size;
    metadata_schema.types[0] = joining_sc->types[join_cond->fields_left[0]];
    metadata_schema.offsets[1] = joining_attr_size;
    metadata_schema.sizes[1] = 4;
    metadata_schema.types[1] = INTEGER;

    // Each table stores the metadata
    std::vector<table_t *> metadatas;

    do {
        // Number of occurance of each joining attribute across all datablocks
        std::unordered_map<std::string, int> total_occurances;

        if (collect_metadata(db_id, join_cond, &total_occurances, &metadatas, metadata_schema)) {
            ERR("Failed to collect metadata");
            rtn = -1;
            break;
        }

        std::vector<std::vector<std::string>> bins;
        if (pack_bins(&total_occurances, metadatas, &bins, metadata_schema)) {
            ERR("Failed to pack bin");
            rtn = -1;
            break;
        }

    } while (0);

    // Clean up
    for (auto &table : metadatas) {
        delete_table(db, table);
    }

    return rtn;
}

int collect_metadata(int db_id, join_condition *join_cond,
                     std::unordered_map<std::string, int> *total_occurances,
                     std::vector<table_t *> *metadatas,
                     schema_t metadata_schema) {

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
    
    // Get meatadata from the left table
    {
        const int table_id = join_cond->table_left;
        table_t *table = db->tables[table_id];
        const int rows_per_dblk = dblk_size / row_size(table);
        const int column = join_cond->fields_left[0];
        schema_t *schema = &(table->sc);

        int row_num = 0;
        // Due to memory limitation, we only do half of the table
        // TODO(tianjiao): increase to full size
        while (row_num < table->num_rows / 2) {
            std::map<std::string, int> counter;
            for (int i = 0; i < rows_per_dblk; ++i) {
                row_t row;
                RETURN_IF_FAILED(read_row(table, row_num++, &row));

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
        }
    }
    // Get meatadata from the right table
    {
        const int table_id = join_cond->table_right;
        table_t *table = db->tables[table_id];
        const int rows_per_dblk = dblk_size / row_size(table);
        const int column = join_cond->fields_right[0];
        schema_t *schema = &(table->sc);

        int row_num = 0;
        // Due to memory limitation, we only do half of the table
        // TODO(tianjiao): increase to full size
        while (row_num < table->num_rows / 2) {
            std::map<std::string, int> counter;
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
            // std::map is sorted, so we just insert it
            int i = 0;
            row_t row;
            row.header.fake = false;
	        row.header.from = tmp_table->id;
            for (const auto& it : counter) {
                // Populate row
                if(metadata_schema.types[0] == TINYTEXT) {
		            strncpy((char*)&(row.data), it.first.c_str(), metadata_schema.sizes[0]);
		
                } else if (metadata_schema.types[0] == TINYTEXT) {
                    memcpy(&(row.data), it.first.c_str(), metadata_schema.sizes[0]);
                }
                memcpy(&row.data[metadata_schema.offsets[0]], &(it.second), 4);
                write_row_dbg(tmp_table, &row, i++);
            }
            print_table_dbg(tmp_table, 0, 10);

        }
    }
    return 0;
}

int pack_bins(std::unordered_map<std::string, int> *total_occurances,
             const std::vector<table_t *> &metadatas,
             std::vector<std::vector<std::string>> *bins,
             schema_t metadata_schema) {
    std::unordered_map<std::string, int> last_seen;
    for (auto& metadata : metadatas) {
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
                val = std::string((char *)get_column(&metadata_schema, 0, &row));
            }
            int count = *(int*)get_column(&metadata_schema, 1, &row);
            (*total_occurances)[val] -= count;
            if ((*total_occurances)[val] < 0) {
                ERR("Total occurance is smaller than the sum of all occurances");
            }

            // Fit it into the cell
            // Metadata is pre-sorted
        }

        last_seen.swap(curr_seen);
    }
    
    return 0;
}
