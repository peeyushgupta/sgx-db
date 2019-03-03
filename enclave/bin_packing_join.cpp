#include "bin_packing_join.hpp"

#include <vector>
#include <unordered_map>
#include <cerrno>

#include "db.hpp"
#include "util.hpp"
#include "dbg.hpp"
#include "time.hpp"
#include "sort_helper.hpp"

#if defined(NO_SGX)
#include "env.hpp"
#else
#include "enclave_t.h"
#endif

#define RETURN_IF_FAILED(x) if (x) return x;


// Assuming only one joining column
int ecall_bin_packing_join(int db_id, join_condition_t *join_cond, int *join_tbl_id) {
    DBG("wats\n");
    std::vector<std::unordered_map<std::string, int>> metadata;
    
    std::unordered_map<std::string, int> counter;
    auto ptr = &metadata;
    ptr->push_back(counter);
    if (collect_metadata(db_id, join_cond, &metadata)) {
        ERR("bab");
        return -1;
    }
    return 0;
}


int collect_metadata(int db_id, join_condition* join_cond, std::vector<std::unordered_map<std::string, int>>* metadata) {
    // Determine size of data blocks
#ifdef MAX_HEAP_SIZE
    const size_t dblk_size = MAX_HEAP_SIZE / 3;
#else
// TODO(tianjiao): make this bigger
    const size_t dblk_size = 4E7;
#endif

	const data_base_t *db = get_db(db_id);
    if (!db) {
        return -1;
    }

    const int left_table_id = join_cond->table_left;
	if ((left_table_id > (MAX_TABLES - 1)) || !db->tables[left_table_id]) {
        return -2; 
    }
	

    {
        table_t *table = db->tables[left_table_id];
        const int rows_per_dblk = dblk_size / row_size(table);
        const int column = *join_cond->fields_left;
        schema_t* schema = &(table->sc);
        

        int row_num = 0;
        while (row_num < table->num_rows) {
            std::unordered_map<std::string, int> counter;
            for (int i = 0; i < rows_per_dblk; ++i) {
                row_t row;
                if (read_row(table, row_num++, &row)) {
                    return -3;
                }
                        
                std::string val;
                if (schema->types[column] != TINYTEXT) {
                    val = std::string((char*)get_column(schema, column, &row), schema->sizes[column]);
                } else {
                    val = std::string((char*)get_column(schema, column, &row));
                }
                counter[val]++;
                
            }
            DBG("%d\n", counter.size());
            
            metadata->push_back(std::move(counter));
            
        }
        
    }
    return 0;
}
