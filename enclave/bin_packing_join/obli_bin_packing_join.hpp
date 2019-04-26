#pragma once

#include <vector>

#include "db.hpp"

int obli_fill_bins(data_base_t *db, table_t *data_table, int column,
                   const int rows_per_dblk, table_t *bin_info_table,
                   const int start_dblk, const int end_dblk, const int num_bins,
                   const int rows_per_cell, schema_t *bin_sc,
                   std::vector<table_t *> *bins);

int obli_fill_bins_per_dblk(table_t *data_table, int column, int *data_row_num,
                            const int rows_per_dblk, const int dblk_cnt,
                            table_t *bin_info_table, int *info_row_num,
                            const int rows_per_cell, schema_t *bin_sc,
                            std::vector<table_t *> *bins);