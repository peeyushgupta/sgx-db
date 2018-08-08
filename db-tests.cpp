#include "db-tests.hpp"
#include "db.hpp"
#include <string>
#include <string.h>
#include <fstream>
#include <sstream>

int test_rankings() {
 	
	schema_t sc, sc_udata;
	std::string db_name("rankings-and-udata");
	std::string table_name("rankings");
	std::string udata_table_name("udata");
	int db_id, table_id, udata_table_id, join_table_id, ret; 
	join_condition_t c;
	char line[MAX_ROW_SIZE]; 
	char data[MAX_ROW_SIZE];
	uint8_t *row;

	sc.num_fields = 4;
	sc.offsets[0] = 0;
	sc.sizes[0] = 1;
	sc.types[0] = CHARACTER;
	sc.offsets[1] = 1;
	sc.sizes[1] = 255;
	sc.types[1] = TINYTEXT;
	sc.offsets[2] = 256;
	sc.sizes[2] = 4;
	sc.types[2] = INTEGER;
	sc.offsets[3] = 260;
	sc.sizes[3] = 4;
	sc.types[3] = INTEGER;
	sc.row_size = sc.offsets[sc.num_fields - 1] + sc.sizes[sc.num_fields - 1];

	//row = (uint8_t*)malloc(sc.row_size);
	row = (uint8_t*)malloc(MAX_ROW_SIZE);

	ret = ecall_create_db(db_name.c_str(), db_name.length(), &db_id);
	if (ret) {
		ERR("create db error:%d\n", ret);
		return ret; 
	}

	ret = ecall_create_table(db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (ret) {
		ERR("create table error:%d\n", ret);
		return ret; 
	}

	std::ifstream file("rankings.csv");

	row[0] = 'a';
	//for(int i = 0; i < 360000; i++) { 
	for(int i = 0; i < 100; i++) { 

		memset(row, 'a', MAX_ROW_SIZE);
		file.getline(line, MAX_ROW_SIZE); //get the field

		std::istringstream ss(line);
		for(int j = 1; j < sc.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')) {
				ERR("something is wrong with data (skipping):%s\n", line);
				break;
			}
			if(sc.types[j] == INTEGER) {
				int d = 0;
				d = atoi(data);
				memcpy(&row[sc.offsets[j]], &d, 4);
			} else if (sc.types[j] == TINYTEXT) {
				strncpy((char*)&row[sc.offsets[j]], data, strlen(data) + 1);
			}
		}

		DBG("insert row:%s\n", (char*)row); 
	
		ret = ecall_insert_row_dbg(db_id, table_id, row);
		if (ret) {
			ERR("insert row:%d, err:%d\n", i, ret);
			return ret; 
		}
		
	
	}

	printf("created rankings table\n");

	sc_udata.num_fields = 10;
	sc_udata.offsets[0] = 0;
	sc_udata.sizes[0] = 1;
	sc_udata.types[0] = CHARACTER;
	sc_udata.offsets[1] = 1;
	sc_udata.sizes[1] = 255;
	sc_udata.types[1] = TINYTEXT;
	sc_udata.offsets[2] = 256;
	sc_udata.sizes[2] = 255;
	sc_udata.types[2] = TINYTEXT;
	sc_udata.offsets[3] = 511;
	sc_udata.sizes[3] = 4;
	sc_udata.types[3] = INTEGER;
	sc_udata.offsets[4] = 515;
	sc_udata.sizes[4] = 4;
	sc_udata.types[4] = INTEGER;
	sc_udata.offsets[5] = 519;
	sc_udata.sizes[5] = 255;
	sc_udata.types[5] = TINYTEXT;
	sc_udata.offsets[6] = 774;
	sc_udata.sizes[6] = 255;
	sc_udata.types[6] = TINYTEXT;
	sc_udata.offsets[7] = 1029;
	sc_udata.sizes[7] = 255;
	sc_udata.types[7] = TINYTEXT;
	sc_udata.offsets[8] = 1284;
	sc_udata.sizes[8] = 255;
	sc_udata.types[8] = TINYTEXT;
	sc_udata.offsets[9] = 1539;
	sc_udata.sizes[9] = 4;
	sc_udata.types[9] = INTEGER;

	sc_udata.row_size = sc_udata.offsets[sc_udata.num_fields - 1] + sc_udata.sizes[sc_udata.num_fields - 1];

	ret = ecall_create_table(db_id, udata_table_name.c_str(), udata_table_name.length(), &sc_udata, &udata_table_id);
	if (ret) {
		ERR("create table error:%d, table:%s\n", ret, udata_table_name.c_str());
		return ret; 
	}

	std::ifstream file2("uservisits.csv");

	row[0] = 'a';
	//for(int i = 0; i < 350000; i++){//TODO temp really 350000
	for(int i = 0; i < 100; i++){//TODO temp really 350000
	
		memset(row, 'a', MAX_ROW_SIZE);
		file2.getline(line, MAX_ROW_SIZE);//get the field

		std::istringstream ss(line);

		for(int j = 1; j < sc_udata.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')){
				break;
			}
			if(sc_udata.types[j] == INTEGER) {
				int d = 0;
				d = atoi(data);
				memcpy(&row[sc_udata.offsets[j]], &d, 4);
			} else if (sc_udata.types[j] == TINYTEXT) {
				strncpy((char*)&row[sc_udata.offsets[j]], data, strlen(data) + 1);
			}
		}

		ret = ecall_insert_row_dbg(db_id, udata_table_id, row);
		if (ret) {
			ERR("insert row:%d into %s, err:%d\n", 
				i, udata_table_name.c_str(), ret);
			return ret; 
		}

	}

	printf("created uservisits table\n");

	c.num_conditions = 1; 
	c.table_left = table_id; 
	c.table_right = udata_table_id; 
	c.fields_left[0] = 1;
	c.fields_right[0] = 2;

	ret = ecall_join(db_id, &c, &join_table_id);
	if (ret) {
		ERR("join failed, err:%d\n", ret);
		return ret; 
	}

	printf("joined successfully\n");

	return 0;

}


