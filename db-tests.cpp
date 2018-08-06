#include "db-tests.hpp"
#include "db.hpp"
#include <string>
#include <string.h>
#include <fstream>
#include <sstream>

int test_rankings() {
 	
	schema_t sc;
	std::string db_name("rankings");
	std::string table_name("rankings");
	int db_id, table_id, ret; 
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
		printf("%s:create db error:%d\n", __func__, ret);
		return ret; 
	}

	ret = ecall_create_table(db_id, table_name.c_str(), table_name.length(), &sc, &table_id);
	if (ret) {
		printf("%s:create table error:%d\n", __func__, ret);
		return ret; 
	}

	std::ifstream file("rankings.csv");

	row[0] = 'a';
	for(int i = 0; i < 360000; i++) { 

		memset(row, 'a', MAX_ROW_SIZE);
		file.getline(line, MAX_ROW_SIZE); //get the field

		std::istringstream ss(line);
		for(int j = 1; j < sc.num_fields; j++) {
			if(!ss.getline(data, MAX_ROW_SIZE, ',')){
				break;
			}
			//printf("data: %s\n", data);
			if(sc.types[j] == INTEGER) {
				int d = 0;
				d = atoi(data);
				//printf("data: %s\n", data);
				//printf("d %d\n", d);
				memcpy(&row[sc.offsets[j]], &d, 4);
			} else if (sc.types[j] == TINYTEXT) {
				strncpy((char*)&row[sc.offsets[j]], data, strlen(data) + 1);
			}
		}
	
		ret = ecall_insert_row_dbg(db_id, table_id, row);
		if (ret) {
			printf("%s:insert row:%d, err:%d\n", __func__, i, ret);
			return ret; 
		}
		
	
	}
	printf("created rankings table\n");


/* Lets create one table first... */
#if 0
	Schema userdataSchema;
	userdataSchema.numFields = 10;
	userdataSchema.fieldOffsets[0] = 0;
	userdataSchema.fieldSizes[0] = 1;
	userdataSchema.fieldTypes[0] = CHAR;
	userdataSchema.fieldOffsets[1] = 1;
	userdataSchema.fieldSizes[1] = 255;
	userdataSchema.fieldTypes[1] = TINYTEXT;
	userdataSchema.fieldOffsets[2] = 256;
	userdataSchema.fieldSizes[2] = 255;
	userdataSchema.fieldTypes[2] = TINYTEXT;
	userdataSchema.fieldOffsets[3] = 511;
	userdataSchema.fieldSizes[3] = 4;
	userdataSchema.fieldTypes[3] = INTEGER;
	userdataSchema.fieldOffsets[4] = 515;
	userdataSchema.fieldSizes[4] = 4;
	userdataSchema.fieldTypes[4] = INTEGER;
	userdataSchema.fieldOffsets[5] = 519;
	userdataSchema.fieldSizes[5] = 255;
	userdataSchema.fieldTypes[5] = TINYTEXT;
	userdataSchema.fieldOffsets[6] = 774;
	userdataSchema.fieldSizes[6] = 255;
	userdataSchema.fieldTypes[6] = TINYTEXT;
	userdataSchema.fieldOffsets[7] = 1029;
	userdataSchema.fieldSizes[7] = 255;
	userdataSchema.fieldTypes[7] = TINYTEXT;
	userdataSchema.fieldOffsets[8] = 1284;
	userdataSchema.fieldSizes[8] = 255;
	userdataSchema.fieldTypes[8] = TINYTEXT;
	userdataSchema.fieldOffsets[9] = 1539;
	userdataSchema.fieldSizes[9] = 4;
	userdataSchema.fieldTypes[9] = INTEGER;

	Condition cond;
	cond.numClauses = 0;
	cond.nextCondition = NULL;

	char* tableName2 = "uservisits";
	createTable(enclave_id, (int*)&status, 
			&userdataSchema, 
			tableName2, 
			strlen(tableName2), 
			TYPE_LINEAR_SCAN, 
			350010, &structureId2); //TODO temp really 350010

	std::ifstream file2("uservisits.csv");

	//file.getline(line, BLOCK_DATA_SIZE);//burn first line
	row[0] = 'a';
	for(int i = 0; i < 350000; i++){//TODO temp really 350000
	//for(int i = 0; i < 1000; i++){
		memset(row, 'a', BLOCK_DATA_SIZE);
		file2.getline(line, BLOCK_DATA_SIZE);//get the field

		std::istringstream ss(line);
		for(int j = 0; j < 9; j++){
			if(!ss.getline(data, BLOCK_DATA_SIZE, ',')){
				break;
			}
			//printf("data: %s\n", data);
			if(j == 2 || j == 3 || j == 8){//integer
				int d = 0;
				if(j==3) d = atof(data)*100;
				else d = atoi(data);
				//printf("data: %s\n", data);
				//printf("d %d ", d);
				memcpy(&row[userdataSchema.fieldOffsets[j+1]], &d, 4);
			}
			else{//tinytext
				strncpy((char*)&row[userdataSchema.fieldOffsets[j+1]], data, strlen(data)+1);
			}
		}
		//manually insert into the linear scan structure for speed purposes
		opOneLinearScanBlock(enclave_id, (int*)&status, structureId2, i, (Linear_Scan_Block*)row, 1);
		incrementNumRows(enclave_id, (int*)&status, structureId2);
	}

	printf("created uservisits table\n");
	time_t startTime, endTime;
	double elapsedTime;

	Condition cond1, cond2;
	int l = 19800100, h = 19800402;
	cond1.numClauses = 1;
	cond1.fieldNums[0] = 3;
	cond1.conditionType[0] = 1;
	cond1.values[0] = (uint8_t*)malloc(4);
	memcpy(cond1.values[0], &l, 4);
	cond1.nextCondition = &cond2;
	cond2.numClauses = 1;
	cond2.fieldNums[0] = 3;
	cond2.conditionType[0] = 2;
	cond2.values[0] = (uint8_t*)malloc(4);
	memcpy(cond2.values[0], &h, 4);
	cond2.nextCondition = NULL;
	Condition noCond;
	noCond.numClauses = 0;
	noCond.nextCondition = NULL;

	startTime = clock();
	if(baseline == 1){
		selectRows(enclave_id, (int*)&status, "uservisits", -1, cond1, -1, -1, 5, 0);
		renameTable(enclave_id, (int*)&status, "ReturnTable", "uvJ");
		//printTable(enclave_id, (int*)&status, "uvJ");
		joinTables(enclave_id, (int*)&status, "uvJ", "rankings",  2, 1, -1, -1);
		//int joinTables(char* tableName1, char* tableName2, int joinCol1, int joinCol2, int startKey, int endKey) {//put the smaller table first for
		renameTable(enclave_id, (int*)&status, "JoinReturn", "jr");
		//printTable(enclave_id, (int*)&status, "jr");
		selectRows(enclave_id, (int*)&status, "jr", 10, noCond, 4, 1, 4, 2);
		renameTable(enclave_id, (int*)&status, "ReturnTable", "last");
		//printTable(enclave_id, (int*)&status, "last");
		selectRows(enclave_id, (int*)&status, "last", 2, noCond, 3, -1, 0, 0);
	}
	else{
		selectRows(enclave_id, (int*)&status, "uservisits", -1, cond1, -1, -1, 2, 0);
		//indexSelect(enclave_id, (int*)&status, "uservisits", -1, cond1, -1, -1, 2, l, h);
		renameTable(enclave_id, (int*)&status, "ReturnTable", "uvJ");
		//printTable(enclave_id, (int*)&status, "uvJ");
		joinTables(enclave_id, (int*)&status, "uvJ", "rankings",  2, 1, -1, -1);
		//int joinTables(char* tableName1, char* tableName2, int joinCol1, int joinCol2, int startKey, int endKey) {//put the smaller table first for
		renameTable(enclave_id, (int*)&status, "JoinReturn", "jr");
		//printTable(enclave_id, (int*)&status, "jr");
		selectRows(enclave_id, (int*)&status, "jr", 10, noCond, 4, 1, 4, 0);
		renameTable(enclave_id, (int*)&status, "ReturnTable", "last");
		//printTable(enclave_id, (int*)&status, "last");
		selectRows(enclave_id, (int*)&status, "last", 2, noCond, 3, -1, -1, 0);
		//select from index
		//join
		//fancy group by
		//select max
		//char* tableName, int colChoice, Condition c, int aggregate, int groupCol, int algChoice
	}
	endTime = clock();
	elapsedTime = (double)(endTime - startTime)/(CLOCKS_PER_SEC);
	printf("BDB3 running time: %.5f\n", elapsedTime);
	//printTable(enclave_id, (int*)&status, "ReturnTable");
	deleteTable(enclave_id, (int*)&status, "ReturnTable");

	deleteTable(enclave_id, (int*)&status, "uservisits");
#endif
	//deleteTable(enclave_id, (int*)&status, "rankings");
	return 0;

}


