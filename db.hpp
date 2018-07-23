#pragma once
#include "util.hpp"
#define MAX_COLS 20
#define MAX_TABLES 100
enum SchemaType{
    BOOLEAN,
    BINARY,
    VARBINARY,
    DECIMAL,
    CHARACTER,
    VARCHAR,


};
struct Column{
    SchemaType ty;
    union{
        struct {
            u64 integral, decimal;
        } dec;
        struct {
            u64 len;
        } buf;
    };
};
struct Schema{
    Column schema[MAX_COLS]; 
    u64 lenRow;
};
struct Table{
    str name;
    str    colNames[MAX_COLS];
    Schema sc;
};

struct DataBlock{
    void * data;
};