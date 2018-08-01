#pragma once
#include <cstdint>

using u64 = uint64_t;
using u32 = uint32_t;
using u16 = uint16_t;
using u8  = uint8_t;
using s64 = int64_t;
using s32 = int32_t;
using s16 = int16_t;
using s8 = int8_t;

template <typename T>
struct buf{
    T * data;
    u64 len;
};
template <typename T, u64 n>
struct arr{
    T arr[n];
};
using str = buf<char>;
