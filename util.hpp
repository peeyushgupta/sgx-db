#pragma once
using u64 = unsigned long long;
using u32 = unsigned int;
using u16 = unsigned short;
using u8  = unsigned char;
using s64 = long long;
using s32 = int;
using s16 = short;
using s8  = char;
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