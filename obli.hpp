#pragma once
#include "util.hpp"

s64 obli_cmp64(s64 a, s64 b){
    s64 out = 0;
    u64 temp = 0;
    asm volatile("cmp %3, %2\n\t"
                 "setg %b0\n\t"
                 "setl %b1\n\t"
                 "subq %1,%0":"+r"(out),"+r"(temp): "r"(a), "r"(b):"cc");
    return out;
}
s64 obli_cmp32(s32 a, s32 b){
    s64 out = 0;
    u64 temp = 0;
    asm volatile("cmp %3, %2\n\t"
                 "setg %b0\n\t"
                 "setl %b1\n\t"
                 "subq %1,%0":"+r"(out),"+r"(temp): "r"(a), "r"(b):"cc");
    return out;
}
s64 obli_cmp16(s16 a, s16 b){
    s64 out = 0;
    u64 temp = 0;
    asm volatile("cmp %3, %2\n\t"
                 "setg %b0\n\t"
                 "setl %b1\n\t"
                 "subq %1,%0":"+r"(out),"+r"(temp): "r"(a), "r"(b):"cc");
    return out;
}
s64 obli_cmp8(s8 a, s8 b){
    s64 out = 0;
    u64 temp = 0;
    asm volatile("cmp %3, %2\n\t"
                 "setg %b0\n\t"
                 "setl %b1\n\t"
                 "subq %1,%0":"+r"(out),"+r"(temp): "r"(a), "r"(b):"cc");
    return out;
}

void obli_cswap64(u64 * i1, u64 * i2, s64 cond, s64 cmp){
    u64 temp = 0;
        asm volatile("xor %4, %3\n\t"
                     "cmovz %0, %2\n\t"
                     "cmovz %1, %0\n\t"
                     "cmovz %2, %1":"+r"(i1[0]), "+r"(i2[0]), "+r"(temp), "+r"(cond): "r"(cmp):"cc");
}
void obli_cswap32(u32 * i1, u32 * i2, s64 cond, s64 cmp){
    u32 temp = 0;
        asm volatile("xor %4, %3\n\t"
                     "cmovz %0, %2\n\t"
                     "cmovz %1, %0\n\t"
                     "cmovz %2, %1":"+r"(i1[0]), "+r"(i2[0]), "+r"(temp), "+r"(cond): "r"(cmp):"cc");
}
void obli_cswap16(u16 * i1, u16 * i2, s64 cond, s64 cmp){
    u16 temp = 0;
        asm volatile("xor %4, %3\n\t"
                     "cmovz %0, %2\n\t"
                     "cmovz %1, %0\n\t"
                     "cmovz %2, %1":"+r"(i1[0]), "+r"(i2[0]), "+r"(temp), "+r"(cond): "r"(cmp):"cc");
}
void obli_cswap8(u8 * i1, u8 * i2, s64 cond, s64 cmp){
    u64 c1 = *i1, c2 = *i2;
    obli_cswap64(&c1, &c2, cond, cmp);
    *i1 = (u8) c1;
    *i2 = (u8) c2;
}
void obli_cswap(u8 * i1, u8 * i2, u64 len, s64 cond, s64 cmp){
    for(u64 i =0;i<len;i++){
        obli_cswap8(&i1[i], &i2[i], cond, cmp);
    }
}
u64 obli_cmov64(u64 i1, u64 i2,s64 cond, s64 cmp){
    asm volatile("xor %3, %1\n\t"
                 "cmovz %2,%0":"+r"(i1), "+r"(cond): "r"(i2), "r"(cmp):"cc");
    return i1;

}
u32 obli_cmov32(u32 i1, u32 i2,s64 cond, s64 cmp){

    asm volatile("xor %3, %1\n\t"
                 "cmovz %2,%0":"+r"(i1), "+r"(cond): "r"(i2), "r"(cmp):"cc");
    return i1;
}
u16 obli_cmov16(u16 i1, u16 i2,s64 cond, s64 cmp){
    asm volatile("xor %3, %1\n\t"
                 "cmovz %2,%0":"+r"(i1), "+r"(cond): "r"(i2), "r"(cmp):"cc");
    return i1;
}
u8 obli_cmov8(u8 i1, u8 i2,s64 cond, s64 cmp){
    return (u8)obli_cmov64(i1, i2, cond, cmp);
}
s64 obli_strcmp(u8 * a, u8 * b, u64 l){
    s64 out = 0;
    for(auto i = 0;i<l;i++){
        s64 c = obli_cmp8(a[i], b[i]);
        out = obli_cmov64((u64)out,(u64)c,out,0);
        
    }
    return out;
}
s64 obli_varcmp(u8 * a, u8 * b, u64 l, u64 lim){
    s64 out = 0;
    for(u64 i = 0;i<lim;i++){
        s64 c = obli_cmp8(a[i], b[i]);
        
        u64 temp = obli_cmov64((u64)out,(u64)c,obli_cmp64(i, l),-1);
        out = obli_cmov64((u64)out,(u64)temp,out,0);
        
    }
    return out;
}
