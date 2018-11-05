#pragma once
#include "util.hpp"

/* AB: cmp is oblivious operation by default, no? */

#if 0 

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

#endif 

// check if a type is 8-bit (cmovXX works only with 16, 32, 64-bit types)
template <class T>
constexpr bool is_8bit = std::is_same<T, char>::value | std::is_same<T, unsigned char>::value;

// oblivious swap
// if (cond)
// 	src and dst are swapped
// else
// 	nop
template <typename T>
void obli_cswap_t(T *src, T *dst, bool cond) {
    T temp;
    if (is_8bit<T>) {
	u64 c1 = *src, c2 = *dst;
	obli_cswap_t(&c1, &c2, cond);
	*src = (u8) c1;
	*dst = (u8) c2;
	return;
    }

    asm volatile("test %[cond], %[cond]\n\t"
                 "cmovnz %[src], %[temp]\n\t"
                 "cmovnz %[dst], %[src]\n\t"
                 "cmovnz %[temp], %[dst]"  : [src]"+r"(src[0]), [dst]"+r"(dst[0]), [temp]"=r"(temp) : [cond]"r"(cond) : "cc");
    return;
}

void obli_cswap(u8 * src, u8 * dst, u64 len, s64 cond){
    u64 i =0;
    for( ; i < len - 7; i+=8) {
        obli_cswap_t(((u64*)(&src[i])), ((u64*)(&dst[i])), cond);
    }
    for( ; i < len - 3; i+=4) {
        obli_cswap_t(((u32*)(&src[i])), ((u32*)(&dst[i])), cond);
    }
    for( ; i < len - 1; i+=2) {
        obli_cswap_t(((u16*)(&src[i])), ((u16*)(&dst[i])), cond);
    }
    for( ; i < len; i++) {
       obli_cswap_t(((u8*)(&src[i])), ((u8*)(&dst[i])), cond);
    }
}

// oblivious move
// if (cond)
// 	src = dst;
// 	return dst;
// else
// 	return src;
template <typename T>
T obli_cmove_t(T src, T dst, bool cond) {
    T temp;
    if (is_8bit<T>) {
	return static_cast<T>(obli_cmove_t((u64) src, (u64) dst, cond));
    }

    asm volatile("test %[cond], %[cond]\n\t"
                 "cmovnz %[dst], %[src]" : [src]"+r"(src) : [cond]"r"(cond), [dst]"r"(dst) : "cc");

    return src;
}

void obli_cmove(u8* src, u8* dst, u64 len, bool cond){
    u64 i = 0;

    for(;i < len - 7; i += 8) {
        *((u64*)(&src[i]))= obli_cmove_t(*((u64*)(&src[i])), *((u64*)(&dst[i])), cond);
    }

    for(;i < len - 3; i += 4) {
        *((u32*)(&src[i])) = obli_cmove_t(*((u32*)(&src[i])), *((u32*)(&dst[i])), cond);
    }

    for(;i < len - 1; i += 2){
        *((u16*)(&src[i])) = obli_cmove_t(*((u16*)(&src[i])), *((u16*)(&dst[i])), cond);
    }
    
    for(;i < len; i++){
        *((u8*)(&src[i])) = obli_cmove_t(*((u8*)(&src[i])), *((u8*)(&dst[i])), cond);
    }
} 

/* String comparison */
s64 obli_memcmp(u8 * a, u8 * b, u64 l){
    s64 out = 0;
    for(u64 i = 0; i < l; i++){
        s64 c = (a[i] == b[i]);
        out = obli_cmove_t((u64)out, (u64)c, out);
    }
    return out;
}

/* AB: Do we need it? */
#if 0
s64 obli_varcmp(u8 * a, u8 * b, u64 l, u64 lim){
    s64 out = 0;
    for(u64 i = 0;i<lim;i++){
        s64 c = (a[i] == b[i]);
        
        u64 temp = obli_cmov64((u64)out,(u64)c,obli_cmp64(i, l),-1);
        out = obli_cmov64((u64)out,(u64)temp,out,0);
        
    }
    return out;
}

#endif
