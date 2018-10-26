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

/* Conditional swap */
void obli_cswap64(u64 * src, u64 * dst, bool cond){
    u64 temp;
    asm volatile("test %3, %3\n\t"
                 "cmovz %0, %2\n\t"
                 "cmovz %1, %0\n\t"
                 "cmovz %2, %1"  : "+r"(src[0]), "+r"(dst[0]), "=r"(temp) : "r"(cond) : "cc");
    return;
}

void obli_cswap32(u32 * src, u32 * dst, s64 cond){
    u32 temp;
    asm volatile("test  %3, %3\n\t"
                 "cmovz %0, %2\n\t"
                 "cmovz %1, %0\n\t"
                 "cmovz %2, %1" : "+r"(src[0]), "+r"(dst[0]), "=r"(temp) : "r"(cond) : "cc");
}

void obli_cswap16(u16 * src, u16 * dst, s64 cond){
    u16 temp;
    asm volatile("test %3, %3\n\t"
                 "cmovz %0, %2\n\t"
                 "cmovz %1, %0\n\t"
                 "cmovz %2, %1":"+r"(src[0]), "+r"(dst[0]), "=r"(temp) : "r"(cond) : "cc");
}
void obli_cswap8(u8 * src, u8 * dst, s64 cond){
    u64 c1 = *src, c2 = *dst;
    obli_cswap64(&c1, &c2, cond);
    *src = (u8) c1;
    *dst = (u8) c2;
}

void obli_cswap(u8 * src, u8 * dst, u64 len, s64 cond){
    u64 i =0;
    for( ; i < len - 7; i+=8) {
        obli_cswap64(((u64*)(&src[i])), ((u64*)(&dst[i])), cond);
    }
    for( ; i < len - 3; i+=4) {
        obli_cswap32(((u32*)(&src[i])), ((u32*)(&dst[i])), cond);
    }
    for( ; i < len - 1; i+=2) {
        obli_cswap16(((u16*)(&src[i])), ((u16*)(&dst[i])), cond);
    }
    
    for( ; i < len; i++) {
       obli_cswap8(((u8*)(&src[i])), ((u8*)(&dst[i])), cond);
    }
}

/* Conditional mov: moves dst into src if cond is true, returns src */

/* AB: Do we need 32bit and 64 bit versions? after all your 8 bit version simply 
       uses 64bit one? */

u64 obli_cmov64(u64 src, u64 dst, bool cond){
    asm volatile("test %[cond], %[cond]\n\t"
                 "cmovnz %[dst], %[src]" : [src]"+r"(src) : [cond]"r"(cond), [dst]"r"(dst) : "cc");
    return src;
}

u32 obli_cmov32(u32 src, u32 dst, bool cond){

    asm volatile("test %[cond], %[cond]\n\t"
                 "cmovnz %[dst], %[src]" : [src]"=r"(src), [cond]"=r"(cond) : [dst]"r"(dst) : "cc");
    return src;
}

u16 obli_cmov16(u16 src, u16 dst, bool cond){

    asm volatile("test %[cond], %[cond]\n\t"
                 "cmovnz %[dst], %[src]" : [src]"=r"(src), [cond]"=r"(cond) : [dst]"r"(dst) : "cc");
    return src;
}

u8 obli_cmov8(u8 src, u8 dst, bool cond) {
    return obli_cmov64((u64)src, (u64)dst, cond);
}


void obli_cmov(u8* src, u8* dst, u64 len, s64 cond){
    u64 i =0;

    for(;i<len-7;i+=8){
        *((u64*)(&src[i]))= obli_cmov64(*((u64*)(&src[i])), *((u64*)(&dst[i])), cond);
    }

    for(;i<len-3;i+=4){
        *((u32*)(&src[i])) = obli_cmov32(*((u32*)(&src[i])), *((u32*)(&dst[i])), cond);
    }

    for(;i<len-1;i+=2){
        *((u16*)(&src[i])) = obli_cmov16(*((u16*)(&src[i])), *((u16*)(&dst[i])), cond);
    }
    
    for(;i<len;i++){
        *((u8*)(&src[i])) = obli_cmov8(*((u8*)(&src[i])), *((u8*)(&dst[i])), cond);
    }
} 

/* String comparison */
s64 obli_memcmp(u8 * a, u8 * b, u64 l){
    s64 out = 0;
    for(u64 i = 0; i < l; i++){
        s64 c = (a[i] == b[i]);
        out = obli_cmov64((u64)out, (u64)c, out);
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
