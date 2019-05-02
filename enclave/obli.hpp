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
                 "cmovnz %[temp], %[dst]"  : [src]"+r"(src[0]), [dst]"+r"(dst[0]), [temp]"=r"(temp) : [cond]"r"(cond) : "cc", "memory");
    return;
}

void obli_cswap_128(double *src, double *dst, bool cond)
{
	// set high bit and broadcast - to be used as mask
	// refer wiki on how double is represented in 64-bits
	__m128d _mask = _mm_set1_pd((double)(-(int)cond));
	// load src
	__m128d s = _mm_load_pd(src);
	// load dst
	__m128d d = _mm_load_pd(dst);
	__m128d temp;

	// blend registers
	// t = cond ? src : dst;
	// src = cond ? dst : src;
	// dst = cond ? temp : dst;
	temp = _mm_blendv_pd(d, s, _mask);
	s = _mm_blendv_pd(s, d, _mask);
	d = _mm_blendv_pd(d, temp, _mask);
	// write back both src and dst
	_mm_store_pd(src, s);
	_mm_store_pd(dst, d);
}

//unsigned long counter_cswap = 0; 
#ifdef AVX2
//#warning "Compiling with AVX2"
void obli_cswap_256(double *src, double *dst, bool cond)
{
	
//	WARN_ON((counter_cswap ++ % 4096 == 0), "cswap_256 is called\n");

	// set high bit and broadcast - to be used as mask
	// refer wiki on how double is represented in 64-bits
	__m256d _mask = _mm256_set1_pd((double)(-(int)cond));
	// load src
	__m256d s = _mm256_load_pd(src);
	// load dst
	__m256d d = _mm256_load_pd(dst);
	__m256d temp;
	// blend registers
	// t = cond ? src : dst;
	// src = cond ? dst : src;
	// dst = cond ? temp : dst;
	temp = _mm256_blendv_pd(d, s, _mask);
	s = _mm256_blendv_pd(s, d, _mask);
	d = _mm256_blendv_pd(d, temp, _mask);
	// write back both src and dst
	_mm256_store_pd(src, s);
	_mm256_store_pd(dst, d);
}
#endif

#ifdef AVX512F
#warning "Compiling with AVX512F"
void obli_cswap_512(double *src, double *dst, bool cond)
{
	// set high bit and broadcast - to be used as mask
	// refer wiki on how double is represented in 64-bits
	__mmask8 _mask = -(static_cast<int>(cond));
	// load src
	__m512d s = _mm512_load_pd(src);
	// load dst
	__m512d d = _mm512_load_pd(dst);
	__m512d temp;
	// blend registers
	// t = cond ? src : dst;
	// src = cond ? dst : src;
	// dst = cond ? temp : dst;
	temp = _mm512_mask_blend_pd(_mask, d, s);
	s = _mm512_mask_blend_pd(_mask, s, d);
	d = _mm512_mask_blend_pd(_mask, d, temp);
	// write back both src and dst
	_mm512_store_pd(src, s);
	_mm512_store_pd(dst, d);
}
#endif
//
//   8    u8
//  16   u16
//  32   u32
//  64   u64
// 128  u128 mmx
// 256  u256 avx2
// 512  u512 avx512
void obli_cswap(u8 * src, u8 * dst, u64 len, bool cond) {
    u64 i = 0;

#ifdef AVX512F
    for( ; i < len - 63; i+=64) {
        obli_cswap_512(((double*)(&src[i])), ((double*)(&dst[i])), cond);
    }
#endif

#ifdef AVX2
    for( ; i < len - 31; i+=32) {
        obli_cswap_256(((double*)(&src[i])), ((double*)(&dst[i])), cond);
    }
#endif

    for( ; i < len - 15; i+=16) {
        obli_cswap_128(((double*)(&src[i])), ((double*)(&dst[i])), cond);
    }

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
