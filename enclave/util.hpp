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

#if defined(__cplusplus)
extern "C" {
#endif

void printf(const char *fmt, ...);

//unsigned long long RDTSC( void );


#if defined(__cplusplus)
}
#endif

#ifdef AVX512F
// AVX-512 instructions from avx512fintrin.h
typedef double __m512d __attribute__ ((__vector_size__ (64), __may_alias__));

typedef double __v8df __attribute__ ((__vector_size__ (64)));

typedef unsigned char  __mmask8;

extern __inline __m512d
__attribute__ ((__gnu_inline__, __always_inline__, __artificial__))
_mm512_mask_blend_pd (__mmask8 __U, __m512d __A, __m512d __W)
{
  return (__m512d) __builtin_ia32_blendmpd_512_mask ((__v8df) __A,
						     (__v8df) __W,
						     (__mmask8) __U);
}

extern __inline __m512d
__attribute__ ((__gnu_inline__, __always_inline__, __artificial__))
_mm512_load_pd (void const *__P)
{
  return *(__m512d *) __P;
}

extern __inline void
__attribute__ ((__gnu_inline__, __always_inline__, __artificial__))
_mm512_store_pd (void *__P, __m512d __A)
{
  *(__m512d *) __P = __A;
}
#endif // AVX512F

#ifdef AVX2
// AVX instructions from avxintrin.h
typedef double __m256d __attribute__ ((__vector_size__ (32),
				       __may_alias__));
typedef double __v4df __attribute__ ((__vector_size__ (32)));

/* Create a vector with all elements equal to A.  */
extern __inline __m256d __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm256_set1_pd (double __A)
{
  return __extension__ (__m256d){ __A, __A, __A, __A };
}

extern __inline __m256d __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm256_load_pd (double const *__P)
{
  return *(__m256d *)__P;
}

extern __inline __m256d __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm256_blendv_pd (__m256d __X, __m256d __Y, __m256d __M)
{
  return (__m256d) __builtin_ia32_blendvpd256 ((__v4df)__X,
					       (__v4df)__Y,
					       (__v4df)__M);
}

extern __inline void __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm256_store_pd (double *__P, __m256d __A)
{
  *(__m256d *)__P = __A;
}
#endif // AVX2

// SSE128 instructions from emmintrin.h
typedef double __m128d __attribute__ ((__vector_size__ (16), __may_alias__));
/* SSE2 */
typedef double __v2df __attribute__ ((__vector_size__ (16)));

/* Create a vector with both elements equal to F.  */
extern __inline __m128d __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm_set1_pd (double __F)
{
  return __extension__ (__m128d){ __F, __F };
}

/* Load two DPFP values from P.  The address must be 16-byte aligned.  */
extern __inline __m128d __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm_load_pd (double const *__P)
{
  return *(__m128d *)__P;
}

extern __inline __m128d __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm_blendv_pd (__m128d __X, __m128d __Y, __m128d __M)
{
  return (__m128d) __builtin_ia32_blendvpd ((__v2df)__X,
					    (__v2df)__Y,
					    (__v2df)__M);
}

/* Store two DPFP values.  The address must be 16-byte aligned.  */
extern __inline void __attribute__((__gnu_inline__, __always_inline__, __artificial__))
_mm_store_pd (double *__P, __m128d __A)
{
  *(__m128d *)__P = __A;
}


#if 0
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

#endif
