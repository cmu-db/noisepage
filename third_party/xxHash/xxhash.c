/*
*  xxHash - Fast Hash algorithm
*  Copyright (C) 2012-2016, Yann Collet
*
*  BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
*
*  Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions are
*  met:
*
*  * Redistributions of source code must retain the above copyright
*  notice, this list of conditions and the following disclaimer.
*  * Redistributions in binary form must reproduce the above
*  copyright notice, this list of conditions and the following disclaimer
*  in the documentation and/or other materials provided with the
*  distribution.
*
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
*  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
*  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
*  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
*  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
*  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
*  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
*  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
*  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
*  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
*  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*
*  You can contact the author at :
*  - xxHash homepage: http://www.xxhash.com
*  - xxHash source repository : https://github.com/Cyan4973/xxHash
*/


/* since xxhash.c can be included (via XXH_INLINE_ALL),
 * it's good practice to protect it with guard
 * in case of multiples inclusions */
#ifndef XXHASH_C_01393879
#define XXHASH_C_01393879

/* *************************************
*  Tuning parameters
***************************************/
#define XXH_ACCEPT_NULL_INPUT_POINTER 0
#define XXH_FORCE_ALIGN_CHECK 0
#ifndef XXH_REROLL
#  if defined(__OPTIMIZE_SIZE__)
#    define XXH_REROLL 1
#  else
#    define XXH_REROLL 0
#  endif
#endif

/* *************************************
*  Includes & Memory related functions
***************************************/
/*! and for memcpy() */
#include <string.h>
#include <limits.h>   /* ULLONG_MAX */

#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"


/* *************************************
*  Compiler Specific Options
***************************************/

#if defined (__cplusplus) || defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* C99 */
#  ifdef __GNUC__
#    define XXH_FORCE_INLINE static inline __attribute__((always_inline))
#    define XXH_NO_INLINE static __attribute__((noinline))
#  else
#    define XXH_FORCE_INLINE static inline
#    define XXH_NO_INLINE static
#  endif
#else
#  define XXH_FORCE_INLINE static
#  define XXH_NO_INLINE static
#endif /* __STDC_VERSION__ */


/* *************************************
*  Debug
***************************************/
/* DEBUGLEVEL is expected to be defined externally,
 * typically through compiler command line.
 * Value must be a number. */
#ifndef DEBUGLEVEL
#  define DEBUGLEVEL 0
#endif

#if (DEBUGLEVEL>=1)
#  include <assert.h>   /* note : can still be disabled with NDEBUG */
#  define XXH_ASSERT(c)   assert(c)
#else
#  define XXH_ASSERT(c)   ((void)0)
#endif

/* note : use after variable declarations */
#define XXH_STATIC_ASSERT(c)  { enum { XXH_sa = 1/(int)(!!(c)) }; }

/* ===   Memory access   === */

/* portable and safe solution. Generally efficient.
 * see : http://stackoverflow.com/a/32095106/646947
 */
static xxh_u32 XXH_read32(const void* memPtr)
{
    xxh_u32 val;
    memcpy(&val, memPtr, sizeof(val));
    return val;
}

/* ****************************************
*  Compiler-specific Functions and Macros
******************************************/

#ifndef __has_builtin
#  define __has_builtin(x) 0
#endif

#if !defined(NO_CLANG_BUILTIN) && __has_builtin(__builtin_rotateleft32) && __has_builtin(__builtin_rotateleft64)
#  define XXH_rotl32 __builtin_rotateleft32
#  define XXH_rotl64 __builtin_rotateleft64
#else
#  define XXH_rotl32(x,r) (((x) << (r)) | ((x) >> (32 - (r))))
#  define XXH_rotl64(x,r) (((x) << (r)) | ((x) >> (64 - (r))))
#endif

/* ***************************
*  Memory reads
*****************************/
typedef enum { XXH_aligned, XXH_unaligned } XXH_alignment;

XXH_FORCE_INLINE xxh_u32 XXH_readLE32(const void* ptr)
{
    return XXH_read32(ptr);
}

/* *******************************************************************
*  64-bit hash functions
*********************************************************************/

/*======   Memory access   ======*/

typedef XXH64_hash_t xxh_u64;

#define XXH_REROLL_XXH64 0

#if (defined(XXH_FORCE_MEMORY_ACCESS) && (XXH_FORCE_MEMORY_ACCESS==2))

/* Force direct memory access. Only works on CPU which support unaligned memory access in hardware */
static xxh_u64 XXH_read64(const void* memPtr) { return *(const xxh_u64*) memPtr; }

#elif (defined(XXH_FORCE_MEMORY_ACCESS) && (XXH_FORCE_MEMORY_ACCESS==1))

/* __pack instructions are safer, but compiler specific, hence potentially problematic for some compilers */
/* currently only defined for gcc and icc */
typedef union { xxh_u32 u32; xxh_u64 u64; } __attribute__((packed)) unalign64;
static xxh_u64 XXH_read64(const void* ptr) { return ((const unalign64*)ptr)->u64; }

#else

/* portable and safe solution. Generally efficient.
 * see : http://stackoverflow.com/a/32095106/646947
 */

static xxh_u64 XXH_read64(const void* memPtr)
{
    xxh_u64 val;
    memcpy(&val, memPtr, sizeof(val));
    return val;
}

#endif   /* XXH_FORCE_DIRECT_MEMORY_ACCESS */

XXH_FORCE_INLINE xxh_u64 XXH_readLE64(const void* ptr)
{
    return XXH_read64(ptr);
}

XXH_FORCE_INLINE xxh_u64
XXH_readLE64_align(const void* ptr, XXH_alignment align)
{
    if (align==XXH_unaligned)
        return XXH_readLE64(ptr);
    else
        return *(const xxh_u64*)ptr;
}


/*======   xxh64   ======*/

static const xxh_u64 PRIME64_1 = 0x9E3779B185EBCA87ULL;   /* 0b1001111000110111011110011011000110000101111010111100101010000111 */
static const xxh_u64 PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;   /* 0b1100001010110010101011100011110100100111110101001110101101001111 */
static const xxh_u64 PRIME64_3 = 0x165667B19E3779F9ULL;   /* 0b0001011001010110011001111011000110011110001101110111100111111001 */
static const xxh_u64 PRIME64_4 = 0x85EBCA77C2B2AE63ULL;   /* 0b1000010111101011110010100111011111000010101100101010111001100011 */
static const xxh_u64 PRIME64_5 = 0x27D4EB2F165667C5ULL;   /* 0b0010011111010100111010110010111100010110010101100110011111000101 */

static xxh_u64 XXH64_round(xxh_u64 acc, xxh_u64 input)
{
    acc += input * PRIME64_2;
    acc  = XXH_rotl64(acc, 31);
    acc *= PRIME64_1;
    return acc;
}

static xxh_u64 XXH64_mergeRound(xxh_u64 acc, xxh_u64 val)
{
    val  = XXH64_round(0, val);
    acc ^= val;
    acc  = acc * PRIME64_1 + PRIME64_4;
    return acc;
}

static xxh_u64 XXH64_avalanche(xxh_u64 h64)
{
    h64 ^= h64 >> 33;
    h64 *= PRIME64_2;
    h64 ^= h64 >> 29;
    h64 *= PRIME64_3;
    h64 ^= h64 >> 32;
    return h64;
}


XXH_FORCE_INLINE xxh_u32
XXH_readLE32_align(const void* ptr, XXH_alignment align)
{
  if (align==XXH_unaligned) {
    return XXH_readLE32(ptr);
  } else {
    return *(const xxh_u32*)ptr;
  }
}

#define XXH_get32bits(p) XXH_readLE32_align(p, align)
#define XXH_get64bits(p) XXH_readLE64_align(p, align)

static xxh_u64
XXH64_finalize(xxh_u64 h64, const xxh_u8* ptr, size_t len, XXH_alignment align)
{
#define PROCESS1_64            \
    h64 ^= (*ptr++) * PRIME64_5; \
    h64 = XXH_rotl64(h64, 11) * PRIME64_1;

#define PROCESS4_64          \
    h64 ^= (xxh_u64)(XXH_get32bits(ptr)) * PRIME64_1; \
    ptr+=4;                    \
    h64 = XXH_rotl64(h64, 23) * PRIME64_2 + PRIME64_3;

#define PROCESS8_64 {        \
    xxh_u64 const k1 = XXH64_round(0, XXH_get64bits(ptr)); \
    ptr+=8;                    \
    h64 ^= k1;               \
    h64  = XXH_rotl64(h64,27) * PRIME64_1 + PRIME64_4; \
}

    /* Rerolled version for 32-bit targets is faster and much smaller. */
    if (XXH_REROLL || XXH_REROLL_XXH64) {
        len &= 31;
        while (len >= 8) {
            PROCESS8_64;
            len -= 8;
        }
        if (len >= 4) {
            PROCESS4_64;
            len -= 4;
        }
        while (len > 0) {
            PROCESS1_64;
            --len;
        }
         return  XXH64_avalanche(h64);
    } else {
        switch(len & 31) {
           case 24: PROCESS8_64;
                         /* fallthrough */
           case 16: PROCESS8_64;
                         /* fallthrough */
           case  8: PROCESS8_64;
                    return XXH64_avalanche(h64);

           case 28: PROCESS8_64;
                         /* fallthrough */
           case 20: PROCESS8_64;
                         /* fallthrough */
           case 12: PROCESS8_64;
                         /* fallthrough */
           case  4: PROCESS4_64;
                    return XXH64_avalanche(h64);

           case 25: PROCESS8_64;
                         /* fallthrough */
           case 17: PROCESS8_64;
                         /* fallthrough */
           case  9: PROCESS8_64;
                    PROCESS1_64;
                    return XXH64_avalanche(h64);

           case 29: PROCESS8_64;
                         /* fallthrough */
           case 21: PROCESS8_64;
                         /* fallthrough */
           case 13: PROCESS8_64;
                         /* fallthrough */
           case  5: PROCESS4_64;
                    PROCESS1_64;
                    return XXH64_avalanche(h64);

           case 26: PROCESS8_64;
                         /* fallthrough */
           case 18: PROCESS8_64;
                         /* fallthrough */
           case 10: PROCESS8_64;
                    PROCESS1_64;
                    PROCESS1_64;
                    return XXH64_avalanche(h64);

           case 30: PROCESS8_64;
                         /* fallthrough */
           case 22: PROCESS8_64;
                         /* fallthrough */
           case 14: PROCESS8_64;
                         /* fallthrough */
           case  6: PROCESS4_64;
                    PROCESS1_64;
                    PROCESS1_64;
                    return XXH64_avalanche(h64);

           case 27: PROCESS8_64;
                         /* fallthrough */
           case 19: PROCESS8_64;
                         /* fallthrough */
           case 11: PROCESS8_64;
                    PROCESS1_64;
                    PROCESS1_64;
                    PROCESS1_64;
                    return XXH64_avalanche(h64);

           case 31: PROCESS8_64;
                         /* fallthrough */
           case 23: PROCESS8_64;
                         /* fallthrough */
           case 15: PROCESS8_64;
                         /* fallthrough */
           case  7: PROCESS4_64;
                         /* fallthrough */
           case  3: PROCESS1_64;
                         /* fallthrough */
           case  2: PROCESS1_64;
                         /* fallthrough */
           case  1: PROCESS1_64;
                         /* fallthrough */
           case  0: return XXH64_avalanche(h64);
        }
    }
    /* impossible to reach */
    XXH_ASSERT(0);
    return 0;  /* unreachable, but some compilers complain without it */
}

XXH_FORCE_INLINE xxh_u64
XXH64_endian_align(const xxh_u8* input, size_t len, xxh_u64 seed, XXH_alignment align)
{
    const xxh_u8* bEnd = input + len;
    xxh_u64 h64;

#if defined(XXH_ACCEPT_NULL_INPUT_POINTER) && (XXH_ACCEPT_NULL_INPUT_POINTER>=1)
    if (input==NULL) {
        len=0;
        bEnd=input=(const xxh_u8*)(size_t)32;
    }
#endif

    if (len>=32) {
        const xxh_u8* const limit = bEnd - 32;
        xxh_u64 v1 = seed + PRIME64_1 + PRIME64_2;
        xxh_u64 v2 = seed + PRIME64_2;
        xxh_u64 v3 = seed + 0;
        xxh_u64 v4 = seed - PRIME64_1;

        do {
            v1 = XXH64_round(v1, XXH_get64bits(input)); input+=8;
            v2 = XXH64_round(v2, XXH_get64bits(input)); input+=8;
            v3 = XXH64_round(v3, XXH_get64bits(input)); input+=8;
            v4 = XXH64_round(v4, XXH_get64bits(input)); input+=8;
        } while (input<=limit);

        h64 = XXH_rotl64(v1, 1) + XXH_rotl64(v2, 7) + XXH_rotl64(v3, 12) + XXH_rotl64(v4, 18);
        h64 = XXH64_mergeRound(h64, v1);
        h64 = XXH64_mergeRound(h64, v2);
        h64 = XXH64_mergeRound(h64, v3);
        h64 = XXH64_mergeRound(h64, v4);

    } else {
        h64  = seed + PRIME64_5;
    }

    h64 += (xxh_u64) len;

    return XXH64_finalize(h64, input, len, align);
}


XXH_PUBLIC_API XXH64_hash_t XXH64 (const void* input, size_t len, XXH64_hash_t seed)
{
#if 0
    /* Simple version, good for code maintenance, but unfortunately slow for small inputs */
    XXH64_state_t state;
    XXH64_reset(&state, seed);
    XXH64_update(&state, (const xxh_u8*)input, len);
    return XXH64_digest(&state);

#else

    if (XXH_FORCE_ALIGN_CHECK) {
        if ((((size_t)input) & 7)==0) {  /* Input is aligned, let's leverage the speed advantage */
            return XXH64_endian_align((const xxh_u8*)input, len, seed, XXH_aligned);
    }   }

    return XXH64_endian_align((const xxh_u8*)input, len, seed, XXH_unaligned);

#endif
}

#include "xxh3.h"

#endif  /* XXHASH_C_01393879 */
