// Copyright (c) 2012, Susumu Yata
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.

#ifndef MADOKA_APPROX_H
#define MADOKA_APPROX_H

#include "random.h"

#ifdef __cplusplus
namespace madoka {

const UInt64 APPROX_SIGNIFICAND_SIZE  = 14;
const UInt64 APPROX_MAX_SIGNIFICAND   = (1ULL << APPROX_SIGNIFICAND_SIZE) - 1;
const UInt64 APPROX_SIGNIFICAND_MASK  = APPROX_MAX_SIGNIFICAND;
const UInt64 APPROX_SIGNIFICAND_SHIFT = 0;

const UInt64 APPROX_EXPONENT_SIZE     = 5;
const UInt64 APPROX_MAX_EXPONENT      = (1ULL << APPROX_EXPONENT_SIZE) - 1;
const UInt64 APPROX_EXPONENT_MASK     = APPROX_MAX_EXPONENT;
const UInt64 APPROX_EXPONENT_SHIFT    = APPROX_SIGNIFICAND_SIZE;

const UInt64 APPROX_SIZE              =
    APPROX_EXPONENT_SIZE + APPROX_SIGNIFICAND_SIZE;
const UInt64 APPROX_MASK              = (1ULL << APPROX_SIZE) - 1;

const UInt64 APPROX_VALUE_SIZE        =
    APPROX_SIGNIFICAND_SIZE + (1 << APPROX_EXPONENT_SIZE) - 1;
const UInt64 APPROX_MAX_VALUE         = (1ULL << APPROX_VALUE_SIZE) - 1;
const UInt64 APPROX_VALUE_MASK        = APPROX_MAX_VALUE;

class Approx {
 public:
  static const UInt64 OFFSET_TABLE[APPROX_MAX_EXPONENT + 1];
  static const UInt8 SHIFT_TABLE[APPROX_MAX_EXPONENT + 1];
  static const UInt32 MASK_TABLE[APPROX_MAX_EXPONENT + 1];

  static UInt64 encode(UInt64 value) noexcept {
    value &= APPROX_VALUE_MASK;
    const UInt64 exponent =
        util::bit_scan_reverse(value | APPROX_SIGNIFICAND_MASK)
        - (APPROX_SIGNIFICAND_SIZE - 1);

#ifndef MADOKA_NOT_PREFER_BRANCH
    if (exponent <= 1) {
      return value;
    }
#endif  // MADOKA_NOT_PREFER_BRANCH

    return (exponent << APPROX_EXPONENT_SHIFT) |
        ((value >> get_shift(exponent)) & APPROX_SIGNIFICAND_MASK);
  }

  static UInt64 decode(UInt64 approx) noexcept {
    const UInt64 exponent =
        (approx >> APPROX_EXPONENT_SHIFT) & APPROX_EXPONENT_MASK;

#ifndef MADOKA_NOT_PREFER_BRANCH
    if (exponent <= 1) {
      return approx;
    }
#endif  // MADOKA_NOT_PREFER_BRANCH

    const UInt64 significand =
        (approx >> APPROX_SIGNIFICAND_SHIFT) & APPROX_SIGNIFICAND_MASK;

#ifndef MADOKA_NOT_PREFER_BRANCH
    return get_offset(exponent) | (significand << get_shift(exponent));
#else  // MADOKA_NOT_PREFER_BRANCH
    return get_offset(exponent) | (significand << get_shift(exponent));
#endif  // MADOKA_NOT_PREFER_BRANCH
  }

  static UInt64 decode(UInt64 approx, Random *random) noexcept {
    const UInt64 exponent =
        (approx >> APPROX_EXPONENT_SHIFT) & APPROX_EXPONENT_MASK;

#ifndef MADOKA_NOT_PREFER_BRANCH
    if (exponent <= 1) {
      return approx;
    }
#endif  // MADOKA_NOT_PREFER_BRANCH

    const UInt64 significand =
        (approx >> APPROX_SIGNIFICAND_SHIFT) & APPROX_SIGNIFICAND_MASK;

#ifndef MADOKA_NOT_PREFER_BRANCH
    return get_offset(exponent) | (significand << get_shift(exponent)) |
        ((*random)() & get_mask(exponent));
#else  // MADOKA_NOT_PREFER_BRANCH
    return get_offset(exponent) | (significand << get_shift(exponent)) |
        ((*random)() & get_mask(exponent));
#endif  // MADOKA_NOT_PREFER_BRANCH
  }

  static UInt64 inc(UInt64 approx, Random *random) noexcept {
    const UInt64 exponent =
        (approx >> APPROX_EXPONENT_SHIFT) & APPROX_EXPONENT_MASK;

#ifndef MADOKA_NOT_PREFER_BRANCH
    approx += (exponent <= 1) || (((*random)() & get_mask(exponent)) == 0);
#else  // MADOKA_NOT_PREFER_BRANCH
    approx += (((*random)() & get_mask(exponent)) - 1ULL) >> 63;
#endif  // MADOKA_NOT_PREFER_BRANCH
    return approx;
  }

 private:
  static UInt64 get_offset(UInt64 exponent) {
#ifndef MADOKA_NOT_PREFER_BRANCH
    return 1ULL << (exponent + (APPROX_SIGNIFICAND_SIZE - 1));
#else  // MADOKA_NOT_PREFER_BRANCH
    static const UInt64 OFFSET_TABLE[APPROX_MAX_EXPONENT + 1] = {
               0, 1ULL << 14, 1ULL << 15, 1ULL << 16, 1ULL << 17, 1ULL << 18,
      1ULL << 19, 1ULL << 20, 1ULL << 21, 1ULL << 22, 1ULL << 23, 1ULL << 24,
      1ULL << 25, 1ULL << 26, 1ULL << 27, 1ULL << 28, 1ULL << 29, 1ULL << 30,
      1ULL << 31, 1ULL << 32, 1ULL << 33, 1ULL << 34, 1ULL << 35, 1ULL << 36,
      1ULL << 37, 1ULL << 38, 1ULL << 39, 1ULL << 40, 1ULL << 41, 1ULL << 42,
      1ULL << 43, 1ULL << 44
    };
    return OFFSET_TABLE[exponent];
#endif  // MADOKA_NOT_PREFER_BRANCH
  }

  static UInt64 get_shift(UInt64 exponent) {
#ifndef MADOKA_NOT_PREFER_BRANCH
    return exponent - 1;
#else  // MADOKA_NOT_PREFER_BRANCH
    static const UInt8 SHIFT_TABLE[APPROX_MAX_EXPONENT + 1] = {
       0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
      15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
    };
    return SHIFT_TABLE[exponent];
#endif  // MADOKA_NOT_PREFER_BRANCH
  }

  static UInt64 get_mask(UInt64 exponent) {
#ifndef MADOKA_NOT_PREFER_BRANCH
    return (1ULL << (exponent - 1)) - 1;
#else  // MADOKA_NOT_PREFER_BRANCH
    static const UInt32 MASK_TABLE[APPROX_MAX_EXPONENT + 1] = {
      0x00000000, 0x00000000, 0x00000001, 0x00000003, 0x00000007, 0x0000000F,
      0x0000001F, 0x0000003F, 0x0000007F, 0x000000FF, 0x000001FF, 0x000003FF,
      0x000007FF, 0x00000FFF, 0x00001FFF, 0x00003FFF, 0x00007FFF, 0x0000FFFF,
      0x0001FFFF, 0x0003FFFF, 0x0007FFFF, 0x000FFFFF, 0x001FFFFF, 0x003FFFFF,
      0x007FFFFF, 0x00FFFFFF, 0x01FFFFFF, 0x03FFFFFF, 0x07FFFFFF, 0x0FFFFFFF,
      0x1FFFFFFF, 0x3FFFFFFF
    };
    return MASK_TABLE[exponent];
#endif  // MADOKA_NOT_PREFER_BRANCH
  }
};

}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_APPROX_H
