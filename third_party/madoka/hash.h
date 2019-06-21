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

#ifndef MADOKA_HASH_H
#define MADOKA_HASH_H

#include "util.h"

#ifdef __cplusplus
namespace madoka {

class Hash {
 public:
  void operator()(const void *key_addr, std::size_t key_size,
                  UInt64 seed, UInt64 hash_values[2]) const noexcept {
    const UInt8 * const bytes = static_cast<const UInt8 *>(key_addr);
    const std::size_t num_blocks = key_size / 16;

    UInt64 h1 = seed;
    UInt64 h2 = seed;

    const UInt64 * const blocks = reinterpret_cast<const UInt64 *>(bytes);

    for (std::size_t i = 0; i < num_blocks; ++i) {
      UInt64 k1 = blocks[i * 2];
      UInt64 k2 = blocks[(i * 2) + 1];

      k1 *= C1;
      k1 = rotate(k1, 31);
      k1 *= C2;
      h1 ^= k1;

      h1 = rotate(h1, 27);
      h1 += h2;
      h1 = (h1 * 5) + 0x52DCE729;

      k2 *= C2;
      k2 = rotate(k2, 33);
      k2 *= C1;
      h2 ^= k2;

      h2 = rotate(h2, 31);
      h2 += h1;
      h2 = (h2 * 5) + 0x38495AB5;
    }

    const UInt8 * const tail =
        static_cast<const UInt8 *>(bytes + (num_blocks * 16));

    UInt64 k1 = 0;
    UInt64 k2 = 0;

    switch (key_size & 15) {
      case 15: {
        k2 ^= static_cast<UInt64>(tail[14]) << 48;
      }
      /* FALLTHRU */
      case 14: {
        k2 ^= static_cast<UInt64>(tail[13]) << 40;
      }
      /* FALLTHRU */
      case 13: {
        k2 ^= static_cast<UInt64>(tail[12]) << 32;
      }
      /* FALLTHRU */
      case 12: {
        k2 ^= static_cast<UInt64>(tail[11]) << 24;
      }
      /* FALLTHRU */
      case 11: {
        k2 ^= static_cast<UInt64>(tail[10]) << 16;
      }
      /* FALLTHRU */
      case 10: {
        k2 ^= static_cast<UInt64>(tail[ 9]) << 8;
      }
      /* FALLTHRU */
      case 9: {
        k2 ^= static_cast<UInt64>(tail[8]) << 0;
        k2 *= C2;
        k2 = rotate(k2, 33);
        k2 *= C1;
        h2 ^= k2;
      }
      /* FALLTHRU */
      case 8: {
        k1 ^= static_cast<UInt64>(tail[7]) << 56;
      }
      /* FALLTHRU */
      case 7: {
        k1 ^= static_cast<UInt64>(tail[6]) << 48;
      }
      /* FALLTHRU */
      case 6: {
        k1 ^= static_cast<UInt64>(tail[5]) << 40;
      }
      /* FALLTHRU */
      case 5: {
        k1 ^= static_cast<UInt64>(tail[4]) << 32;
      }
      /* FALLTHRU */
      case 4: {
        k1 ^= static_cast<UInt64>(tail[3]) << 24;
      }
      /* FALLTHRU */
      case 3: {
        k1 ^= static_cast<UInt64>(tail[2]) << 16;
      }
      /* FALLTHRU */
      case 2: {
        k1 ^= static_cast<UInt64>(tail[1]) << 8;
      }
      /* FALLTHRU */
      case 1: {
        k1 ^= static_cast<UInt64>(tail[0]) << 0;
        k1 *= C1;
        k1 = rotate(k1, 31);
        k1 *= C2;
        h1 ^= k1;
      }
    };

    h1 ^= key_size;
    h2 ^= key_size;

    h1 += h2;
    h2 += h1;

    h1 = mix(h1);
    h2 = mix(h2);

    h1 += h2;
    h2 += h1;

    hash_values[0] = h1;
    hash_values[1] = h2;
  }

 private:
  static const UInt64 C1 = 0x87C37B91114253D5ULL;
  static const UInt64 C2 = 0x4CF5AD432745937FULL;

  static UInt64 rotate(UInt64 x, UInt64 y) noexcept {
    return (x << y) | (x >> (64 - y));
  }

  static UInt64 mix(UInt64 x) noexcept {
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDULL;
    x ^= x >> 33;
    x *= 0xC4CEB9FE1A85EC53ULL;
    x ^= x >> 33;
    return x;
  }
};

}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_HASH_H
