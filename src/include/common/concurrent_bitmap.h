#pragma once

#include "common/common_defs.h"

#define BYTE_SIZE 8
// n must be [0, 7], all 0 except for 1 on the nth bit
#define ONE_BYTE_MASK(n) (1 << (BYTE_SIZE - (n) - 1))
// n must be [0, 7], all 1 except for 0 on the nth bit
#define ZERO_BYTE_MASK(n) (0xFF - ONE_BYTE_MASK(n))

namespace terrier {
uint32_t BitmapSize(uint32_t n) {
  return n % BYTE_SIZE == 0 ? n / BYTE_SIZE : n / BYTE_SIZE + 1;
}

// TODO(Tianyu): Pretty sure we need this? Maybe not concurrent...
// C++ bitmap pads an unspecified amount of bytes.
// TODO(Tianyu): Implement
template <uint32_t N>
class PACKED ConcurrentBitmap {
  bool test(uint32_t pos) const {
    PELOTON_ASSERT(pos < N);
    return static_cast<bool>(
        bits_[pos / BYTE_SIZE] & ONE_BYTE_MASK(pos % BYTE_SIZE));
  }

  bool operator[](uint32_t pos) const {
    return test(pos);
  }

  uint32_t Size() const {
    return N;
  }

  uint32_t ByteSize() {
    return BitmapSize(N);
  }

  ConcurrentBitmap &Set(uint32_t pos, bool val) {
    if (val)
      bits_[pos / BYTE_SIZE] |= ONE_BYTE_MASK(pos);
    else
      bits_[pos / BYTE_SIZE] &= ZERO_BYTE_MASK(pos);
    return *this;
  }

  ConcurrentBitmap &Flip(uint32_t pos) {
    bits_[pos / BYTE_SIZE] ^= ONE_BYTE_MASK(pos);
    return *this;
  }

 private:
  // TODO(Tianyu): Change to atomic bytes if concurrent
  uint8_t bits_[BitmapSize(N)] {};
};
}
