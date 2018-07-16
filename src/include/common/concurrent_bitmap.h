#pragma once

#include "common/common_defs.h"

#ifndef BYTE_SIZE
#define BYTE_SIZE 8u
#endif

static_assert(BYTE_SIZE == 8u, "BYTE_SIZE should be set to 8!");

// n must be [0, 7], all 0 except for 1 on the nth bit
#define ONE_HOT_MASK(n) (1u << (BYTE_SIZE - (n) - 1u))
// n must be [0, 7], all 1 except for 0 on the nth bit
#define ONE_COLD_MASK(n) (0xFF - ONE_HOT_MASK(n))

namespace terrier {
constexpr uint32_t BitmapSize(uint32_t n) {
  return n % BYTE_SIZE == 0 ? n / BYTE_SIZE : n / BYTE_SIZE + 1;
}

// TODO(Tianyu): Pretty sure we need this? Maybe not concurrent...
// C++ bitmap pads an unspecified amount of bytes.
// TODO(Tianyu): Implement
template <uint32_t N>
class PACKED ConcurrentBitmap {

 public:

  bool Test(uint32_t pos) const {
    PELOTON_ASSERT(pos < N);
    return static_cast<bool>(
        bits_[pos / BYTE_SIZE].load() & ONE_HOT_MASK(pos % BYTE_SIZE));
  }

  bool operator[](uint32_t pos) const {
    return Test(pos);
  }

  uint32_t Size() const {
    return N;
  }

  uint32_t ByteSize() {
    return BitmapSize(N);
  }

  // For test or initialization only
  ConcurrentBitmap &UnsafeSet(uint32_t pos, bool val) {
    PELOTON_ASSERT(pos < N);
    if (val)
      bits_[pos / BYTE_SIZE] |= ONE_HOT_MASK(pos);
    else
      bits_[pos / BYTE_SIZE] &= ONE_COLD_MASK(pos);
    return *this;
  }

  /**
   * @brief Flip the bit only if current value is actually expected_val
   * The expected_val is needed to guard against the following situation:
   * Caller 1 flips from 0 to 1, Caller 2 flips from 0 to 1, without using
   * expected_val, both calls could succeed with the resulting bit value of 0
   * @param pos the position of the bit to flip
   * @param expected_val the expected current value of the bit to be flipped
   * @return true if flip succeeds, otherwise expected_val didn't match
   */
  bool Flip(uint32_t pos, bool expected_val) {
    PELOTON_ASSERT(pos < N);
    uint32_t element = pos / BYTE_SIZE;
    auto mask = static_cast<uint8_t>(ONE_HOT_MASK(pos % BYTE_SIZE));
    for (uint8_t old_val = bits_[element].load();
        static_cast<bool>(old_val & mask) == expected_val;
        old_val = bits_[element].load()) {
      uint8_t new_val = old_val ^ mask;
      if (bits_[element].compare_exchange_strong(old_val, new_val)) return true;
    }
    return false;
  }

 private:
  std::atomic<uint8_t> bits_[BitmapSize(N)] {};
};
}
