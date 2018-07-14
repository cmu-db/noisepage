#pragma once

#include "common/common_defs.h"

namespace terrier {
uint32_t BitmapSize(uint32_t n) {
  return n % 8 == 0 ? n / 8 : n / 8 + 1;
}

// TODO(Tianyu): Pretty sure we need this? Maybe not concurrent...
// C++ bitmap pads an unspecified amount of bytes.
// TODO(Tianyu): Implement
template <uint32_t N>
class PACKED ConcurrentBitmap {
 private:
  byte bits_[BitmapSize(N)] {};
};
}
