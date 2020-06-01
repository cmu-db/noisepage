#include "common/gate.h"

#include <immintrin.h>

namespace terrier::common {

void Gate::Traverse() {
  while (count_.load() > 0) {
    _mm_pause();
  }
}

} // namespace terrier::common
