#pragma once

#include <bitset>

namespace terrier {
template <typename Bitmap, uint32_t num_elements>
void CheckReferenceBitmap(const Bitmap &tested, const std::bitset<num_elements> &reference) {
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_EQ(reference[i], tested[i]);
  }
}
}  // namespace terrier
