#pragma once

#include <vector>

namespace terrier {
/**
 * Static utility class for container tests
 */
struct ContainerTestUtil {
  ContainerTestUtil() = delete;

  template <typename Bitmap, uint32_t num_elements>
  static void CheckReferenceBitmap(const Bitmap &tested, const std::vector<bool> &reference) {
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_EQ(reference[i], tested[i]);
    }
  }
};

}  // namespace terrier
