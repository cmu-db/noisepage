#include <unordered_set>
#include <bitset>
#include <atomic>
#include <thread>

#include "gtest/gtest.h"
#include "common/test_util.h"
#include "common/concurrent_bitmap.h"

namespace terrier {
class ConcurrentBitmapTests : public ::testing::Test {
 public:
  template<uint32_t num_elements>
  void CheckReferenceBitmap(const ConcurrentBitmap<num_elements> &tested,
                            const std::bitset<num_elements> &reference) {
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_EQ(reference[i], tested[i]);
      if (reference[i] != tested[i]) {
        printf("%d\n", i);
      }
    }
  }
};

TEST_F(ConcurrentBitmapTests, SimpleCorrectnessTest) {
  const uint32_t num_elements = 16;
  ConcurrentBitmap<num_elements> bitmap;

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_FALSE(bitmap.Test(i));
  }

  // Verify size is correct
  EXPECT_EQ(num_elements, bitmap.Size());

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements> stl_bitmap;
  CheckReferenceBitmap<num_elements>(bitmap, stl_bitmap);
  uint32_t num_iterations = 32;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element = std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
    EXPECT_TRUE(bitmap.Flip(element, bitmap.Test(element)));
    stl_bitmap.flip(element);
    CheckReferenceBitmap<num_elements>(bitmap, stl_bitmap);
  }

  // Verify that Flip fails if expected_val doesn't match current value
  auto element = std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
  EXPECT_FALSE(bitmap.Flip(element, !bitmap.Test(element)));
}

}