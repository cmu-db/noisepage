#include <algorithm>
#include <atomic>
#include <bitset>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"
#include "common/test_util.h"
#include "common/container/bitmap.h"

namespace terrier {
template<uint32_t num_elements>
void CheckReferenceBitmap(const common::RawBitmap &tested,
                          const std::bitset<num_elements> &reference) {
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_EQ(reference[i], tested[i]);
  }
}

TEST(BitmapTests, AlignedCorrectnessTest) {
  std::default_random_engine generator;
  // Number of times to randomly permute bitmap
  uint32_t num_iterations = 1000;

  // Test a bitmap that is byte aligned for correctness
  const uint32_t num_elements_aligned = 16;
  common::RawBitmap *aligned_bitmap = common::RawBitmap::Allocate(num_elements_aligned);

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements_aligned; ++i) {
    EXPECT_FALSE(aligned_bitmap->Test(i));
  }

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements_aligned> stl_bitmap;
  CheckReferenceBitmap<num_elements_aligned>(*aligned_bitmap, stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, (int) num_elements_aligned - 1)(generator);
    aligned_bitmap->Flip(element);
    stl_bitmap.flip(element);
    CheckReferenceBitmap<num_elements_aligned>(*aligned_bitmap, stl_bitmap);
  }

  common::RawBitmap::Deallocate(aligned_bitmap);
}

TEST(BitmapTests, UnalignedCorrectnessTest) {
  std::default_random_engine generator;
  // Number of times to randomly permute bitmap
  uint32_t num_iterations = 1000;

  const uint32_t num_elements_unaligned = 19;
  common::RawBitmap *unaligned_bitmap = common::RawBitmap::Allocate(num_elements_unaligned);

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements_unaligned; ++i) {
    EXPECT_FALSE(unaligned_bitmap->Test(i));
  }

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements_unaligned> unaligned_stl_bitmap;
  CheckReferenceBitmap<num_elements_unaligned>(*unaligned_bitmap, unaligned_stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, (int) num_elements_unaligned - 1)(generator);
    unaligned_bitmap->Flip(element);
    unaligned_stl_bitmap.flip(element);
    CheckReferenceBitmap<num_elements_unaligned>(*unaligned_bitmap, unaligned_stl_bitmap);
  }

  common::RawBitmap::Deallocate(unaligned_bitmap);
}
}
