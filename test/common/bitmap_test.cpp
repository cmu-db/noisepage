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

TEST(BitmapTests, ByteMultipleCorrectnessTest) {
  std::default_random_engine generator;
  // Number of times to randomly permute bitmap
  uint32_t num_iterations = 1000;

  // Test a bitmap that whose size is byte multiple
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

TEST(BitmapTests, NonByteMultipleCorrectnessTest) {
  std::default_random_engine generator;
  // Number of times to randomly permute bitmap
  uint32_t num_iterations = 1000;

  // Test a bitmap that whose size is not byte multiple
  const uint32_t num_elements_unaligned = 19;
  common::RawBitmap *non_multiple_bitmap = common::RawBitmap::Allocate(num_elements_unaligned);

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements_unaligned; ++i) {
    EXPECT_FALSE(non_multiple_bitmap->Test(i));
  }

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements_unaligned> stl_bitmap;
  CheckReferenceBitmap<num_elements_unaligned>(*non_multiple_bitmap, stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, (int) num_elements_unaligned - 1)(generator);
    non_multiple_bitmap->Flip(element);
    stl_bitmap.flip(element);
    CheckReferenceBitmap<num_elements_unaligned>(*non_multiple_bitmap, stl_bitmap);
  }

  common::RawBitmap::Deallocate(non_multiple_bitmap);
}

TEST(BitmapTests, WordUnalignedCorrectnessTest) {
  std::default_random_engine generator;
  // Number of times to randomly permute bitmap
  uint32_t num_iterations = 1000;

  // Test a bitmap that whose size is byte multiple
  const uint32_t num_elements = 16;
//  common::RawBitmap *aligned_bitmap = common::RawBitmap::Allocate(num_elements);

  // provision enough space for the bitmap elements, plus 3 extra because we're going to make it unaligned to wordsize
  auto size = BitmapSize(num_elements) + 3;
  auto allocated_buffer = new uint8_t[size];
  PELOTON_MEMSET(allocated_buffer, 0, size);

  // make the bitmap not word-aligned
  auto unaligned_buffer = allocated_buffer;
  while (reinterpret_cast<uintptr_t>(unaligned_buffer) % sizeof(uint64_t) != 3) {
    unaligned_buffer++;
  }

  auto unaligned_bitmap = reinterpret_cast<common::RawBitmap *>(unaligned_buffer);

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_FALSE(unaligned_bitmap->Test(i));
  }

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements> stl_bitmap;
  CheckReferenceBitmap<num_elements>(*unaligned_bitmap, stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
    unaligned_bitmap->Flip(element);
    stl_bitmap.flip(element);
    CheckReferenceBitmap<num_elements>(*unaligned_bitmap, stl_bitmap);
  }

  delete[] allocated_buffer;
}
}
