#include <random>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>
#include "gtest/gtest.h"
#include "common/container/bitmap.h"
#include "util/container_test_util.h"

namespace terrier {

// tests a RawBitmap whose base pointer is word aligned, and the num_elements is a multiple of 8 (byte size)
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
  std::vector<bool> stl_bitmap = std::vector<bool>(num_elements_aligned);
  ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap, num_elements_aligned>(*aligned_bitmap, stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, static_cast<int>(num_elements_aligned - 1))(generator);
    aligned_bitmap->Flip(element);
    stl_bitmap[element] = !stl_bitmap[element];
    ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap, num_elements_aligned>(*aligned_bitmap, stl_bitmap);
  }

  common::RawBitmap::Deallocate(aligned_bitmap);
}

// tests a RawBitmap whose base pointer is word aligned, but the num_elements is not a multiple of 8 (byte size)
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
  std::vector<bool> stl_bitmap = std::vector<bool>(num_elements_unaligned);
  ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap, num_elements_unaligned>(*non_multiple_bitmap, stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, static_cast<int>(num_elements_unaligned - 1))(generator);
    non_multiple_bitmap->Flip(element);
    stl_bitmap[element] = !stl_bitmap[element];
    ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap, num_elements_unaligned>(*non_multiple_bitmap,
                                                                                       stl_bitmap);
  }

  common::RawBitmap::Deallocate(non_multiple_bitmap);
}

// tests a RawBitmap whose base pointer is not word aligned
TEST(BitmapTests, WordUnalignedCorrectnessTest) {
  std::default_random_engine generator;
  // Number of times to randomly permute bitmap
  uint32_t num_iterations = 1000;

  // Test a bitmap that whose size is byte multiple
  const uint32_t num_elements = 16;

  // provision enough space for the bitmap elements, plus padding because we're going to make it unaligned to wordsize
  auto size = common::BitmapSize(num_elements) + sizeof(uint64_t);
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
  std::vector<bool> stl_bitmap = std::vector<bool>(num_elements);
  ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap, num_elements>(*unaligned_bitmap, stl_bitmap);
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, static_cast<int>(num_elements - 1))(generator);
    unaligned_bitmap->Flip(element);
    stl_bitmap[element] = !stl_bitmap[element];
    ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap, num_elements>(*unaligned_bitmap, stl_bitmap);
  }

  delete[] allocated_buffer;
}
}  // namespace terrier
