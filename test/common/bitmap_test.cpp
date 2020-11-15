#include "common/container/bitmap.h"

#include <cstring>
#include <random>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "test_util/container_test_util.h"

namespace noisepage {

// Tests a RawBitmap whose base pointer is word aligned, where num_elements is a multiple of 8 (byte size)
// NOLINTNEXTLINE
TEST(BitmapTests, ByteMultipleCorrectnessTest) {
  std::default_random_engine generator;
  // Number of bitmap sizes to test.
  const uint32_t num_bitmap_sizes = 300;
  // Maximum bitmap size multiplier.
  const uint32_t max_size_multiplier = 100;
  // Number of times to randomly permute bitmap
  const uint32_t num_iterations = 100;

  for (uint32_t iter = 0; iter < num_bitmap_sizes; ++iter) {
    // Test a byte-multiple sized bitmap
    auto multiplier = std::uniform_int_distribution(1U, max_size_multiplier)(generator);
    const uint32_t num_elements_aligned = 16 * multiplier;

    common::RawBitmap *aligned_bitmap = common::RawBitmap::Allocate(num_elements_aligned);

    // Verify bitmap initialized to all 0s
    for (uint32_t i = 0; i < num_elements_aligned; ++i) {
      EXPECT_FALSE(aligned_bitmap->Test(i));
    }

    // Randomly permute bitmap and STL bitmap and compare equality
    std::vector<bool> stl_bitmap = std::vector<bool>(num_elements_aligned);
    ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap>(*aligned_bitmap, stl_bitmap, num_elements_aligned);
    for (uint32_t i = 0; i < num_iterations; ++i) {
      auto element = std::uniform_int_distribution(0, static_cast<int>(num_elements_aligned - 1))(generator);
      aligned_bitmap->Flip(element);
      stl_bitmap[element] = !stl_bitmap[element];
      ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap>(*aligned_bitmap, stl_bitmap, num_elements_aligned);
    }

    common::RawBitmap::Deallocate(aligned_bitmap);
  }
}

// Tests a RawBitmap whose base pointer is word aligned, but the num_elements is not a multiple of 8 (byte size)
// NOLINTNEXTLINE
TEST(BitmapTests, NonByteMultipleCorrectnessTest) {
  std::default_random_engine generator;
  // Number of bitmap sizes to test.
  const uint32_t num_bitmap_sizes = 300;
  // Maximum bitmap size multiplier.
  const uint32_t max_size_multiplier = 100;
  // Number of times to randomly permute bitmap
  const uint32_t num_iterations = 100;

  for (uint32_t iter = 0; iter < num_bitmap_sizes; ++iter) {
    // Test a non-byte-multiple sized bitmap
    auto multiplier = std::uniform_int_distribution(1U, max_size_multiplier)(generator);
    auto offset = std::uniform_int_distribution(1U, BYTE_SIZE - 1)(generator);
    const uint32_t num_elements_aligned = 16 * multiplier + offset;

    common::RawBitmap *aligned_bitmap = common::RawBitmap::Allocate(num_elements_aligned);

    // Verify bitmap initialized to all 0s
    for (uint32_t i = 0; i < num_elements_aligned; ++i) {
      EXPECT_FALSE(aligned_bitmap->Test(i));
    }

    // Randomly permute bitmap and STL bitmap and compare equality
    std::vector<bool> stl_bitmap = std::vector<bool>(num_elements_aligned);
    ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap>(*aligned_bitmap, stl_bitmap, num_elements_aligned);
    for (uint32_t i = 0; i < num_iterations; ++i) {
      auto element = std::uniform_int_distribution(0, static_cast<int>(num_elements_aligned - 1))(generator);
      aligned_bitmap->Flip(element);
      stl_bitmap[element] = !stl_bitmap[element];
      ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap>(*aligned_bitmap, stl_bitmap, num_elements_aligned);
    }

    common::RawBitmap::Deallocate(aligned_bitmap);
  }
}

// Tests a RawBitmap whose base pointer is not word aligned
// NOLINTNEXTLINE
TEST(BitmapTests, WordUnalignedCorrectnessTest) {
  std::default_random_engine generator;
  // Number of bitmap sizes to test.
  const uint32_t num_bitmap_sizes = 300;
  // Maximum bitmap size multiplier.
  const uint32_t max_size_multiplier = 100;
  // Number of times to randomly permute bitmap
  const uint32_t num_iterations = 100;

  for (uint32_t iter = 0; iter < num_bitmap_sizes; ++iter) {
    // Test a byte-multiple sized bitmap
    auto multiplier = std::uniform_int_distribution(1U, max_size_multiplier)(generator);
    const uint32_t num_elements = 16 * multiplier;

    // provision enough space for the bitmap elements, plus padding because we're going to make it unaligned to wordsize
    auto size = common::RawBitmap::SizeInBytes(num_elements) + sizeof(uint64_t);
    auto allocated_buffer = new uint8_t[size];
    std::memset(allocated_buffer, 0, size);

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
    ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap>(*unaligned_bitmap, stl_bitmap, num_elements);
    for (uint32_t i = 0; i < num_iterations; ++i) {
      auto element = std::uniform_int_distribution(0, static_cast<int>(num_elements - 1))(generator);
      unaligned_bitmap->Flip(element);
      stl_bitmap[element] = !stl_bitmap[element];
      ContainerTestUtil::CheckReferenceBitmap<common::RawBitmap>(*unaligned_bitmap, stl_bitmap, num_elements);
    }

    delete[] allocated_buffer;
  }
}
}  // namespace noisepage
