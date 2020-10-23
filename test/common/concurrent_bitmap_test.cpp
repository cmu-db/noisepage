#include "common/container/concurrent_bitmap.h"

#include <algorithm>
#include <atomic>
#include <bitset>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "test_util/container_test_util.h"
#include "test_util/multithread_test_util.h"

namespace noisepage {

// Tests that the ConcurrentBitmap works in a single-threaded context
// NOLINTNEXTLINE
TEST(ConcurrentBitmapTests, SimpleCorrectnessTest) {
  std::default_random_engine generator;
  // Number of bitmap sizes to test.
  const uint32_t num_bitmap_sizes = 50;
  // Maximum bitmap size.
  const uint32_t max_bitmap_size = 1000;

  for (uint32_t iter = 0; iter < num_bitmap_sizes; ++iter) {
    auto num_elements = std::uniform_int_distribution(1U, max_bitmap_size)(generator);
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);

    // Verify bitmap initialized to all 0s
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_FALSE(bitmap->Test(i));
    }

    // Randomly permute bitmap and STL bitmap and compare equality
    std::vector<bool> stl_bitmap = std::vector<bool>(num_elements);
    ContainerTestUtil::CheckReferenceBitmap<common::RawConcurrentBitmap>(*bitmap, stl_bitmap, num_elements);
    uint32_t num_iterations = 32;
    std::default_random_engine generator;
    for (uint32_t i = 0; i < num_iterations; ++i) {
      auto element = std::uniform_int_distribution(0, static_cast<int>(num_elements - 1))(generator);
      EXPECT_TRUE(bitmap->Flip(element, bitmap->Test(element)));
      stl_bitmap[element] = !stl_bitmap[element];
      ContainerTestUtil::CheckReferenceBitmap<common::RawConcurrentBitmap>(*bitmap, stl_bitmap, num_elements);
    }

    // Verify that Flip fails if expected_val doesn't match current value
    auto element = std::uniform_int_distribution(0, static_cast<int>(num_elements - 1))(generator);
    EXPECT_FALSE(bitmap->Flip(element, !bitmap->Test(element)));
    common::RawConcurrentBitmap::Deallocate(bitmap);
  }
}
// The test exercises FirstUnsetPos in a single-threaded context
// NOLINTNEXTLINE
TEST(ConcurrentBitmapTests, FirstUnsetPosTest) {
  std::default_random_engine generator;
  // Number of bitmap sizes to test.
  const uint32_t num_bitmap_sizes = 50;
  // Maximum bitmap size.
  const uint32_t max_bitmap_size = 1000;
  uint32_t pos = 0;

  // test a wide range of sizes
  for (uint32_t iter = 0; iter < num_bitmap_sizes; ++iter) {
    auto num_elements = std::uniform_int_distribution(1U, max_bitmap_size)(generator);
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);

    // should return false if we start searching out of range
    EXPECT_FALSE(bitmap->FirstUnsetPos(num_elements, num_elements, &pos));
    EXPECT_FALSE(bitmap->FirstUnsetPos(num_elements, num_elements + 1, &pos));

    // as we flip bits from start to end, verify that the position of the next unset pos is correct
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
      EXPECT_EQ(pos, i);
      EXPECT_TRUE(bitmap->Flip(i, false));
    }
    // once the bitmap is full, we should not be able to find an unset bit
    EXPECT_FALSE(bitmap->FirstUnsetPos(num_elements, 0, &pos));

    common::RawConcurrentBitmap::Deallocate(bitmap);
  }

  // manual targeted test for specific bits
  {
    const uint32_t num_elements = 16;
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);

    // x = set, _ = unset
    // fill everything, resulting in x x x
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_TRUE(bitmap->Flip(i, false));
    }

    // once the bitmap is full, we should not be able to find an unset bit
    EXPECT_FALSE(bitmap->FirstUnsetPos(num_elements, 0, &pos));

    // try to find specific unset bits, x = set, _ = unset
    uint32_t flip_idx[3] = {5, 12, 13};
    // note: there are more than 3 bits
    // x _ x should return middle
    EXPECT_TRUE(bitmap->Flip(flip_idx[1], true));
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
    EXPECT_EQ(pos, flip_idx[1]);
    // _ _ x should return first
    EXPECT_TRUE(bitmap->Flip(flip_idx[0], true));
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
    EXPECT_EQ(pos, flip_idx[0]);
    // _ _ x should return middle if searching from middle
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, flip_idx[1] - 1, &pos));
    EXPECT_EQ(pos, flip_idx[1]);
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, flip_idx[1], &pos));
    EXPECT_EQ(pos, flip_idx[1]);
    // x x _ should return last
    EXPECT_TRUE(bitmap->Flip(flip_idx[0], false));
    EXPECT_TRUE(bitmap->Flip(flip_idx[1], false));
    EXPECT_TRUE(bitmap->Flip(flip_idx[2], true));
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
    EXPECT_EQ(pos, flip_idx[2]);
    // x _ _, note we expect idx [1] and [2] to be part of the same word
    EXPECT_TRUE(bitmap->Flip(flip_idx[1], true));
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, flip_idx[2], &pos));
    EXPECT_EQ(pos, flip_idx[2]);

    common::RawConcurrentBitmap::Deallocate(bitmap);
  }
}
// The test exercises FirstUnsetPos with edge-case sizes
// NOLINTNEXTLINE
TEST(ConcurrentBitmapTests, FirstUnsetPosSizeTest) {
  // fill an entire block of 16, then try to find an unset bit
  {
    const uint32_t num_elements = 16;
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
    uint32_t pos;

    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_TRUE(bitmap->Flip(i, false));
    }
    EXPECT_FALSE(bitmap->FirstUnsetPos(num_elements, 0, &pos));

    common::RawConcurrentBitmap::Deallocate(bitmap);
  }
  // fill the first 128, then try to find unset bit 129
  // meant to test if we are reading out of bounds (naively reading 64+64+64 should trigger ASAN)
  {
    const uint32_t num_elements = 129;
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
    uint32_t pos;

    // flip everything but the last bit
    for (uint32_t i = 0; i < num_elements - 1; ++i) {
      EXPECT_TRUE(bitmap->Flip(i, false));
    }
    EXPECT_TRUE(bitmap->Test(0));
    // expect the last bit to be ok
    EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
    EXPECT_EQ(pos, 128);

    common::RawConcurrentBitmap::Deallocate(bitmap);
  }
}
// The test attempts to concurrently flip every bit from 0 to 1 using FirstUnsetPos
// NOLINTNEXTLINE
TEST(ConcurrentBitmapTests, ConcurrentFirstUnsetPosTest) {
  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t max_elements = 10000;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    const uint32_t num_elements = std::uniform_int_distribution(1U, max_elements)(generator);
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
    std::vector<std::vector<uint32_t>> elements(num_threads);

    auto workload = [&](uint32_t thread_id) {
      uint32_t pos = 0;
      for (uint32_t i = 0; i < num_elements; ++i) {
        if (bitmap->FirstUnsetPos(num_elements, 0, &pos) && bitmap->Flip(pos, false)) {
          elements[thread_id].push_back(pos);
        }
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    // Coalesce the thread-local result vectors into one vector, and
    // then sort the results
    std::vector<uint32_t> all_elements;
    for (uint32_t i = 0; i < num_threads; ++i)
      all_elements.insert(all_elements.end(), elements[i].begin(), elements[i].end());

    // Verify coalesced result size
    EXPECT_EQ(num_elements, all_elements.size());
    std::sort(all_elements.begin(), all_elements.end());
    // Verify 1:1 mapping of indices to element value
    // This represents that every slot was grabbed by only one thread
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_EQ(i, all_elements[i]);
    }
    common::RawConcurrentBitmap::Deallocate(bitmap);
  }
}

// The test attempts to concurrently flip every bit from 0 to 1, and
// record successful flips into thread-local storage
// This is equivalent to grabbing a free slot if used in an allocator
// NOLINTNEXTLINE
TEST(ConcurrentBitmapTests, ConcurrentCorrectnessTest) {
  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t max_elements = 100000;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    const uint32_t num_elements = std::uniform_int_distribution(1U, max_elements)(generator);
    common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
    std::vector<std::vector<uint32_t>> elements(num_threads);

    auto workload = [&](uint32_t thread_id) {
      for (uint32_t i = 0; i < num_elements; ++i)
        if (bitmap->Flip(i, false)) elements[thread_id].push_back(i);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    // Coalesce the thread-local result vectors into one vector, and
    // then sort the results
    std::vector<uint32_t> all_elements;
    for (uint32_t i = 0; i < num_threads; ++i)
      all_elements.insert(all_elements.end(), elements[i].begin(), elements[i].end());

    // Verify coalesced result size
    EXPECT_EQ(num_elements, all_elements.size());
    std::sort(all_elements.begin(), all_elements.end());
    // Verify 1:1 mapping of indices to element value
    // This represents that every slot was grabbed by only one thread
    for (uint32_t i = 0; i < num_elements; ++i) {
      EXPECT_EQ(i, all_elements[i]);
    }
    common::RawConcurrentBitmap::Deallocate(bitmap);
  }
}
}  // namespace noisepage
