#include <algorithm>
#include <atomic>
#include <bitset>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"
#include "util/multi_threaded_test_util.h"
#include "common/concurrent_bitmap.h"
#include "util/container_test_util.h"

namespace terrier {

TEST(ConcurrentBitmapTests, SimpleCorrectnessTest) {
  const uint32_t num_elements = 16;
  common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_FALSE(bitmap->Test(i));
  }

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements> stl_bitmap;
  ContainerTestUtil::CheckReferenceBitmap<common::RawConcurrentBitmap, num_elements>(*bitmap, stl_bitmap);
  uint32_t num_iterations = 32;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
    EXPECT_TRUE(bitmap->Flip(element, bitmap->Test(element)));
    stl_bitmap.flip(element);
    ContainerTestUtil::CheckReferenceBitmap<common::RawConcurrentBitmap, num_elements>(*bitmap, stl_bitmap);
  }

  // Verify that Flip fails if expected_val doesn't match current value
  auto element =
      std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
  EXPECT_FALSE(bitmap->Flip(element, !bitmap->Test(element)));
  common::RawConcurrentBitmap::Deallocate(bitmap);
}
// The test exercises FirstUnsetPos in a single-threaded context
TEST(ConcurrentBitmapTests, FirstUnsetPosTest) {
  const uint32_t num_elements = 18;
  common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
  uint32_t pos;

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

  // try to find specific unset bits, x = set, _ = unset
  uint32_t flip_idx[3] = {5, 12, 13};
  // x _ x should return middle
  EXPECT_TRUE(bitmap->Flip(flip_idx[1], true));
  EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
  EXPECT_EQ(pos, flip_idx[1]);
  // _ _ x should return first
  EXPECT_TRUE(bitmap->Flip(flip_idx[0], true));
  EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
  EXPECT_EQ(pos, flip_idx[0]);
  // _ _ x should return middle if searching from middle
  EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 11, &pos));
  EXPECT_EQ(pos, flip_idx[1]);
  EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 12, &pos));
  EXPECT_EQ(pos, flip_idx[1]);
  // x x _ should return last
  EXPECT_TRUE(bitmap->Flip(flip_idx[0], false));
  EXPECT_TRUE(bitmap->Flip(flip_idx[1], false));
  EXPECT_TRUE(bitmap->Flip(flip_idx[2], true));
  EXPECT_TRUE(bitmap->FirstUnsetPos(num_elements, 0, &pos));
  EXPECT_EQ(pos, flip_idx[2]);

  common::RawConcurrentBitmap::Deallocate(bitmap);
}
// The test exercises FirstUnsetPos with edge-case sizes
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
TEST(ConcurrentBitmapTests, ConcurrentFirstUnsetPosTest) {
  const uint32_t num_elements = 10000;
  const uint32_t num_threads = 8;
  common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
  std::vector<std::vector<uint32_t>> elements(num_threads);

  auto workload = [&](uint32_t thread_id) {
    uint32_t pos = 0;
    for (uint32_t i = 0; i < num_elements; ++i) {
      if (bitmap->FirstUnsetPos(num_elements, 0, &pos)) {
        if (bitmap->Flip(pos, false)) {
          elements[thread_id].push_back(pos);
        }
      }
    }
  };

  testutil::RunThreadsUntilFinish(num_threads, workload);

  // Coalesce the thread-local result vectors into one vector, and
  // then sort the results
  std::vector<uint32_t> all_elements;
  for (uint32_t i = 0; i < num_threads; ++i)
    all_elements.insert(all_elements.end(),
                        elements[i].begin(),
                        elements[i].end());

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

// The test attempts to concurrently flip every bit from 0 to 1, and
// record successful flips into thread-local storage
// This is equivalent to grabbing a free slot if used in an
// allocator
TEST(ConcurrentBitmapTests, ConcurrentCorrectnessTest) {
  const uint32_t num_elements = 1000000;
  const uint32_t num_threads = 8;
  common::RawConcurrentBitmap *bitmap = common::RawConcurrentBitmap::Allocate(num_elements);
  std::vector<std::vector<uint32_t>> elements(num_threads);

  auto workload = [&](uint32_t thread_id) {
    for (uint32_t i = 0; i < num_elements; ++i)
      if (bitmap->Flip(i, false)) elements[thread_id].push_back(i);
  };

  MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads, workload);

  // Coalesce the thread-local result vectors into one vector, and
  // then sort the results
  std::vector<uint32_t> all_elements;
  for (uint32_t i = 0; i < num_threads; ++i)
    all_elements.insert(all_elements.end(),
                        elements[i].begin(),
                        elements[i].end());

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