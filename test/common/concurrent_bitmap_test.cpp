#include <algorithm>
#include <atomic>
#include <bitset>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"
#include "common/test_util.h"
#include "common/concurrent_bitmap.h"

namespace terrier {
template<uint32_t num_elements>
void CheckReferenceBitmap(const RawConcurrentBitmap &tested,
                          const std::bitset<num_elements> &reference) {
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_EQ(reference[i], tested[i]);
  }
}

TEST(ConcurrentBitmapTests, SimpleCorrectnessTest) {
  const uint32_t num_elements = 16;
  RawConcurrentBitmap *bitmap = RawConcurrentBitmap::Allocate(num_elements);

  // Verify bitmap initialized to all 0s
  for (uint32_t i = 0; i < num_elements; ++i) {
    EXPECT_FALSE(bitmap->Test(i));
  }

  // Randomly permute bitmap and STL bitmap and compare equality
  std::bitset<num_elements> stl_bitmap;
  CheckReferenceBitmap<num_elements>(*bitmap, stl_bitmap);
  uint32_t num_iterations = 32;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < num_iterations; ++i) {
    auto element =
        std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
    EXPECT_TRUE(bitmap->Flip(element, bitmap->Test(element)));
    stl_bitmap.flip(element);
    CheckReferenceBitmap<num_elements>(*bitmap, stl_bitmap);
  }

  // Verify that Flip fails if expected_val doesn't match current value
  auto element =
      std::uniform_int_distribution(0, (int) num_elements - 1)(generator);
  EXPECT_FALSE(bitmap->Flip(element, !bitmap->Test(element)));
  RawConcurrentBitmap::Deallocate(bitmap);
}
// The test attempts to concurrently flip every bit from 0 to 1, and
// record successful flips into thread-local storage
// This is equivalent to grabbing a free slot if used in an
// allocator
TEST(ConcurrentBitmapTests, ConcurrentCorrectnessTest) {
  const uint32_t num_elements = 1000000;
  const uint32_t num_threads = 8;
  RawConcurrentBitmap *bitmap = RawConcurrentBitmap::Allocate(num_elements);
  std::vector<std::vector<uint32_t>> elements(num_threads);

  auto workload = [&](uint32_t thread_id) {
    for (uint32_t i = 0; i < num_elements; ++i)
      if (bitmap->Flip(i, false)) elements[thread_id].push_back(i);
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
  RawConcurrentBitmap::Deallocate(bitmap);
}
}