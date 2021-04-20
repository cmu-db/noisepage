#include "common/container/chunked_array.h"

#include <random>
#include <vector>

#include "gtest/gtest.h"

namespace noisepage {

template <int chunk>
void FillMergeSize(size_t size1, size_t size2) {
  std::default_random_engine generator;
  auto dist = std::uniform_int_distribution(1, 1000);

  common::ChunkedArray<int, chunk> array1;
  common::ChunkedArray<int, chunk> array2;
  std::vector<int> vec;
  for (size_t i = 0; i < size1; i++) {
    int num = dist(generator);
    vec.push_back(num);
    array1.Push(std::move(num));  // NOLINT
  }

  for (size_t i = 0; i < size2; i++) {
    int num = dist(generator);
    vec.push_back(num);
    array2.Push(std::move(num));  // NOLINT
  }

  array1.Merge(&array2);

  size_t idx = 0;
  auto it = array1.begin();
  while (it != array1.end()) {
    EXPECT_EQ(vec[idx], *it);
    it++;
    idx++;
  }

  EXPECT_EQ(idx, size1 + size2);
}

// NOLINTNEXTLINE
TEST(ChunkedArrayTests, InsertSizes) {
  FillMergeSize<16>(0, 0);
  FillMergeSize<16>(8, 0);
  FillMergeSize<16>(16, 0);
  FillMergeSize<16>(32, 0);
}

// NOLINTNEXTLINE
TEST(ChunkedArrayTests, Merge) {
  FillMergeSize<16>(0, 8);
  FillMergeSize<16>(0, 16);
  FillMergeSize<16>(8, 8);
  FillMergeSize<16>(8, 16);
  FillMergeSize<16>(16, 1);
  FillMergeSize<16>(16, 16);
}

}  // namespace noisepage
