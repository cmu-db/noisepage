#include "common/container/chunked_array.h"

#include <cstring>
#include <random>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

namespace noisepage {

template <int chunk>
void fill_merge_size(size_t size1, size_t size2) {
  std::default_random_engine generator;
  auto dist = std::uniform_int_distribution(1, 1000);

  common::ChunkedArray<int, chunk> array1;
  common::ChunkedArray<int, chunk> array2;
  std::vector<int> vec;
  for (size_t i = 0; i < size1; i++) {
    int num = dist(generator);
    vec.push_back(num);
    array1.push(std::move(num));
  }

  for (size_t i = 0; i < size2; i++) {
    int num = dist(generator);
    vec.push_back(num);
    array2.push(std::move(num));
  }

  array1.merge(array2);

  size_t idx = 0;
  auto it = array1.begin();
  while (it != array1.end()) {
    ASSERT_EQ(vec[idx], *it);
    it++;
    idx++;
  }

  ASSERT_EQ(idx, size1 + size2);
}

// NOLINTNEXTLINE
TEST(ChunkedArrayTests, InsertSizes) {
  fill_merge_size<16>(0, 0);
  fill_merge_size<16>(8, 0);
  fill_merge_size<16>(16, 0);
  fill_merge_size<16>(32, 0);
}

// NOLINTNEXTLINE
TEST(ChunkedArrayTests, Merge) {
  fill_merge_size<16>(0, 8);
  fill_merge_size<16>(0, 16);
  fill_merge_size<16>(8, 8);
  fill_merge_size<16>(8, 16);
  fill_merge_size<16>(16, 1);
  fill_merge_size<16>(16, 16);
}

}  // namespace noisepage
