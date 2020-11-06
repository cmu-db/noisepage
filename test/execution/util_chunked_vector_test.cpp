#include <algorithm>
#include <deque>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"
#include "execution/util/chunked_vector.h"
#include "ips4o/ips4o.hpp"

namespace noisepage::execution::util::test {

class ChunkedVectorTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, InsertAndIndexTest) {
  const uint32_t num_elems = 10;

  ChunkedVectorT<uint32_t> vec;

  EXPECT_TRUE(vec.empty());

  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  EXPECT_FALSE(vec.empty());
  EXPECT_EQ(num_elems, vec.size());
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomLookupTest) {
  const uint32_t num_elems = 1000;

  ChunkedVectorT<uint32_t> vec;

  EXPECT_TRUE(vec.empty());

  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Do a bunch of random lookup
  std::random_device random;
  for (uint32_t i = 0; i < 1000; i++) {
    auto idx = random() % num_elems;
    EXPECT_EQ(idx, vec[idx]);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, IterationTest) {
  ChunkedVectorT<uint32_t> vec;

  for (uint32_t i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  {
    uint32_t i = 0;
    for (auto x : vec) {
      EXPECT_EQ(i++, x);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, PopBackTest) {
  ChunkedVectorT<uint32_t> vec;

  for (uint32_t i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  vec.pop_back();
  EXPECT_EQ(9u, vec.size());

  vec.pop_back();
  EXPECT_EQ(8u, vec.size());

  for (uint32_t i = 0; i < vec.size(); i++) {
    EXPECT_EQ(i, vec[i]);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, FrontBackTest) {
  ChunkedVectorT<uint32_t> vec;

  for (uint32_t i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  EXPECT_EQ(0u, vec.front());
  EXPECT_EQ(9u, vec.back());

  vec.front() = 44;
  vec.back() = 100;

  EXPECT_EQ(44u, vec[0]);
  EXPECT_EQ(100u, vec[9]);

  vec.pop_back();
  EXPECT_EQ(8u, vec.back());
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, ChunkReuseTest) {
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t, StlRegionAllocator<uint32_t>> vec{StlRegionAllocator<uint32_t>(&tmp)};

  for (uint32_t i = 0; i < 1000; i++) {
    vec.push_back(i);
  }

  EXPECT_EQ(1000u, vec.size());
  EXPECT_EQ(999u, vec.back());

  // Track memory allocation after all data inserted
  auto allocated_1 = tmp.Allocated();

  for (uint32_t i = 0; i < 1000; i++) {
    vec.pop_back();
  }

  // Pops shouldn't allocated memory
  auto allocated_2 = tmp.Allocated();
  EXPECT_EQ(0u, allocated_2 - allocated_1);

  for (uint32_t i = 0; i < 1000; i++) {
    vec.push_back(i);
  }

  // The above pushes should reuse chunks, i.e., no allocations
  auto allocated_3 = tmp.Allocated();
  EXPECT_EQ(0u, allocated_3 - allocated_2);
}

// Object that allocates 20-bytes of data. Used to ensure objects pushed into vector are destroyed.
class Simple {
 public:
  // Not thread-safe!
  static uint32_t count;

  explicit Simple(uint32_t _id) : id_(_id), ptr_(std::make_unique<char[]>(20)) { count++; }

  ~Simple() { count--; }

  uint32_t Id() const noexcept { return id_; }

 private:
  uint32_t id_;
  std::unique_ptr<char[]> ptr_;
};

uint32_t Simple::count = 0;

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, ClearTest) {
  ChunkedVectorT<Simple> v;
  v.emplace_back(1);
  v.emplace_back(2);
  v.emplace_back(3);

  EXPECT_EQ(3u, v.size());
  EXPECT_EQ(1u, v[0].Id());
  EXPECT_EQ(2u, v[1].Id());
  EXPECT_EQ(3u, v[2].Id());

  v.clear();

  EXPECT_EQ(0u, v.size());

  v.emplace_back(10);
  v.emplace_back(11);
  v.emplace_back(12);

  EXPECT_EQ(3u, v.size());
  EXPECT_EQ(10u, v[0].Id());
  EXPECT_EQ(11u, v[1].Id());
  EXPECT_EQ(12u, v[2].Id());
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, ElementConstructDestructTest) {
  util::Region tmp("tmp");
  ChunkedVectorT<Simple> vec;

  for (uint32_t i = 0; i < 1000; i++) {
    vec.emplace_back(i);
  }
  EXPECT_EQ(1000u, Simple::count);

  vec.pop_back();
  EXPECT_EQ(999u, Simple::count);

  for (uint32_t i = 0; i < 999; i++) {
    vec.pop_back();
  }
  EXPECT_EQ(0u, Simple::count);
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, MoveConstructorTest) {
  const uint32_t num_elems = 1000;

  // Populate vec1
  ChunkedVectorT<Simple> vec1;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec1.emplace_back(i);
  }

  ChunkedVectorT<Simple> vec2(std::move(vec1));
  EXPECT_EQ(num_elems, vec2.size());
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, AssignmentMoveTest) {
  const uint32_t num_elems = 1000;

  // Populate vec1
  ChunkedVectorT<Simple> vec1;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec1.emplace_back(i);
  }

  ChunkedVectorT<Simple> vec2;
  EXPECT_EQ(0u, vec2.size());

  // Move vec1 into vec2
  vec2 = std::move(vec1);
  EXPECT_EQ(num_elems, vec2.size());
}

// Check that adding random integers to the iterator works.
// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorAdditionTest) {
  const uint32_t num_elems = 1000;
  const uint32_t num_rolls = 1000000;  // Number of additions to make
  // Create vector
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) vec.push_back(i);

  // Jump at random offsets within the vector.
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(0, num_elems - 1);
  auto iter = vec.begin();
  int32_t prev_idx = 0;
  for (uint32_t i = 0; i < num_rolls; i++) {
    int32_t new_idx = distribution(generator);
    iter += (new_idx - prev_idx);
    ASSERT_EQ(*iter, static_cast<uint32_t>(new_idx));
    prev_idx = new_idx;
  }
}

// Check that subtracting random integers from the iterator works.
// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorSubtractionTest) {
  const uint32_t num_elems = 1000;
  const uint32_t num_rolls = 1000000;  // number of subtractions to make
  // Create vector
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) vec.push_back(i);

  // Jump at random offsets within the vector
  std::default_random_engine generator;
  std::uniform_int_distribution<int32_t> distribution(0, num_elems - 1);
  auto iter = vec.begin();
  int32_t prev_idx = 0;
  for (uint32_t i = 0; i < num_rolls; i++) {
    int32_t new_idx = distribution(generator);
    iter -= (prev_idx - new_idx);
    ASSERT_EQ(*iter, static_cast<uint32_t>(new_idx));
    prev_idx = new_idx;
  }
}

// Check that all binary operators are working.
// <, <=, >, >=, ==, !=, -.
// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorBinaryOpsTest) {
  const uint32_t num_elems = 1000;
  const uint32_t num_rolls = 1000000;  // Number of checks to make
  // Create vector
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) vec.push_back(i);

  // Perform binary operations on random indices.
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(0, num_elems - 1);
  for (uint32_t i = 0; i < num_rolls; i++) {
    int32_t idx1 = distribution(generator);
    int32_t idx2 = distribution(generator);
    auto iter1 = vec.begin() + idx1;
    auto iter2 = vec.begin() + idx2;
    ASSERT_EQ(idx1 < idx2, iter1 < iter2);
    ASSERT_EQ(idx1 > idx2, iter1 > iter2);
    ASSERT_EQ(idx1 <= idx2, iter1 <= iter2);
    ASSERT_EQ(idx1 >= idx2, iter1 >= iter2);
    ASSERT_EQ(idx1 == idx2, iter1 == iter2);
    ASSERT_EQ(idx1 != idx2, iter1 != iter2);
    ASSERT_EQ(idx1 - idx2, iter1 - iter2);
    ASSERT_EQ(idx2 - idx1, iter2 - iter1);
  }
}

// Check that pre-incrementing works.
// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorPreIncrementTest) {
  const uint32_t num_elems = 512;

  // Generate random elements
  std::vector<uint32_t> std_vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    std_vec.push_back(i);
  }

  std::default_random_engine generator;
  std::shuffle(std_vec.begin(), std_vec.end(), generator);

  // Create chunked vector
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(std_vec[i]);
  }

  auto iter = vec.begin();
  for (uint32_t i = 0; i < num_elems; i++) {
    ASSERT_EQ(*iter, std_vec[i]);
    ++iter;
  }
}

// Check that pre-decrementing works.
// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorPreDecrementTest) {
  const uint32_t num_elems = 512;

  // Generate random elements
  std::vector<uint32_t> std_vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    std_vec.push_back(i);
  }

  std::default_random_engine generator;
  std::shuffle(std_vec.begin(), std_vec.end(), generator);

  // Create chunked vector
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;
  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(std_vec[i]);
  }

  auto iter = vec.end();
  for (uint32_t i = 0; i < num_elems; i++) {
    --iter;
    ASSERT_EQ(*iter, std_vec[num_elems - i - 1]);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, SortTest) {
  const uint32_t num_elems = 1000;
  util::Region tmp("tmp");
  ChunkedVectorT<uint32_t> vec;

  // Insert elements
  for (uint32_t i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Shuffle
  std::default_random_engine generator;
  std::shuffle(vec.begin(), vec.end(), generator);

  // Sort
  ips4o::sort(vec.begin(), vec.end());

  // Verify
  for (uint32_t i = 0; i < num_elems; i++) {
    ASSERT_EQ(vec[i], i);
  }
}

}  // namespace noisepage::execution::util::test
