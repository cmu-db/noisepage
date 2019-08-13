#include <algorithm>
#include <deque>
#include <random>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/util/chunked_vector.h"
#include "ips4o/ips4o.hpp"

namespace terrier::execution::util::test {

class ChunkedVectorTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, InsertAndIndexTest) {
  const u32 num_elems = 10;

  ChunkedVectorT<u32> vec;

  EXPECT_TRUE(vec.empty());

  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  EXPECT_FALSE(vec.empty());
  EXPECT_EQ(num_elems, vec.size());
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomLookupTest) {
  const u32 num_elems = 1000;

  ChunkedVectorT<u32> vec;

  EXPECT_TRUE(vec.empty());

  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Do a bunch of random lookup
  std::random_device random;
  for (u32 i = 0; i < 1000; i++) {
    auto idx = random() % num_elems;
    EXPECT_EQ(idx, vec[idx]);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, IterationTest) {
  ChunkedVectorT<u32> vec;

  for (u32 i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  {
    u32 i = 0;
    for (auto x : vec) {
      EXPECT_EQ(i++, x);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, PopBackTest) {
  ChunkedVectorT<u32> vec;

  for (u32 i = 0; i < 10; i++) {
    vec.push_back(i);
  }

  vec.pop_back();
  EXPECT_EQ(9u, vec.size());

  vec.pop_back();
  EXPECT_EQ(8u, vec.size());

  for (u32 i = 0; i < vec.size(); i++) {
    EXPECT_EQ(i, vec[i]);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, FrontBackTest) {
  ChunkedVectorT<u32> vec;

  for (u32 i = 0; i < 10; i++) {
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
  ChunkedVectorT<u32, StlRegionAllocator<u32>> vec{StlRegionAllocator<u32>(&tmp)};

  for (u32 i = 0; i < 1000; i++) {
    vec.push_back(i);
  }

  EXPECT_EQ(1000u, vec.size());
  EXPECT_EQ(999u, vec.back());

  // Track memory allocation after all data inserted
  auto allocated_1 = tmp.allocated();

  for (u32 i = 0; i < 1000; i++) {
    vec.pop_back();
  }

  // Pops shouldn't allocated memory
  auto allocated_2 = tmp.allocated();
  EXPECT_EQ(0u, allocated_2 - allocated_1);

  for (u32 i = 0; i < 1000; i++) {
    vec.push_back(i);
  }

  // The above pushes should reuse chunks, i.e., no allocations
  auto allocated_3 = tmp.allocated();
  EXPECT_EQ(0u, allocated_3 - allocated_2);
}

struct Simple {
  // Not thread-safe!
  static u32 count;
  Simple() { count++; }
  ~Simple() { count--; }
};

u32 Simple::count = 0;

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, ElementConstructDestructTest) {
  util::Region tmp("tmp");
  ChunkedVectorT<Simple> vec;

  for (u32 i = 0; i < 1000; i++) {
    vec.emplace_back();
  }
  EXPECT_EQ(1000u, Simple::count);

  vec.pop_back();
  EXPECT_EQ(999u, Simple::count);

  for (u32 i = 0; i < 999; i++) {
    vec.pop_back();
  }
  EXPECT_EQ(0u, Simple::count);
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, MoveConstructorTest) {
  const u32 num_elems = 1000;

  // Populate vec1
  ChunkedVectorT<u32> vec1;
  for (u32 i = 0; i < num_elems; i++) {
    vec1.push_back(i);
  }

  ChunkedVectorT<u32> vec2(std::move(vec1));
  EXPECT_EQ(num_elems, vec2.size());
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, AssignmentMoveTest) {
  const u32 num_elems = 1000;

  // Populate vec1
  ChunkedVectorT<u32> vec1;
  for (u32 i = 0; i < num_elems; i++) {
    vec1.push_back(i);
  }

  ChunkedVectorT<u32> vec2;
  EXPECT_EQ(0u, vec2.size());

  // Move vec1 into vec2
  vec2 = std::move(vec1);
  EXPECT_EQ(num_elems, vec2.size());
}

// Check that adding random integers to the iterator works.

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorAdditionTest) {
  const u32 num_elems = 1000;
  const u32 num_rolls = 1000000;  // Number of additions to make
  // Create vector
  ChunkedVectorT<u32> vec;
  for (u32 i = 0; i < num_elems; i++) vec.push_back(i);

  // Jump at random offsets within the vector.
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(0, num_elems - 1);
  auto iter = vec.begin();
  i32 prev_idx = 0;
  for (u32 i = 0; i < num_rolls; i++) {
    i32 new_idx = distribution(generator);
    iter += (new_idx - prev_idx);
    ASSERT_EQ(*iter, static_cast<u32>(new_idx));
    prev_idx = new_idx;
  }
}

// Check that subtracting random integers from the iterator works.

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorSubtractionTest) {
  const u32 num_elems = 1000;
  const u32 num_rolls = 1000000;  // number of subtractions to make
  // Create vector
  ChunkedVectorT<u32> vec;
  for (u32 i = 0; i < num_elems; i++) vec.push_back(i);

  // Jump at random offsets within the vector
  std::default_random_engine generator;
  std::uniform_int_distribution<i32> distribution(0, num_elems - 1);
  auto iter = vec.begin();
  i32 prev_idx = 0;
  for (u32 i = 0; i < num_rolls; i++) {
    i32 new_idx = distribution(generator);
    iter -= (prev_idx - new_idx);
    ASSERT_EQ(*iter, static_cast<u32>(new_idx));
    prev_idx = new_idx;
  }
}

// Check that all binary operators are working.
// <, <=, >, >=, ==, !=, -.

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorBinaryOpsTest) {
  const u32 num_elems = 1000;
  const u32 num_rolls = 1000000;  // Number of checks to make
  // Create vector
  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec;
  for (u32 i = 0; i < num_elems; i++) vec.push_back(i);

  // Perform binary operations on random indices.
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(0, num_elems - 1);
  for (u32 i = 0; i < num_rolls; i++) {
    i32 idx1 = distribution(generator);
    i32 idx2 = distribution(generator);
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
  const u32 num_elems = 512;

  // Generate random elements
  std::vector<u32> std_vec;
  for (u32 i = 0; i < num_elems; i++) {
    std_vec.push_back(i);
  }

  std::default_random_engine generator;
  std::shuffle(std_vec.begin(), std_vec.end(), generator);

  // Create chunked vector
  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec;
  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(std_vec[i]);
  }

  auto iter = vec.begin();
  for (u32 i = 0; i < num_elems; i++) {
    ASSERT_EQ(*iter, std_vec[i]);
    ++iter;
  }
}

// Check that pre-decrementing works.

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, RandomIteratorPreDecrementTest) {
  const u32 num_elems = 512;

  // Generate random elements
  std::vector<u32> std_vec;
  for (u32 i = 0; i < num_elems; i++) {
    std_vec.push_back(i);
  }

  std::default_random_engine generator;
  std::shuffle(std_vec.begin(), std_vec.end(), generator);

  // Create chunked vector
  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec;
  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(std_vec[i]);
  }

  auto iter = vec.end();
  for (u32 i = 0; i < num_elems; i++) {
    --iter;
    ASSERT_EQ(*iter, std_vec[num_elems - i - 1]);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, SortTest) {
  const u32 num_elems = 1000;
  util::Region tmp("tmp");
  ChunkedVectorT<u32> vec;

  // Insert elements
  for (u32 i = 0; i < num_elems; i++) {
    vec.push_back(i);
  }

  // Shuffle
  std::default_random_engine generator;
  std::shuffle(vec.begin(), vec.end(), generator);

  // Sort
  ips4o::sort(vec.begin(), vec.end());

  // Verify
  for (u32 i = 0; i < num_elems; i++) {
    ASSERT_EQ(vec[i], i);
  }
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, DISABLED_PerfInsertTest) {
  auto stdvec_ms = Bench(3, []() {
    util::Region tmp("tmp");
    std::vector<u32, StlRegionAllocator<u32>> v{StlRegionAllocator<u32>(&tmp)};
    for (u32 i = 0; i < 10000000; i++) {
      v.push_back(i);
    }
  });

  auto stddeque_ms = Bench(3, []() {
    util::Region tmp("tmp");
    std::deque<u32, StlRegionAllocator<u32>> v{StlRegionAllocator<u32>(&tmp)};
    for (u32 i = 0; i < 10000000; i++) {
      v.push_back(i);
    }
  });

  auto chunked_ms = Bench(3, []() {
    util::Region tmp("tmp");
    ChunkedVectorT<u32, StlRegionAllocator<u32>> v{util::StlRegionAllocator<u32>(&tmp)};
    for (u32 i = 0; i < 10000000; i++) {
      v.push_back(i);
    }
  });

  std::cout << std::fixed << std::setprecision(4);
  std::cout << "std::vector  : " << stdvec_ms << " ms" << std::endl;
  std::cout << "std::deque   : " << stddeque_ms << " ms" << std::endl;
  std::cout << "ChunkedVector: " << chunked_ms << " ms" << std::endl;
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, DISABLED_PerfScanTest) {
  static const u32 num_elems = 10000000;

  util::Region tmp("vec"), tmp2("deque"), tmp3("chunk");
  std::vector<u32, StlRegionAllocator<u32>> stdvec{StlRegionAllocator<u32>(&tmp)};
  std::deque<u32, StlRegionAllocator<u32>> stddeque{StlRegionAllocator<u32>(&tmp2)};
  ChunkedVectorT<u32, StlRegionAllocator<u32>> chunkedvec{util::StlRegionAllocator<u32>(&tmp3)};
  for (u32 i = 0; i < num_elems; i++) {
    stdvec.push_back(i);
    stddeque.push_back(i);
    chunkedvec.push_back(i);
  }

  auto stdvec_ms = Bench(10, [&stdvec]() {
    u32 c = 0;
    for (auto x : stdvec) {
      c += x;
    }
    stdvec[0] = c;
  });

  auto stddeque_ms = Bench(10, [&stddeque]() {
    u32 c = 0;
    for (auto x : stddeque) {
      c += x;
    }
    stddeque[0] = c;
  });

  auto chunked_ms = Bench(10, [&chunkedvec]() {
    u32 c = 0;
    for (auto x : chunkedvec) {
      c += x;
    }
    chunkedvec[0] = c;
  });

  std::cout << std::fixed << std::setprecision(4);
  std::cout << "std::vector  : " << stdvec_ms << " ms" << std::endl;
  std::cout << "std::deque   : " << stddeque_ms << " ms" << std::endl;
  std::cout << "ChunkedVector: " << chunked_ms << " ms" << std::endl;
}

// NOLINTNEXTLINE
TEST_F(ChunkedVectorTest, DISABLED_PerfRandomAccessTest) {
  static const u32 num_elems = 10000000;
  std::default_random_engine generator;
  std::uniform_int_distribution<u32> rng(0, num_elems);

  util::Region tmp("vec"), tmp2("deque"), tmp3("chunk");
  std::vector<u32, StlRegionAllocator<u32>> stdvec{StlRegionAllocator<u32>(&tmp)};
  std::deque<u32, StlRegionAllocator<u32>> stddeque{StlRegionAllocator<u32>(&tmp2)};
  ChunkedVectorT<u32, StlRegionAllocator<u32>> chunkedvec{util::StlRegionAllocator<u32>(&tmp3)};
  for (u32 i = 0; i < num_elems; i++) {
    stdvec.push_back(i % 4);
    stddeque.push_back(i % 4);
    chunkedvec.push_back(i % 4);
  }

  std::vector<u32> random_indexes(num_elems);
  for (u32 i = 0; i < num_elems; i++) {
    random_indexes[i] = rng(generator);
  }

  auto stdvec_ms = Bench(10, [&stdvec, &random_indexes]() {
    u32 c = 0;
    for (auto idx : random_indexes) {
      c += stdvec[idx];
    }
    stdvec[0] = c;
  });

  auto stddeque_ms = Bench(10, [&stddeque, &random_indexes]() {
    u32 c = 0;
    for (auto idx : random_indexes) {
      c += stddeque[idx];
    }
    stddeque[0] = c;
  });

  auto chunked_ms = Bench(10, [&chunkedvec, &random_indexes]() {
    u32 c = 0;
    for (auto idx : random_indexes) {
      c += chunkedvec[idx];
    }
    chunkedvec[0] = c;
  });

  std::cout << std::fixed << std::setprecision(4);
  std::cout << "std::vector  : " << stdvec_ms << " ms" << std::endl;
  std::cout << "std::deque   : " << stddeque_ms << " ms" << std::endl;
  std::cout << "ChunkedVector: " << chunked_ms << " ms" << std::endl;
}

}  // namespace terrier::execution::util::test
