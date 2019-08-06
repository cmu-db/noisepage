#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <queue>
#include <random>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"  // NOLINT

#include <tbb/tbb.h>  // NOLINT

#include "ips4o/ips4o.hpp"

#include "execution/exec/execution_context.h"
#include "execution/sql/sorter.h"
#include "execution/sql/thread_state_container.h"

#define TestAllSigned(FuncName, Args...) \
  FuncName<i8>(Args);                    \
  FuncName<i16>(Args);                   \
  FuncName<i32>(Args);                   \
  FuncName<i64>(Args);

#define TestAllUnsigned(FuncName, Args...) \
  FuncName<u8>(Args);                      \
  FuncName<u16>(Args);                     \
  FuncName<u32>(Args);                     \
  FuncName<u64>(Args);

#define TestAllIntegral(FuncName, Args...) TestAllSigned(FuncName, Args) TestAllUnsigned(FuncName, Args)

namespace terrier::sql::test {

class SorterTest : public TplTest {
 public:
  std::default_random_engine generator_;
};

template <typename IntType, typename Random>
void TestSortRandomTupleSize(const u32 num_iters, const u32 max_elems, Random *generator) {
  std::uniform_int_distribution<IntType> rng(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());
  std::uniform_int_distribution<u32> rng_elems(std::numeric_limits<u32>::min(), std::numeric_limits<u32>::max());

  // We insert tuples of size IntType. It would be nice to std::memcmp, but
  // cmp_fn must be a function pointer which means the lambda cannot capture
  // tuple_size, so this is not possible without heavy macro abuse or passing in
  // something that we can sizeof(). Limiting ourselves to IntType should be
  // fine.
  constexpr const auto tuple_size = sizeof(IntType);
  const auto cmp_fn = [](const void *a, const void *b) -> i32 {
    const auto val_a = *reinterpret_cast<const IntType *>(a);
    const auto val_b = *reinterpret_cast<const IntType *>(b);
    return val_a < val_b ? -1 : (val_a == val_b ? 0 : 1);
  };

  for (u32 curr_iter = 0; curr_iter < num_iters; curr_iter++) {
    // Test a random number of elements.
    const auto num_elems = (rng_elems(*generator) % max_elems) + 1;

    // Create a reference vector. This contains our real data, and sorter should
    // match it at the end.
    std::vector<IntType> reference;
    reference.reserve(num_elems);

    // Create a sorter.
    MemoryPool memory(nullptr);
    sql::Sorter sorter(&memory, cmp_fn, tuple_size);

    // Randomly create and insert elements to both sorter and reference.
    for (u32 i = 0; i < num_elems; i++) {
      const auto rand_data = rng(*generator);
      reference.emplace_back(rand_data);
      auto *elem = reinterpret_cast<IntType *>(sorter.AllocInputTuple());
      *elem = rand_data;
    }

    // Sort the data from small to big.
    std::sort(reference.begin(), reference.end());
    sorter.Sort();

    // Check that the elements are in the same order.
    sql::SorterIterator iter(&sorter);
    for (u32 i = 0; i < num_elems; i++) {
      EXPECT_EQ(*reinterpret_cast<const IntType *>(*iter), reference[i]);
      ++iter;
    }
  }
}

template <typename IntType, typename Random>
void TestTopKRandomTupleSize(const u32 num_iters, const u32 max_elems, Random *generator) {
  std::uniform_int_distribution<IntType> rng(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());
  std::uniform_int_distribution<u32> rng_elems(std::numeric_limits<u32>::min(), std::numeric_limits<u32>::max());

  // We insert tuples of size IntType. It would be nice to std::memcmp, but
  // cmp_fn must be a function pointer which means the lambda cannot capture
  // tuple_size, so this is not possible without heavy macro abuse or passing in
  // something that we can sizeof(). Limiting ourselves to IntType should be
  // fine.
  const auto tuple_size = sizeof(IntType);
  auto cmp_fn = [](const void *a, const void *b) -> int {
    const auto val_a = *reinterpret_cast<const IntType *>(a);
    const auto val_b = *reinterpret_cast<const IntType *>(b);
    return val_a < val_b ? -1 : (val_a == val_b ? 0 : 1);
  };

  for (u32 curr_iter = 0; curr_iter < num_iters; curr_iter++) {
    // Test a random number of elements.
    const auto num_elems = (rng_elems(*generator) % max_elems) + 1;
    // For a random number of top k.
    const auto top_k = std::uniform_int_distribution<u32>(1, num_elems)(*generator);

    // Create a reference top-K min-heap. This contains our real data, and
    // sorter should match it at the end.
    std::priority_queue<IntType, std::vector<IntType>, std::greater<>> reference;

    // Create a sorter.
    MemoryPool memory(nullptr);
    sql::Sorter sorter(&memory, cmp_fn, tuple_size);

    // Randomly create and insert elements to both sorter and reference.
    for (u32 i = 0; i < num_elems; i++) {
      const auto rand_data = rng(*generator);
      reference.push(rand_data);

      auto *elem = reinterpret_cast<IntType *>(sorter.AllocInputTupleTopK(top_k));
      *elem = rand_data;
      sorter.AllocInputTupleTopKFinish(top_k);
    }

    // Check that only the top k elements are left.
    sorter.Sort();  // Sort because the reference is sorted.
    sql::SorterIterator iter(&sorter);
    for (u32 i = 0; i < top_k; i++) {
      const auto ref_elem = reference.top();
      reference.pop();
      EXPECT_EQ(*reinterpret_cast<const IntType *>(*iter), ref_elem);
      ++iter;
    }
  }
}

// NOLINTNEXTLINE
TEST_F(SorterTest, SortTest) {
  const uint32_t num_iters = 5;
  const uint32_t max_elems = 10000;
  TestAllIntegral(TestSortRandomTupleSize, num_iters, max_elems, &generator_);
}

// NOLINTNEXTLINE
TEST_F(SorterTest, TopKTest) {
  const uint32_t num_iters = 5;
  const uint32_t max_elems = 10000;
  TestAllIntegral(TestTopKRandomTupleSize, num_iters, max_elems, &generator_);
}

// NOLINTNEXTLINE
TEST_F(SorterTest, DISABLED_PerfSortTest) {
  // TODO(Amadou): Figure out a way to avoid manually changing this. Maybe
  // metaprogramming?
  using data = std::array<byte, 128>;
  using int_type = uint32_t;

  // 10 million elements
  const uint32_t num_elems = 10000000;

  // The sort comparison function
  auto sorter_cmp_fn = [](const void *a, const void *b) -> i32 {
    // Just compare the first few bytes
    const auto val_a = *reinterpret_cast<const int_type *>(a);
    const auto val_b = *reinterpret_cast<const int_type *>(b);
    return val_a < val_b ? -1 : (val_a == val_b ? 0 : 1);
  };

  auto cmp_fn = [](const data &a, const data &b) -> bool {
    const auto val_a = *reinterpret_cast<const int_type *>(a.data());
    const auto val_b = *reinterpret_cast<const int_type *>(b.data());
    return val_a < val_b;
  };

  // Create the different kinds of vectors.
  // Some of these are commented out to reduce the memory usage of this test.
  MemoryPool memory(nullptr);
  std::vector<data, MemoryPoolAllocator<data>> vec{MemoryPoolAllocator<data>(&memory)};
  util::ChunkedVectorT<data, MemoryPoolAllocator<data>> chunk_vec{MemoryPoolAllocator<data>(&memory)};
  sql::Sorter sorter(&memory, sorter_cmp_fn, sizeof(data));
  std::cout << "Sizeof(data) is " << (sizeof(data)) << std::endl;

  // Fill up the regular vector. This is our reference.
  for (int_type i = 0; i < num_elems; i++) {
    data val;
    // Only the first few bytes are useful for comparison
    std::memcpy(val.data(), &i, sizeof(int_type));
    vec.push_back(val);
  }

  // Shuffle vector to get a random ordering
  std::shuffle(vec.begin(), vec.end(), generator_);

  // Fill the ChunkedVectorT<data> and the Sorter instance with the same data
  for (int_type i = 0; i < num_elems; i++) {
    chunk_vec.push_back(vec[i]);
  }
  for (int_type i = 0; i < num_elems; i++) {
    auto *elem = sorter.AllocInputTuple();
    std::memcpy(elem, vec[i].data(), sizeof(int_type));
  }

  // Run benchmarks
  // NOTE: keep the number of runs to 1.
  // Otherwise the vector will be presorted in subsequent runs, which avoids
  // copies and speeds up the function.
  auto stdvec_ms = Bench(1, [&vec, &cmp_fn]() { ips4o::sort(vec.begin(), vec.end(), cmp_fn); });

  auto chunk_ms = Bench(1, [&chunk_vec, &cmp_fn]() { ips4o::sort(chunk_vec.begin(), chunk_vec.end(), cmp_fn); });

  auto sorter_ms = Bench(1, [&sorter]() { sorter.Sort(); });

  for (u32 i = 0; i < num_elems; i++) {
    const auto std_a = *reinterpret_cast<const int_type *>(vec[i].data());
    const auto chunk_a = *reinterpret_cast<const int_type *>(chunk_vec[i].data());
    EXPECT_EQ(std_a, chunk_a);
  }

  std::cout << std::fixed << std::setprecision(4);
  std::cout << "std::sort(std::vector): " << stdvec_ms << " ms" << std::endl;
  std::cout << "ips4o::sort(ChunkedVector): " << chunk_ms << " ms" << std::endl;
  std::cout << "Sorter.Sort(): " << sorter_ms << " ms" << std::endl;
}

template <u32 N>
struct TestTuple {
  u32 key;
  u32 data[N];

  i32 Compare(const TestTuple<N> &other) const { return key - other.key; }
};

// Generic function to perform a parallel sort. The input parameter indicates
// the sizes of each thread-local sorter that will be created.
template <u32 N>
void TestParallelSort(const std::vector<u32> &sorter_sizes) {
  // Comparison function
  static const auto cmp_fn = [](const void *left, const void *right) {
    const auto *l = reinterpret_cast<const TestTuple<N> *>(left);
    const auto *r = reinterpret_cast<const TestTuple<N> *>(right);
    return l->Compare(*r);
  };

  // Initialization and destruction function
  const auto init_sorter = [](void *ctx, void *s) {
    new (s) Sorter(reinterpret_cast<exec::ExecutionContext *>(ctx)->GetMemoryPool(), cmp_fn, sizeof(TestTuple<N>));
  };
  const auto destroy_sorter = [](UNUSED void *ctx, void *s) { reinterpret_cast<Sorter *>(s)->~Sorter(); };

  // Create container
  auto memory = std::make_unique<MemoryPool>(nullptr);
  exec::ExecutionContext exec_ctx(terrier::catalog::INVALID_DATABASE_OID, nullptr, nullptr, nullptr, nullptr);
  exec_ctx.SetMemoryPool(std::move(memory));
  ThreadStateContainer container(exec_ctx.GetMemoryPool());

  container.Reset(sizeof(Sorter), init_sorter, destroy_sorter, &exec_ctx);

  // Parallel build
  tbb::task_scheduler_init sched;
  tbb::parallel_for_each(sorter_sizes.begin(), sorter_sizes.end(), [&container](auto sorter_size) {
    auto *sorter = container.AccessThreadStateOfCurrentThreadAs<Sorter>();
    for (u32 i = 0; i < sorter_size; i++) {
      auto *elem = reinterpret_cast<TestTuple<N> *>(sorter->AllocInputTuple());
      elem->key = i;
    }
  });

  // Main parallel sort
  Sorter main(exec_ctx.GetMemoryPool(), cmp_fn, sizeof(TestTuple<N>));
  main.SortParallel(&container, 0);

  u32 expected_total_size =
      std::accumulate(sorter_sizes.begin(), sorter_sizes.end(), 0u, [](auto p, auto s) { return p + s; });

  EXPECT_TRUE(main.is_sorted());
  EXPECT_EQ(expected_total_size, main.NumTuples());

  // Ensure sortedness
  const TestTuple<N> *prev = nullptr;
  for (SorterIterator iter(&main); iter.HasNext(); iter.Next()) {
    auto *curr = iter.GetRowAs<TestTuple<N>>();
    if (prev != nullptr) {
      EXPECT_LE(cmp_fn(prev, curr), 0);
    }
    prev = curr;
  }
}

// NOLINTNEXTLINE
TEST_F(SorterTest, BalancedParallelSortTest) {
  TestParallelSort<2>({1000});
  TestParallelSort<2>({1000, 1000});
  TestParallelSort<2>({1000, 1000, 1000});
  TestParallelSort<2>({1000, 1000, 1000, 1000});
}

// NOLINTNEXTLINE
TEST_F(SorterTest, SingleThreadLocalParallelSortTest) {
  // Single thread-local sorter
  TestParallelSort<2>({0});
  TestParallelSort<2>({1});
  TestParallelSort<2>({10});
  TestParallelSort<2>({100});
}

// NOLINTNEXTLINE
TEST_F(SorterTest, UnbalancedParallelSortTest) {
  // All imbalance permutations
  for (u32 x : {0, 1, 10, 100, 1000}) {
    for (u32 y : {0, 1, 10, 100, 1000}) {
      for (u32 z : {0, 1, 10, 100, 1000}) {
        TestParallelSort<2>({x, y, z});
      }
    }
  }
}

}  // namespace terrier::sql::test
