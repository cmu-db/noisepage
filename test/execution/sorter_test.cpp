#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <queue>
#include <random>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"

#include <tbb/tbb.h>  // NOLINT

#include "ips4o/ips4o.hpp"

#include "execution/exec/execution_context.h"
#include "execution/sql/sorter.h"
#include "execution/sql/thread_state_container.h"

#define TestAllSigned(FuncName, Args...) \
  FuncName<int8_t>(Args);                \
  FuncName<int16_t>(Args);               \
  FuncName<int32_t>(Args);               \
  FuncName<int64_t>(Args);

#define TestAllUnsigned(FuncName, Args...) \
  FuncName<uint8_t>(Args);                 \
  FuncName<uint16_t>(Args);                \
  FuncName<uint32_t>(Args);                \
  FuncName<uint64_t>(Args);

#define TestAllIntegral(FuncName, Args...) TestAllSigned(FuncName, Args) TestAllUnsigned(FuncName, Args)

namespace terrier::execution::sql::test {

class SorterTest : public TplTest {
 public:
  std::default_random_engine generator_;
};

template <typename IntType, typename Random>
void TestSortRandomTupleSize(const uint32_t num_iters, const uint32_t max_elems, Random *generator) {
  std::uniform_int_distribution<IntType> rng(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());
  std::uniform_int_distribution<uint32_t> rng_elems(std::numeric_limits<uint32_t>::min(),
                                                    std::numeric_limits<uint32_t>::max());

  // We insert tuples of size IntType. It would be nice to std::memcmp, but
  // cmp_fn must be a function pointer which means the lambda cannot capture
  // tuple_size, so this is not possible without heavy macro abuse or passing in
  // something that we can sizeof(). Limiting ourselves to IntType should be
  // fine.
  constexpr const auto tuple_size = sizeof(IntType);
  const auto cmp_fn = [](const void *a, const void *b) -> int32_t {
    const auto val_a = *reinterpret_cast<const IntType *>(a);
    const auto val_b = *reinterpret_cast<const IntType *>(b);
    return val_a < val_b ? -1 : (val_a == val_b ? 0 : 1);
  };

  for (uint32_t curr_iter = 0; curr_iter < num_iters; curr_iter++) {
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
    for (uint32_t i = 0; i < num_elems; i++) {
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
    for (uint32_t i = 0; i < num_elems; i++) {
      EXPECT_EQ(*reinterpret_cast<const IntType *>(*iter), reference[i]);
      ++iter;
    }
  }
}

template <typename IntType, typename Random>
void TestTopKRandomTupleSize(const uint32_t num_iters, const uint32_t max_elems, Random *generator) {
  std::uniform_int_distribution<IntType> rng(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::max());
  std::uniform_int_distribution<uint32_t> rng_elems(std::numeric_limits<uint32_t>::min(),
                                                    std::numeric_limits<uint32_t>::max());

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

  for (uint32_t curr_iter = 0; curr_iter < num_iters; curr_iter++) {
    // Test a random number of elements.
    const auto num_elems = (rng_elems(*generator) % max_elems) + 1;
    // For a random number of top k.
    const auto top_k = std::uniform_int_distribution<uint32_t>(1, num_elems)(*generator);

    // Create a reference top-K min-heap. This contains our real data, and
    // sorter should match it at the end.
    std::priority_queue<IntType, std::vector<IntType>, std::greater<>> reference;

    // Create a sorter.
    MemoryPool memory(nullptr);
    sql::Sorter sorter(&memory, cmp_fn, tuple_size);

    // Randomly create and insert elements to both sorter and reference.
    for (uint32_t i = 0; i < num_elems; i++) {
      const auto rand_data = rng(*generator);
      reference.push(rand_data);

      auto *elem = reinterpret_cast<IntType *>(sorter.AllocInputTupleTopK(top_k));
      *elem = rand_data;
      sorter.AllocInputTupleTopKFinish(top_k);
    }

    // Check that only the top k elements are left.
    sorter.Sort();  // Sort because the reference is sorted.
    sql::SorterIterator iter(&sorter);
    for (uint32_t i = 0; i < top_k; i++) {
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

template <uint32_t N>
struct TestTuple {
  uint32_t key_;
  uint32_t data_[N];

  int32_t Compare(const TestTuple<N> &other) const { return key_ - other.key_; }
};

// Generic function to perform a parallel sort. The input parameter indicates
// the sizes_ of each thread-local sorter that will be created.
template <uint32_t N>
void TestParallelSort(const std::vector<uint32_t> &sorter_sizes_) {
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
  const auto destroy_sorter = [](UNUSED_ATTRIBUTE void *ctx, void *s) { reinterpret_cast<Sorter *>(s)->~Sorter(); };

  // Create container
  exec::ExecutionContext exec_ctx(catalog::INVALID_DATABASE_OID, nullptr, nullptr, nullptr, nullptr);
  ThreadStateContainer container(exec_ctx.GetMemoryPool());

  container.Reset(sizeof(Sorter), init_sorter, destroy_sorter, &exec_ctx);

  // Parallel build
  tbb::task_scheduler_init sched;
  tbb::parallel_for_each(sorter_sizes_.begin(), sorter_sizes_.end(), [&container](auto sorter_size) {
    auto *sorter = container.AccessThreadStateOfCurrentThreadAs<Sorter>();
    for (uint32_t i = 0; i < sorter_size; i++) {
      auto *elem = reinterpret_cast<TestTuple<N> *>(sorter->AllocInputTuple());
      elem->key_ = i;
    }
  });

  // Main parallel sort
  Sorter main(exec_ctx.GetMemoryPool(), cmp_fn, sizeof(TestTuple<N>));
  main.SortParallel(&container, 0);

  uint32_t expected_total_size =
      std::accumulate(sorter_sizes_.begin(), sorter_sizes_.end(), 0u, [](auto p, auto s) { return p + s; });

  EXPECT_TRUE(main.IsSorted());
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
  for (uint32_t x : {0, 1, 10, 100, 1000}) {
    for (uint32_t y : {0, 1, 10, 100, 1000}) {
      for (uint32_t z : {0, 1, 10, 100, 1000}) {
        TestParallelSort<2>({x, y, z});
      }
    }
  }
}

}  // namespace terrier::execution::sql::test
