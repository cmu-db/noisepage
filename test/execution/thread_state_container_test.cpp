#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include <tbb/tbb.h>  // NOLINT

#include "execution/tpl_test.h"

#include "execution/sql/thread_state_container.h"

namespace terrier::execution::sql::test {

class ThreadStateContainerTest : public TplTest {
 protected:
  void ForceCreationOfThreadStates(ThreadStateContainer *container) {
    std::vector<uint32_t> input(2000);
    tbb::task_scheduler_init sched;
    tbb::parallel_for_each(input.begin(), input.end(),
                           [&container](auto c) { container->AccessThreadStateOfCurrentThread(); });
  }
};

// NOLINTNEXTLINE
TEST_F(ThreadStateContainerTest, EmptyStateTest) {
  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);
  container.Reset(0, nullptr, nullptr, nullptr);
  UNUSED_ATTRIBUTE auto *state = container.AccessThreadStateOfCurrentThread();
  container.Clear();
}

// NOLINTNEXTLINE
TEST_F(ThreadStateContainerTest, ComplexObjectContainerTest) {
  struct Object {
    uint64_t x_{0};
    uint32_t arr_[10] = {0};
    uint32_t arr_2_[2] = {44, 23};
    Object *next_{nullptr};
    bool initialized_{false};
  };

  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);

  container.Reset(
      sizeof(Object),
      [](UNUSED_ATTRIBUTE auto *_, auto *s) {
        // Set some stuff to indicate object is initialized
        auto obj = new (s) Object();
        obj->x_ = 10;
        obj->initialized_ = true;
      },
      nullptr, nullptr);
  ForceCreationOfThreadStates(&container);

  // Check
  container.ForEach<Object>([](Object *obj) {
    EXPECT_EQ(10u, obj->x_);
    EXPECT_EQ(nullptr, obj->next_);
    EXPECT_EQ(true, obj->initialized_);
  });
}

// NOLINTNEXTLINE
TEST_F(ThreadStateContainerTest, ContainerResetTest) {
  // The container
  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);

  //
  // Test: Create thread local state that adds to a contextually provided
  //       counter on construction, and decrements upon destruction. Try
  //       resetting the container multiple times. After all is said and done,
  //       the count should be zero.
  //

  const uint32_t init_num = 44;
  std::atomic<uint32_t> count(init_num);

#define RESET(N)                                                                                                \
  {                                                                                                             \
    /* Reset the container, add/sub upon creation/destruction by amount */                                      \
    container.Reset(                                                                                            \
        sizeof(uint32_t),                                                                                       \
        [](auto *ctx, UNUSED_ATTRIBUTE auto *s) { (*reinterpret_cast<decltype(count) *>(ctx)) += N; },          \
        [](auto *ctx, UNUSED_ATTRIBUTE auto *s) { (*reinterpret_cast<decltype(count) *>(ctx)) -= N; }, &count); \
    ForceCreationOfThreadStates(&container);                                                                    \
  }

  RESET(1)
  RESET(2)
  RESET(3)
  RESET(4)

  container.Clear();

  EXPECT_EQ(init_num, count);
}

// NOLINTNEXTLINE
TEST_F(ThreadStateContainerTest, SimpleContainerTest) {
  //
  // Test: Iterate a vector of elements, incrementing a count for each element.
  //       Each thread maintains a separate count. After iteration, the sum of
  //       all thread-local counts must match the size of the input vector.
  //

  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);
  container.Reset(
      sizeof(uint32_t), [](UNUSED_ATTRIBUTE auto *ctx, auto *s) { *reinterpret_cast<uint32_t *>(s) = 0; }, nullptr,
      nullptr);

  std::vector<uint32_t> input(10000);
  std::iota(input.begin(), input.end(), 0);

  tbb::task_scheduler_init sched;
  tbb::blocked_range r(std::size_t(0), input.size());
  tbb::parallel_for(r, [&container](const auto &range) {
    auto *state = container.AccessThreadStateOfCurrentThreadAs<uint32_t>();
    for (auto iter = range.begin(), end = range.end(); iter != end; ++iter) {
      (*state)++;
    }
  });

  // Iterate over all to collect counts
  uint32_t total = 0;
  container.ForEach<uint32_t>([&total](const uint32_t *const count) { total += *count; });
  EXPECT_EQ(input.size(), total);

  // Manually collect and add
  {
    std::vector<uint32_t *> counts;
    container.CollectThreadLocalStateElementsAs(&counts, 0);
    LOG_INFO("{} thread states", counts.size());

    total = static_cast<int32_t>(std::accumulate(counts.begin(), counts.end(), uint32_t(0),
                                                 [](uint32_t partial, const uint32_t *c) { return partial + *c; }));
    EXPECT_EQ(input.size(), total);
  }
}

}  // namespace terrier::execution::sql::test
