
/*
 * misc_test.cpp
 *
 * Tests everything not covered in other tests
 *
 * By Ziqi Wang
 */

#include "util/bwtree_test_util.h"

/*
 * TestEpochManager() - Tests epoch manager
 *
 * This function enters epoch and takes a random delay and exits epoch
 * repeat until desired count has been reached
 */
void TestEpochManager(TreeType *t) {
  std::atomic<int> thread_finished;

  thread_finished = 1;

  auto func = [t, &thread_finished](uint64_t thread_id, int iter) {
    for(int i = 0;i < iter;i++) {
      auto node = t->epoch_manager.JoinEpoch();

      // Copied from stack overflow:
      // http://stackoverflow.com/questions/7577452/random-time-delay

      std::mt19937_64 eng{std::random_device{}()};  // or seed however you want
      std::uniform_int_distribution<> dist{1, 100};
      std::this_thread::sleep_for(std::chrono::milliseconds{dist(eng) +
                                                            thread_id});

      t->epoch_manager.LeaveEpoch(node);
    }

    printf("Thread finished: %d        \r", thread_finished.fetch_add(1));

    return;
  };

  LaunchParallelTestID(t, 2, func, 10000);

  putchar('\n');

  return;
}
