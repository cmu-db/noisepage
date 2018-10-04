#include <functional>

#include "bwtree/bwtree.h"
#include "util/bwtree_test_util.h"
#include "util/test_harness.h"

//#define BWTREE_DEBUG

namespace terrier {

// NOLINTNEXTLINE
TEST(BwTreeTests, IteratorTest) {
  TreeType *t1 = nullptr;

  t1 = GetEmptyTree(true);

  const int key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for (int i = 0; i < key_num; i++) {
    t1->Insert(i, i);
  }

  ForwardIteratorTest(t1, key_num);
  BackwardIteratorTest(t1, key_num);

  // PrintStat(t1);

  // Do not forget to deletet the tree here
  DestroyTree(t1, true);
}

// NOLINTNEXTLINE
TEST(BwTreeTests, RandomInsert) {
  TreeType *t1 = nullptr;

  // Do not print here otherwise we could not see result
  t1 = GetEmptyTree(true);

  LaunchParallelTestID(t1, 8, RandomInsertTest, t1);
  RandomInsertVerify(t1);

  // no print
  DestroyTree(t1, true);
}

// NOLINTNEXTLINE
TEST(BwTreeTests, MixedTest) {
  TreeType *t1 = nullptr;
  // no print
  t1 = GetEmptyTree(true);

  LaunchParallelTestID(t1, basic_test_thread_num, MixedTest1, t1);

  // PrintStat(t1);

  MixedGetValueTest(t1);
  // no print
  DestroyTree(t1, true);
}

// NOLINTNEXTLINE
TEST(BwTreeTests, MultiThread) {
  TreeType *t1 = nullptr;
  t1 = GetEmptyTree(true);
  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest2, t1);

  // PrintStat(t1);

  InsertGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest1, t1);

  // PrintStat(t1);

  DeleteGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest1, t1);

  // PrintStat(t1);

  InsertGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest2, t1);

  // PrintStat(t1);

  DeleteGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest1, t1);

  // PrintStat(t1);

  InsertGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest1, t1);

  // PrintStat(t1);

  DeleteGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest2, t1);

  // PrintStat(t1);

  InsertGetValueTest(t1);

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest2, t1);

  // PrintStat(t1);

  DeleteGetValueTest(t1);

  DestroyTree(t1, true);
}

// The following test is disabled because it has infinite loop
// NOLINTNEXTLINE
TEST(BwTreeTests, DISABLED_StressTest) {
  TreeType *t1 = nullptr;
  t1 = GetEmptyTree(true);

  LaunchParallelTestID(t1, 8, StressTest, t1);

  DestroyTree(t1, true);
}

// The following test is disabled because it takes too long (10 mins)
// NOLINTNEXTLINE
TEST(BwTreeTests, DISABLED_MiscTest) {
  TreeType *t1 = nullptr;
  t1 = GetEmptyTree(true);

  TestEpochManager(t1);

  DestroyTree(t1, true);
}
}  // namespace terrier
