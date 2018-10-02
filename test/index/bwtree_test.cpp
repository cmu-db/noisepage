#include <functional>

#include "bwtree/bwtree.h"
#include "util/test_harness.h"
#include "util/bwtree_test_util.h"
namespace terrier {
using TEST_TYPE = char;
//using BwTree = ::wangziqi2013::bwtree::BwTree<TEST_TYPE, TEST_TYPE, std::less<>, std::equal_to<>, std::hash<TEST_TYPE>,
//                                              std::equal_to<>, std::hash<TEST_TYPE>>;

struct BwTreeTests : public TerrierTest {
  //BwTree tree_;
  TreeType *t1 = nullptr;
};

// NOLINTNEXTLINE
TEST_F(BwTreeTests, IteratorTest) {
  /////////////////////////////////////////////////////////////////
  // Test iterator
  /////////////////////////////////////////////////////////////////
  // This could print
  t1 = GetEmptyTree();

  const int key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for(int i = 0;i < key_num;i++) {
    t1->Insert(i, i);
  }

  ForwardIteratorTest(t1, key_num);
  BackwardIteratorTest(t1, key_num);

  PrintStat(t1);

  printf("Finised testing iterator\n");

  // Do not forget to deletet the tree here
  DestroyTree(t1, true);
}

// NOLINTNEXTLINE
TEST_F(BwTreeTests, RandomInsert) {
  /////////////////////////////////////////////////////////////////
  // Test random insert
  /////////////////////////////////////////////////////////////////

  printf("Testing random insert...\n");

  // Do not print here otherwise we could not see result
  t1 = GetEmptyTree(true);

  LaunchParallelTestID(t1, 8, RandomInsertTest, t1);
  RandomInsertVerify(t1);

  printf("Finished random insert testing. Delete the tree.\n");

  // no print
  DestroyTree(t1, true);
}

// NOLINTNEXTLINE
TEST_F(BwTreeTests, MixedTest) {
  /////////////////////////////////////////////////////////////////
  // Test mixed insert/delete
  /////////////////////////////////////////////////////////////////

  // no print
  t1 = GetEmptyTree(true);

  LaunchParallelTestID(t1, basic_test_thread_num, MixedTest1, t1);
  printf("Finished mixed testing\n");

  PrintStat(t1);

  MixedGetValueTest(t1);
}


// NOLINTNEXTLINE
TEST_F(BwTreeTests, MultiThread) {
/////////////////////////////////////////////////////////////////
  // Test Basic Insert/Delete/GetValue
  //   with different patterns and multi thread
  /////////////////////////////////////////////////////////////////

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest2, t1);
  printf("Finished inserting all keys\n");

  PrintStat(t1);

  InsertGetValueTest(t1);
  printf("Finished verifying all inserted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest1, t1);
  printf("Finished deleting all keys\n");

  PrintStat(t1);

  DeleteGetValueTest(t1);
  printf("Finished verifying all deleted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest1, t1);
  printf("Finished inserting all keys\n");

  PrintStat(t1);

  InsertGetValueTest(t1);
  printf("Finished verifying all inserted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest2, t1);
  printf("Finished deleting all keys\n");

  PrintStat(t1);

  DeleteGetValueTest(t1);
  printf("Finished verifying all deleted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest1, t1);
  printf("Finished inserting all keys\n");

  PrintStat(t1);

  InsertGetValueTest(t1);
  printf("Finished verifying all inserted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest1, t1);
  printf("Finished deleting all keys\n");

  PrintStat(t1);

  DeleteGetValueTest(t1);
  printf("Finished verifying all deleted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, InsertTest2, t1);
  printf("Finished inserting all keys\n");

  PrintStat(t1);

  InsertGetValueTest(t1);
  printf("Finished verifying all inserted values\n");

  LaunchParallelTestID(t1, basic_test_thread_num, DeleteTest2, t1);
  printf("Finished deleting all keys\n");

  PrintStat(t1);

  DeleteGetValueTest(t1);
  printf("Finished verifying all deleted values\n");

  DestroyTree(t1);
}

}  // namespace terrier
