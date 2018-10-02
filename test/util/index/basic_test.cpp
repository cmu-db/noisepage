
/*
 * basic_test.cpp
 *
 * This file contains basic insert/delete/read test for correctness
 *
 * by Ziqi Wang
 */

#include "util/bwtree_test_util.h"

int basic_test_key_num = 128 * 1024;
int basic_test_thread_num = 8;

/*
 * InsertTest1() - Each threads inserts in its own consecutive key subspace
 *
 * The intervals of each thread does not intersect, therefore contention
 * is very small and this test is supposed to be very fast
 *
 * |---- thread 0 ----|---- thread 1----|----thread 2----| .... |---- thread n----|
 */
void InsertTest1(uint64_t thread_id, TreeType *t) {
  for(int i = static_cast<int>(thread_id * basic_test_key_num);
      i < (int)(thread_id + 1) * basic_test_key_num;
      i++) {
    t->Insert(i, i + 1);
    t->Insert(i, i + 2);
    t->Insert(i, i + 3);
    t->Insert(i, i + 4);
  }

  return;
}

/*
 * DeleteTest1() - Same pattern as InsertTest1()
 */
void DeleteTest1(uint64_t thread_id, TreeType *t) {
  for(int i = static_cast<int>(thread_id * basic_test_key_num);
      i < (int)(thread_id + 1) * basic_test_key_num;
      i++) {
    t->Delete(i, i + 1);
    t->Delete(i, i + 2);
    t->Delete(i, i + 3);
    t->Delete(i, i + 4);
  }

  return;
}

/*
 * InsertTest2() - All threads collectively insert on the key space
 *
 * | t0 t1 t2 t3 .. tn | t0 t1 t2 t3 .. tn | t0 t1 .. | .. |  ... tn |
 *
 * This test is supposed to be slower since the contention is very high
 * between different threads
 */
void InsertTest2(uint64_t thread_id, TreeType *t) {
  for(int i = 0;i < basic_test_key_num;i++) {
    int key = static_cast<int>(basic_test_thread_num * i + thread_id);

    t->Insert(key, key + 1);
    t->Insert(key, key + 2);
    t->Insert(key, key + 3);
    t->Insert(key, key + 4);
  }

  return;
}

/*
 * DeleteTest2() - The same pattern as InsertTest2()
 */
void DeleteTest2(uint64_t thread_id, TreeType *t) {
  for(int i = 0;i < basic_test_key_num;i++) {
    int key = static_cast<int>(basic_test_thread_num * i + thread_id);

    t->Delete(key, key + 1);
    t->Delete(key, key + 2);
    t->Delete(key, key + 3);
    t->Delete(key, key + 4);
  }

  return;
}

/*
 * DeleteGetValueTest() - Verifies all values have been deleted
 *
 * This function verifies on key_num * thread_num key space
 */
void DeleteGetValueTest(TreeType *t) {
  for(int i = 0;i < basic_test_key_num * basic_test_thread_num;i ++) {
    auto value_set = t->GetValue(i);

    assert(value_set.size() == 0);
  }

  return;
}

/*
 * InsertGetValueTest() - Verifies all values have been inserted
 */
void InsertGetValueTest(TreeType *t) {
  for(int i = 0;i < basic_test_key_num * basic_test_thread_num;i++) {
    auto value_set = t->GetValue(i);

    assert(value_set.size() == 4);
  }

  return;
}
