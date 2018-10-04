
/*
 * random_pattern_test.cpp
 *
 * This file includes test cases that test index performance/correctness
 * with a totally random access pattern
 *
 * by Ziqi Wang
 */

#include "util/bwtree_test_util.h"

/*
 * RandomInsertTest() - Inserts in a 1M key space randomly until
 *                      all keys has been inserted
 *
 * This function should be called in a multithreaded manner
 */
void RandomInsertTest(uint64_t thread_id, TreeType *t) {
  // This defines the key space (0 ~ (1M - 1))
  const size_t key_num = 1024 * 1024;

  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

  static std::atomic<size_t> insert_success_counter;

  while (insert_success_counter.load() < key_num) {
    int key = uniform_dist(e1);

    if (t->Insert(key, key)) insert_success_counter.fetch_add(1);
  }

  printf("Random insert (%lu) finished\n", thread_id);

  return;
}

/*
 * RandomInsertVerify() - Veryfies whether random insert is correct
 *
 * This function assumes that all keys have been inserted and the size of the
 * key space is 1M
 */
void RandomInsertVerify(TreeType *t) {
  for (int i = 0; i < 1024 * 1024; i++) {
    auto s = t->GetValue(i);

    assert(s.size() == 1);
    assert(*s.begin() == i);
  }

  printf("Random insert test OK\n");

  return;
}
