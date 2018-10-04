
/*
 * stress_test.cpp
 *
 * Random Insert and Delete
 *
 * By Ziqi Wang
 */
#include <vector>

#include "util/bwtree_test_util.h"

/*
 * StressTest() - Insert and Delete on the tree until user pauses
 *
 * This function require multiple threads. Half of the threads are assigned
 * as insert thread, the other are assigned delete thread
 *
 * The test pauses after some certain number of keys, and does a read
 * performance test to see how random insert/delete affects performance
 *
 * Also the epoch manager is tested against memory leak and GC efficiency
 */
void StressTest(uint64_t thread_id, TreeType *t) {
  static std::atomic<size_t> tree_size;
  static std::atomic<size_t> insert_success;
  static std::atomic<size_t> delete_success;
  static std::atomic<size_t> total_op;

  const size_t thread_num = 8;

  int max_key = 1024 * 1024;

  std::random_device r;

  // Choose a random mean between 0 and max key value
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, max_key - 1);

  while (1) {
    int key = uniform_dist(e1);

    if ((thread_id % 2) == 0) {
      if (t->Insert(key, key)) {
        tree_size.fetch_add(1);
        insert_success.fetch_add(1);
      }
    } else {
      if (t->Delete(key, key)) {
        tree_size.fetch_sub(1);
        delete_success.fetch_add(1);
      }
    }

    size_t op = total_op.fetch_add(1);

    if (op % max_key == 0) {
      PrintStat(t);
      printf("Total operation = %lu; tree size = %lu\n", op, tree_size.load());
      printf("    insert success = %lu; delete success = %lu\n", insert_success.load(), delete_success.load());
    }

    size_t remainder = (op % (1024UL * 1024UL * 10UL));

    if (remainder < thread_num) {
      std::vector<int64_t> v{};
      int iter = 10;

      v.reserve(100);

      std::chrono::time_point<std::chrono::system_clock> start, end;

      start = std::chrono::system_clock::now();

      for (int j = 0; j < iter; j++) {
        for (int i = 0; i < max_key; i++) {
          t->GetValue(i, v);

          v.clear();
        }
      }

      end = std::chrono::system_clock::now();
      std::chrono::duration<double> elapsed_seconds = end - start;

      std::cout << " Stress Test BwTree: " << (iter * max_key / (1024.0 * 1024.0)) / elapsed_seconds.count()
                << " million read/sec"
                << "\n";
    }
  }

  return;
}
