
/*
 * mixed_test.cpp
 *
 * Multithreaded insert-delete contention test
 *
 * This test focuses on corner cases where insert and delete happens
 * concurrently on the same key
 */

#include "util/bwtree_test_util.h"

std::atomic<size_t> mixed_insert_success;
std::atomic<size_t> mixed_delete_success;
std::atomic<size_t> mixed_delete_attempt;

int mixed_thread_num = 8;
int mixed_key_num = 1024 * 1024;

/*
 * MixedTest1() - Tests insert-delete contention
 *
 * This is the place where most implementations break
 */
void MixedTest1(uint64_t thread_id, TreeType *t) {  
  if((thread_id % 2) == 0) {
    for(int i = 0;i < mixed_key_num;i++) {
      int key = static_cast<int>(mixed_thread_num * i + thread_id);

      if(t->Insert(key, key)) mixed_insert_success.fetch_add(1);
    }

    printf("Finish Inserting (%lu)\n", thread_id);
  } else {
    for(int i = 0;i < mixed_key_num;i++) {
      int key = static_cast<int>(mixed_thread_num * i + thread_id - 1);

      while(t->Delete(key, key) == false) mixed_delete_attempt.fetch_add(1);

      mixed_delete_success.fetch_add(1);
      mixed_delete_attempt.fetch_add(1);
    }
    
    printf("Finish Deleting (%lu -> %lu)\n", thread_id, thread_id - 1);
  }

  return;
}


/*
 * MixedGetValueTest() - Verifies that all values are deleted after
 *                       mixed test
 */
void MixedGetValueTest(TreeType *t) {
  size_t value_count = 0UL;

  for(int i = 0;i < mixed_key_num * mixed_thread_num;i ++) {
    auto value_set = t->GetValue(i);

    value_count += value_set.size();
  }
  
  assert(value_count == 0);

  printf("Finished counting values: %lu\n", value_count);
  printf("    insert success = %lu; delete success = %lu\n",
         mixed_insert_success.load(),
         mixed_delete_success.load());
  printf("    delete attempt = %lu\n", mixed_delete_attempt.load());

  return;
}
