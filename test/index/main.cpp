
#include "util/bwtree_test_util.h"
#include "util/test_harness.h"
/*
 * GetThreadNum() - Returns the number of threads used for multithreaded testing
 *
 * By default 40 threads are used
 */
uint64_t GetThreadNum() {    
    uint64_t thread_num = 40;
    bool ret = Envp::GetValueAsUL("THREAD_NUM", &thread_num);
    if(ret == false) {
      throw "THREAD_NUM must be an unsigned ineteger!"; 
    } else {
      printf("Using thread_num = %lu\n", thread_num); 
    }
    
    return thread_num;
}
struct BwTreeTests : public TerrierTest { };
TEST_F(BwTreeTests, )
int main(int argc, char **argv) {
  bool run_benchmark_all = false;
  bool run_test = false;
  bool run_benchmark_bwtree = false;
  bool run_benchmark_bwtree_full = false;
  bool run_benchmark_btree_full = false;
  bool run_benchmark_art_full = false;
  bool run_stress = false;
  bool run_epoch_test = false;
  bool run_infinite_insert_test = false;
  bool run_email_test = false;
  bool run_mixed_test = false;

  int opt_index = 1;
  while(opt_index < argc) {
    char *opt_p = argv[opt_index];

    if(strcmp(opt_p, "--benchmark-all") == 0) {
      run_benchmark_all = true;
    } else if(strcmp(opt_p, "--test") == 0) {
      run_test = true;
    } else if(strcmp(opt_p, "--benchmark-bwtree") == 0) {
      run_benchmark_bwtree = true;
    } else if(strcmp(opt_p, "--benchmark-bwtree-full") == 0) {
      run_benchmark_bwtree_full = true;
    } else if(strcmp(opt_p, "--benchmark-btree-full") == 0) {
      run_benchmark_btree_full = true;
    } else if(strcmp(opt_p, "--benchmark-art-full") == 0) {
      run_benchmark_art_full = true;
    } else if(strcmp(opt_p, "--stress-test") == 0) {
      run_stress = true;
    } else if(strcmp(opt_p, "--epoch-test") == 0) {
      run_epoch_test = true;
    } else if(strcmp(opt_p, "--infinite-insert-test") == 0) {
      run_infinite_insert_test = true;
    } else if(strcmp(opt_p, "--email-test") == 0) {
      run_email_test = true;
    } else if(strcmp(opt_p, "--mixed-test") == 0) {
      run_mixed_test = true;
    } else {
      printf("ERROR: Unknown option: %s\n", opt_p);

      return 0;
    }

    opt_index++;
  }

  bwt_printf("RUN_BENCHMARK_ALL = %d\n", run_benchmark_all);
  bwt_printf("RUN_BENCHMARK_BWTREE_FULL = %d\n", run_benchmark_bwtree_full);
  bwt_printf("RUN_BENCHMARK_BWTREE = %d\n", run_benchmark_bwtree);
  bwt_printf("RUN_BENCHMARK_ART_FULL = %d\n", run_benchmark_art_full);
  bwt_printf("RUN_TEST = %d\n", run_test);
  bwt_printf("RUN_STRESS = %d\n", run_stress);
  bwt_printf("RUN_EPOCH_TEST = %d\n", run_epoch_test);
  bwt_printf("RUN_INFINITE_INSERT_TEST = %d\n", run_infinite_insert_test);
  bwt_printf("RUN_EMAIL_TEST = %d\n", run_email_test);
  bwt_printf("RUN_MIXED_TEST = %d\n", run_mixed_test);
  bwt_printf("======================================\n");

  //////////////////////////////////////////////////////
  // Next start running test cases
  //////////////////////////////////////////////////////

  TreeType *t1 = nullptr;
  
  if(run_mixed_test == true) {
    t1 = GetEmptyTree();

    printf("Starting mixed testing...\n");
    LaunchParallelTestID(t1, mixed_thread_num, MixedTest1, t1);
    printf("Finished mixed testing\n");

    PrintStat(t1);

    MixedGetValueTest(t1);

    DestroyTree(t1);
  }
  
  if(run_email_test == true) {
    auto t2 = new BwTree<std::string, long int>{true};
    
    TestBwTreeEmailInsertPerformance(t2, "emails_dump.txt");
    
    // t2 has already been deleted for memory reason
  }

  if(run_epoch_test == true) {
    t1 = GetEmptyTree();

    TestEpochManager(t1);

    DestroyTree(t1);
  }


  if(run_test == true) {

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

    /////////////////////////////////////////////////////////////////
    // Test mixed insert/delete
    /////////////////////////////////////////////////////////////////
    
    // no print
    t1 = GetEmptyTree(true);

    LaunchParallelTestID(t1, basic_test_thread_num, MixedTest1, t1);
    printf("Finished mixed testing\n");

    PrintStat(t1);

    MixedGetValueTest(t1);
    
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
  
  if(run_infinite_insert_test == true) {
    t1 = GetEmptyTree();

    InfiniteRandomInsertTest(t1);

    DestroyTree(t1);
  }

  if(run_stress == true) {
    t1 = GetEmptyTree();

    LaunchParallelTestID(t1, 8, StressTest, t1);

    DestroyTree(t1);
  }

  return 0;
}

