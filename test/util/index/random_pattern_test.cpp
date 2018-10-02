
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
 * RandomBtreeMultimapInsertSpeedTest() - Tests stx::btree_multimap with random
 *                                        insert and read pattern
 */
void RandomBtreeMultimapInsertSpeedTest(size_t key_num) {
  btree_multimap<long, long, KeyComparator> test_map{KeyComparator{1}};

  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, static_cast<int>(key_num - 1));

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    test_map.insert((long)key, (long)key);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "stx::btree_multimap: at least " << (
      static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<long int> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    auto it_pair = test_map.equal_range(key);

    for(auto it = it_pair.first;it != it_pair.second;it++) {
      v.push_back(it->second);
    }

    v.clear();
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "stx::btree_multimap: at least " << (
      static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million read after random insert/sec" << "\n";

  return;
}

/*
 * RandomCuckooHashMapInsertSpeedTest() - Tests cuckoohash_map with random
 *                                        insert and read pattern
 */
void RandomCuckooHashMapInsertSpeedTest(size_t key_num) {
  cuckoohash_map<long, long> test_map{};

  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, static_cast<int>(key_num - 1));

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    test_map.insert((long)key, (long)key);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "cuckoohash_map: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<long int> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);
    long int ret;
    
    test_map.find(key, ret);

    v.push_back(ret);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "cuckoohash_map: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million read after random insert/sec" << "\n";

  return;
}


/*
 * RandomInsertSpeedTest() - Tests how fast it is to insert keys randomly
 */
void RandomInsertSpeedTest(TreeType *t, size_t key_num) {
  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, static_cast<int>(key_num - 1));

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);
    
    t->Insert(key, key);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<long int> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    t->GetValue(key, v);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random read after random insert/sec" << "\n";

  // Measure the overhead

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    v.push_back(key);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> overhead = end - start;

  std::cout << "    Overhead = " << overhead.count() << " seconds" << std::endl;

  return;
}

/*
 * RandomInsertSeqReadSpeedTest() - Tests how fast it is to insert keys randomly
 *                                  and reads then sequentially
 */
void RandomInsertSeqReadSpeedTest(TreeType *t, size_t key_num) {
  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, static_cast<int>(key_num - 1));

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    t->Insert(key, key);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<long int> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    t->GetValue(i, v);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million seq read after random insert/sec" << "\n";

  return;
}

/*
 * SeqInsertRandomReadSpeedTest() - Tests how fast it is to insert keys 
 *                                  sequentially and read them randomly
 */
void SeqInsertRandomReadSpeedTest(TreeType *t, size_t key_num) {
  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, static_cast<int>(key_num - 1));

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(size_t i = 0;i < key_num * 2;i++) {
    t->Insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million seq insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<long int> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    t->GetValue(key, v);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: at least " << (static_cast<double>(key_num) * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random read after seq insert/sec" << "\n";

  return;
}


/*
 * InfiniteRandomInsertTest() - Inserts in a 10M key space randomly
 *                              until user pauses
 */
void InfiniteRandomInsertTest(TreeType *t) {
  // This defines the key space (0 ~ (10M - 1))
  const size_t key_num = 1024 * 1024 * 10;

  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

  size_t count = 0;
  size_t success = 0;

  Timer timer{true};
  size_t last_count = 0;

  while(1) {
    int key = uniform_dist(e1);

    success += t->Insert(key, key);
    
    count++;
    
    if((count % (1024 * 50)) == 0) {
      double duration = timer.Stop();
      printf("%lu (%f M) iters; %lu (%f M) succ; %f M/sec  \r",
             count,
             (float)count / (1024.0 * 1024.0),
             success,
             (float)success / (1024.0 * 1024.0),
             (float)(count - last_count) / (1024.0 * 1024.0) / duration);
      
      last_count = count;
      
      // Restart the timer
      timer.Start();
    }
  }

  return;
}

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

  while(insert_success_counter.load() < key_num) {
    int key = uniform_dist(e1);

    if(t->Insert(key, key)) insert_success_counter.fetch_add(1);
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
  for(int i = 0;i < 1024 * 1024;i++) {
    auto s = t->GetValue(i);

    assert(s.size() == 1);
    assert(*s.begin() == i);
  }

  printf("Random insert test OK\n");

  return;
}
