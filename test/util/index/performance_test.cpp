
/*
 * performance_test.cpp
 *
 * This includes performance test for index structures such as std::map, 
 * std::unordered_map, stx::btree and stx::btree_multimap
 */

#include "util/bwtree_test_util.h"

/*
 * TestStdMapInsertReadPerformance() - As name suggests
 */
void TestStdMapInsertReadPerformance(int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  // Insert 1 million keys into std::map
  std::map<long, long> test_map{};
  for(int i = 0;i < key_num;i++) {
    test_map[i] = i;
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "std::map: " << 1.0 * key_num / (1024 * 1024) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from std::map
    for(int i = 0;i < key_num;i++) {
      long t = test_map[i];

      v.push_back(t);
      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "std::map: " << (1.0 * iter * key_num) / (1024 * 1024) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

/*
 * TestStdUnorderedMapInsertReadPerformance() - As name suggests
 */
void TestStdUnorderedMapInsertReadPerformance(int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  // Insert 1 million keys into std::map
  std::unordered_map<long, long> test_map{};
  for(int i = 0;i < key_num;i++) {
    test_map[i] = i;
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "std::unordered_map: " << 1.0 * key_num / (1024 * 1024) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from std::map
    for(int i = 0;i < key_num;i++) {
      long t = test_map[i];

      v.push_back(t);
      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "std::unordered_map: " << (1.0 * iter * key_num) / (1024 * 1024) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

/*
 * TestBTreeInsertReadPerformance() - As name suggests
 */
void TestBTreeInsertReadPerformance(int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  // Insert 1 million keys into stx::btree
  btree<long,
        long,
        std::pair<long, long>,
        KeyComparator> test_map{KeyComparator{1}};

  start = std::chrono::system_clock::now();

  for(long i = 0;i < key_num;i++) {
    test_map.insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "stx::btree: " << 1.0 * key_num / (1024 * 1024) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read keys from stx::btree
    for(int i = 0;i < key_num;i++) {
      auto it = test_map.find(i);

      v.push_back(it->second);
      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "stx::btree " << (1.0 * iter * key_num) / (1024 * 1024) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

/*
 * TestBTreeMultimapInsertReadPerformance() - As name suggests
 *
 * This function tests btree_multimap in a sense that retrieving values
 * are not considered complete until values have all been pushed
 * into the vector. This requires getting the iterator pair first, and
 * then iterate on the interval, pushing values into the vector
 */
void TestBTreeMultimapInsertReadPerformance(int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  // Initialize multimap with a key comparator that is not trivial
  btree_multimap<long, long, KeyComparator> test_map{KeyComparator{1}};

  start = std::chrono::system_clock::now();

  // Insert 1 million keys into stx::btree_multimap
  for(long i = 0;i < key_num;i++) {
    test_map.insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "stx::btree_multimap: " << (1.0 * key_num / (1024 * 1024)) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from stx::btree
    for(int i = 0;i < key_num;i++) {
      auto it_pair = test_map.equal_range(i);

      // For multimap we need to write an external loop to
      // extract all keys inside the multimap
      // This is the place where btree_multimap is slower than
      // btree
      for(auto it = it_pair.first;it != it_pair.second;it++) {
        v.push_back(it->second);
      }

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "stx::btree_multimap " << (1.0 * iter * key_num / (1024 * 1024)) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}


/*
 * TestCuckooHashTableInsertReadPerformance() - Tests cuckoo hash table
 */
void TestCuckooHashTableInsertReadPerformance(int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  // Initialize multimap with a key comparator that is not trivial
  cuckoohash_map<long, long> test_map{};

  start = std::chrono::system_clock::now();

  // Insert 1 million keys into stx::btree_multimap
  for(long i = 0;i < key_num;i++) {
    test_map.insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "cuckoohash_map: " << (1.0 * key_num / (1024 * 1024)) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  ////////////////////////////////////////////
  // Test read
  std::vector<long> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  int iter = 10;
  for(int j = 0;j < iter;j++) {
    // Read 1 million keys from stx::btree
    for(int i = 0;i < key_num;i++) {
      long int ret;
      
      test_map.find(i, ret);

      v.push_back(ret);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "cuckoohash_map " << (1.0 * iter * key_num / (1024 * 1024)) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

/*
 * TestBwTreeInsertReadDeletePerformance() - As name suggests
 *
 * This function runs the following tests:
 *
 * 1. Sequential insert (key, key)
 * 2. Sequential read
 * 3. Forward iterate
 * 4. Backward iterate
 * 5. Reverse order insert (key, key + 1)
 * 6. Reverse order read
 * 7. Remove all values
 */
void TestBwTreeInsertReadDeletePerformance(TreeType *t, int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  // Then test read performance
  int iter = 10;
  std::vector<long> v{};

  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(int j = 0;j < iter;j++) {
    for(int i = 0;i < key_num;i++) {
      t->GetValue(i, v);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  ///////////////////////////////////////////////////////////////////
  // Test Iterator (forward, single value)
  ///////////////////////////////////////////////////////////////////
  
  start = std::chrono::system_clock::now();
  {
    for(int j = 0;j < iter;j++) {
      auto it = t->Begin();
      while(it.IsEnd() == false) {
        v.push_back(it->second);

        v.clear();
        it++;
      }
      
      it--;
      if(it->first != key_num - 1) {
        throw "Error: Forward iterating does not reach the end";
      } 
    }

    end = std::chrono::system_clock::now();

    elapsed_seconds = end - start;
    std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
              << " million forward iteration/sec" << "\n";
  }
  
  ///////////////////////////////////////////////////////////////////
  // Test Iterator (backward, single value)
  ///////////////////////////////////////////////////////////////////
  
  start = std::chrono::system_clock::now();
  {
    for(int j = 0;j < iter;j++) {
      auto it = t->Begin(key_num - 1);
      while(it.IsREnd() == false) {
        v.push_back(it->second);

        v.clear();
        it--;
      }
      
      it++;
      if(it->first != 0) {
        throw "Error: Forward iterating does not reach the beginning";
      } 
    }
    
    end = std::chrono::system_clock::now();

    elapsed_seconds = end - start;
    std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
              << " million backward iteration/sec" << "\n";
  }
  
  ///////////////////////////////////////////////////////////////////
  // Insert 2nd value
  ///////////////////////////////////////////////////////////////////
  
  start = std::chrono::system_clock::now();

  for(int i = key_num - 1;i >= 0;i--) {
    t->Insert(i, i + 1);
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million insertion (reverse order)/sec" << "\n";

  // Read again

  start = std::chrono::system_clock::now();

  for(int j = 0;j < iter;j++) {
    for(int i = 0;i < key_num;i++) {
      t->GetValue(i, v);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million read (2 values)/sec" << "\n";

  // Verify reads

  for(int i = 0;i < key_num;i++) {
    t->GetValue(i, v);

    assert(v.size() == 2);
    if(v[0] == (i)) {
      assert(v[1] == (i + 1));
    } else if(v[0] == (i + 1)) {
      assert(v[1] == (i));
    } else {
      assert(false);
    }

    v.clear();
  }

  std::cout << "    All values are correct!\n";

  // Finally remove values

  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Delete(i, i);
  }

  for(int i = key_num - 1;i >= 0;i--) {
    t->Delete(i, i + 1);
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (key_num * 2 / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million remove/sec" << "\n";

  for(int i = 0;i < key_num;i++) {
    t->GetValue(i, v);

    assert(v.size() == 0);
  }

  std::cout << "    All values have been removed!\n";

  return;
}

/*
 * TestBwTreeInsertReadPerformance() - As name suggests
 */
void TestBwTreeInsertReadPerformance(TreeType *t, int key_num) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Insert(i, i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million insertion/sec" << "\n";

  // Then test read performance

  int iter = 10;
  std::vector<long> v{};

  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(int j = 0;j < iter;j++) {
    for(int i = 0;i < key_num;i++) {
      t->GetValue(i, v);

      v.clear();
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: " << (iter * key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million read/sec" << "\n";

  return;
}

/*
 * TestBwTreeEmailInsertPerformance() - Tests insert performance on string
 *                                      workload (email)
 *
 * This function requires a special email file that is not distributed
 * publicly
 */
void TestBwTreeEmailInsertPerformance(BwTree<std::string, long int> *t,
                                      std::string filename) {
  std::ifstream email_file{filename};
  
  std::vector<std::string> string_list{};
  
  // If unable to open file
  if(email_file.good() == false) {
    std::cout << "Unable to open file: " << filename << std::endl;
    
    return;
  }
  
  int counter = 0;
  std::string s{};
  
  // Then load the line until reaches EOF
  while(std::getline(email_file, s).good() == true) {
    string_list.push_back(s);
    
    counter++;
  }
    
  printf("Successfully loaded %d entries\n", counter);
  
  ///////////////////////////////////////////////////////////////////
  // After this point we continue with insertion
  ///////////////////////////////////////////////////////////////////
  
  int key_num = counter;
  
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    t->Insert(string_list[i], i);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "BwTree: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million email insertion/sec" << "\n";
            
  print_flag = true;
  delete t;
  print_flag = false;
            
  ///////////////////////////////////////////////////////////////////
  // Then test stx::btree_multimap
  ///////////////////////////////////////////////////////////////////

  stx::btree_multimap<std::string, long int> bt{};

  start = std::chrono::system_clock::now();

  for(int i = 0;i < key_num;i++) {
    bt.insert(string_list[i], i);
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;

  std::cout << "stx::btree_multimap: " << (key_num / (1024.0 * 1024.0)) / elapsed_seconds.count()
            << " million email insertion/sec" << "\n";
            
  return;
}
