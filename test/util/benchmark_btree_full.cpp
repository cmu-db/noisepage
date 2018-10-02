
/*
 * benchmark_btree_full.cpp - This file contains test suites for command
 *                            benchmark-btree-full
 *
 * Requires stx::btree_multimap and spinlock in ./benchmark directory
 */

#include "util/bwtree_test_util.h"
#include "bwtree/spinlock/spinlock.h"

// Only use boost for full-speed build
#ifdef NDEBUG 
// For boost reference:
//   http://www.boost.org/doc/libs/1_58_0/doc/html/thread/synchronization.html#thread.synchronization.mutex_types.shared_mutex
#define USE_BOOST
#pragma message "Using boost::thread for shared_mutex"
#endif

#ifdef USE_BOOST
#include <boost/thread/shared_mutex.hpp>
using namespace boost;
#endif

/*
 * BenchmarkBwTreeSeqInsert() - As name suggests
 */
void BenchmarkBTreeSeqInsert(BTreeType *t, 
                             int key_num, 
                             int num_thread) {

  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  #ifndef USE_BOOST
  // Declear a spinlock protecting the data structure
  spinlock_t lock;
  rwlock_init(lock);
  #else
  shared_mutex lock;
  #endif

  auto func = [key_num, 
               &thread_time, 
               num_thread,
               &lock](uint64_t thread_id, BTreeType *t) {
    long int start_key = key_num / num_thread * (long)thread_id;
    long int end_key = start_key + key_num / num_thread;

    // Declare timer and start it immediately
    Timer timer{true};

    for(long int i = start_key;i < end_key;i++) {
      #ifndef USE_BOOST
      write_lock(lock);
      #else
      lock.lock();
      #endif
      
      t->insert(i, i);
      
      #ifndef USE_BOOST
      write_unlock(lock);
      #else
      lock.unlock();
      #endif
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (key_num / num_thread) / (1024.0 * 1024.0) / duration \
              << " million insert/sec" << "\n";

    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (key_num / (1024.0 * 1024.0) * num_thread) / elapsed_seconds
            << " million insert/sec" << "\n";
            
  return;
}

/*
 * BenchmarkBTreeSeqRead() - As name suggests
 */
void BenchmarkBTreeSeqRead(BTreeType *t, 
                           int key_num,
                           int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  #ifndef USE_BOOST
  // Declear a spinlock protecting the data structure
  spinlock_t lock;
  rwlock_init(lock);
  #else
  shared_mutex lock;
  #endif
  
  auto func = [key_num, 
               iter, 
               &thread_time, 
               &lock,
               num_thread](uint64_t thread_id, BTreeType *t) {
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        #ifndef USE_BOOST
        read_lock(lock);
        #else
        lock.lock_shared();
        #endif
        
        auto it_pair = t->equal_range(i);

        // For multimap we need to write an external loop to
        // extract all keys inside the multimap
        // This is the place where btree_multimap is slower than
        // btree
        for(auto it = it_pair.first;it != it_pair.second;it++) {
          v.push_back(it->second);
        }
        
        #ifndef USE_BOOST
        read_unlock(lock);
        #else
        lock.unlock_shared();
        #endif
  
        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (iter * key_num / (1024.0 * 1024.0)) / duration \
              << " million read/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func, t);
  
  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read/sec" << "\n";

  return;
}

/*
 * BenchmarkBTreeRandRead() - As name suggests
 */
void BenchmarkBTreeRandRead(BTreeType *t, 
                            int key_num,
                            int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  #ifndef USE_BOOST
  // Declear a spinlock protecting the data structure
  spinlock_t lock;
  rwlock_init(lock);
  #else
  shared_mutex lock;
  #endif
  
  auto func2 = [key_num, 
                iter, 
                &thread_time,
                &lock,
                num_thread](uint64_t thread_id, BTreeType *t) {
    std::vector<long> v{};

    v.reserve(1);
    
    // This is the random number generator we use
    SimpleInt64Random<0, 30 * 1024 * 1024> h{};

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        //int key = uniform_dist(e1);
        long int key = (long int)h((uint64_t)i, thread_id);

        #ifndef USE_BOOST
        read_lock(lock);
        #else
        lock.lock_shared();
        #endif
        
        auto it_pair = t->equal_range(key);

        // For multimap we need to write an external loop to
        // extract all keys inside the multimap
        // This is the place where btree_multimap is slower than
        // btree
        for(auto it = it_pair.first;it != it_pair.second;it++) {
          v.push_back(it->second);
        }
        
        #ifndef USE_BOOST
        read_unlock(lock);
        #else
        lock.unlock_shared();
        #endif
  
        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (iter * key_num / (1024.0 * 1024.0)) / duration \
              << " million read (random)/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func2, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read (random)/sec" << "\n";

  return;
}

/*
 * BenchmarkBTreeRandLocklessRead() - As name suggests
 */
void BenchmarkBTreeRandLocklessRead(BTreeType *t, 
                                    int key_num,
                                    int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  auto func2 = [key_num, 
                iter, 
                &thread_time,
                num_thread](uint64_t thread_id, BTreeType *t) {
    std::vector<long> v{};

    v.reserve(1);
    
    // This is the random number generator we use
    SimpleInt64Random<0, 30 * 1024 * 1024> h{};

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        //int key = uniform_dist(e1);
        long int key = (long int)h((uint64_t)i, thread_id);
        
        auto it_pair = t->equal_range(key);

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

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (iter * key_num / (1024.0 * 1024.0)) / duration \
              << " million read (random)/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func2, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read (random, lockless)/sec" << "\n";

  return;
}



/*
 * BenchmarkBTreeZipfRead() - As name suggests
 */
void BenchmarkBTreeZipfRead(BTreeType *t, 
                            int key_num,
                            int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  #ifndef USE_BOOST
  // Declear a spinlock protecting the data structure
  spinlock_t lock;
  rwlock_init(lock);
  #else
  shared_mutex lock;
  #endif
  
  // Generate zipfian distribution into this list
  std::vector<long> zipfian_key_list{};
  zipfian_key_list.reserve(key_num);
  
  // Initialize it with time() as the random seed
  Zipfian zipf{(uint64_t)key_num, 0.99, (uint64_t)time(NULL)};
  
  // Populate the array with random numbers 
  for(int i = 0;i < key_num;i++) {
    zipfian_key_list.push_back(zipf.Get()); 
  }
  
  auto func2 = [key_num, 
                iter, 
                &thread_time,
                &lock,
                &zipfian_key_list,
                num_thread](uint64_t thread_id, BTreeType *t) {
    // This is the start and end index we read into the zipfian array
    long int start_index = key_num / num_thread * (long)thread_id;
    long int end_index = start_index + key_num / num_thread;
    
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = start_index;i < end_index;i++) {
        long int key = zipfian_key_list[i];

        #ifndef USE_BOOST
        read_lock(lock);
        #else
        lock.lock_shared();
        #endif
        
        auto it_pair = t->equal_range(key);

        // For multimap we need to write an external loop to
        // extract all keys inside the multimap
        // This is the place where btree_multimap is slower than
        // btree
        for(auto it = it_pair.first;it != it_pair.second;it++) {
          v.push_back(it->second);
        }
        
        #ifndef USE_BOOST
        read_unlock(lock);
        #else
        lock.unlock_shared();
        #endif
  
        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (static_cast<double>(iter * (end_index - start_index)) / (1024.0 * 1024.0)) / duration \
              << " million read (zipfian)/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func2, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (iter * key_num / (1024.0 * 1024.0)) / (elapsed_seconds / num_thread)
            << " million read (zipfian)/sec" << "\n";

  return;
}

/*
 * BenchmarkBTreeZipfLockLessRead() - As name suggests
 *
 * In this benchmark we just let it fire without any lock
 */
void BenchmarkBTreeZipfLockLessRead(BTreeType *t, 
                                    int key_num,
                                    int num_thread) {
  int iter = 1;
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  std::vector<long> zipfian_key_list{};
  zipfian_key_list.reserve(key_num);
  Zipfian zipf{(uint64_t)key_num, 0.99, (uint64_t)time(NULL)};
  for(int i = 0;i < key_num;i++) {
    zipfian_key_list.push_back(zipf.Get()); 
  }
  
  auto func2 = [key_num, 
                iter, 
                &thread_time,
                &zipfian_key_list,
                num_thread](uint64_t thread_id, BTreeType *t) {
    long int start_index = key_num / num_thread * (long)thread_id;
    long int end_index = start_index + key_num / num_thread;
    
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = start_index;i < end_index;i++) {
        long int key = zipfian_key_list[i];
        auto it_pair = t->equal_range(key);
        for(auto it = it_pair.first;it != it_pair.second;it++) {
          v.push_back(it->second);
        }
        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (static_cast<double>(iter * (end_index - start_index)) / (1024.0 * 1024.0)) / duration \
              << " million read (zipfian, lockless)/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func2, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads BTree Multimap: overall "
            << (iter * key_num / (1024.0 * 1024.0)) / (elapsed_seconds / num_thread)
            << " million read (zipfian, lockless)/sec" << "\n";

  return;
}
