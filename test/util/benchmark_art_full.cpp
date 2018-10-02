
/*
 * benchmark_art_full.cpp - Benchmarks adaptive radix tree
 *
 * Please note that the ART we use for benchmark only supports single threaded 
 * execution, so therefore it refuses to execute if the number of thread
 * is more than 1
 */ 

#include "util/bwtree_test_util.h"

/*
 * BenchmarkARTSeqInsert() - As name suggests
 */
void BenchmarkARTSeqInsert(ARTType *t, 
                           int key_num, 
                           int num_thread,
                           long int *array) {

  // Enforce this with explicit assertion that is still valid under 
  // release mode
  if(num_thread != 1) {
    throw "ART must be run under single threaded environment!"; 
  }

  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }

  auto func = [key_num, 
               &thread_time, 
               num_thread,
               array](uint64_t thread_id, ARTType *t) {
    long int start_key = key_num / num_thread * (long)thread_id;
    long int end_key = start_key + key_num / num_thread;

    // Declare timer and start it immediately
    Timer timer{true};

    for(long int i = start_key;i < end_key;i++) {
      // 8 byte key, 8 byte payload (i.e. int *)
      // Since ART itself does not store data, we must allocate an external
      // array which occupies extra space, and also this adds one extra
      // pointer dereference
      art_insert(t, (unsigned char *)&i, sizeof(i), array + i);
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

  std::cout << num_thread << " Threads ART: overall "
            << (key_num / (1024.0 * 1024.0) * num_thread) / elapsed_seconds
            << " million insert/sec" << "\n";
            
  return;
}


/*
 * BenchmarkARTSeqRead() - As name suggests
 */
void BenchmarkARTSeqRead(ARTType *t, 
                         int key_num,
                         int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  auto func = [key_num, 
               iter, 
               &thread_time, 
               num_thread](uint64_t thread_id, ARTType *t) {
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        long int temp = *(long int *)art_search(t, (unsigned char *)&i, sizeof(i));
        v.push_back(temp);
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

  std::cout << num_thread << " Threads ART: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read/sec" << "\n";

  return;
}


/*
 * BenchmarkARTRandRead() - As name suggests
 */
void BenchmarkARTRandRead(ARTType *t, 
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
                num_thread](uint64_t thread_id, ARTType *t) {
    std::vector<long> v{};

    v.reserve(1);
    
    // This is the random number generator we use
    SimpleInt64Random<0, 30 * 1024 * 1024> h{};

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        //int key = uniform_dist(e1);
        long int key = (long int)h((uint64_t)i, thread_id);

        long int temp = \
          *(long int *)art_search(t, (unsigned char *)&key, sizeof(key));
        v.push_back(temp);
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

  std::cout << num_thread << " Threads ART: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read (random)/sec" << "\n";

  return;
}

/*
 * BenchmarkARTZipfRead() - As name suggests
 */
void BenchmarkARTZipfRead(ARTType *t, 
                          int key_num,
                          int num_thread) {
  int iter = 1;
  
  // This is used to record time taken for each individual thread
  double thread_time[num_thread];
  for(int i = 0;i < num_thread;i++) {
    thread_time[i] = 0.0;
  }
  
  // Declear and initialize Zipfian dist
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
                num_thread](uint64_t thread_id, ARTType *t) {
    std::vector<long> v{};

    v.reserve(1);

    Timer timer{true};

    for(int j = 0;j < iter;j++) {
      for(long int i = 0;i < key_num;i++) {
        long int key = zipfian_key_list[i];
        long int temp = \
          *(long int *)art_search(t, (unsigned char *)&key, sizeof(key));
        v.push_back(temp);
        v.clear();
      }
    }

    double duration = timer.Stop();

    std::cout << "[Thread " << thread_id << " Done] @ " \
              << (iter * key_num / (1024.0 * 1024.0)) / duration \
              << " million read (zipfian)/sec" << "\n";
              
    thread_time[thread_id] = duration;

    return;
  };

  LaunchParallelTestID(nullptr, num_thread, func2, t);

  double elapsed_seconds = 0.0;
  for(int i = 0;i < num_thread;i++) {
    elapsed_seconds += thread_time[i];
  }

  std::cout << num_thread << " Threads ART: overall "
            << (iter * key_num / (1024.0 * 1024.0) * num_thread * num_thread) / elapsed_seconds
            << " million read (zipfian)/sec" << "\n";

  return;
}
