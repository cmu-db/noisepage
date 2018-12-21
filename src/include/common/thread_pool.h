//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool.h
//
// Identification: src/include/common/worker_thread_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <vector>


#include "common/macros.h"

namespace terrier {
// a wrapper for boost worker thread pool.
class ThreadPool {
 public:
  ThreadPool()
      : pool_size_(0), dedicated_thread_count_(0) {}

  ~ThreadPool() {}

  void Initialize(const size_t &pool_size,
                  const size_t &dedicated_thread_count) {
    current_thread_count_ = 0;
    pool_size_ = pool_size;
    // PELOTON_ASSERT(pool_size_ != 0);

    dedicated_thread_count_ = dedicated_thread_count;

    //for (size_t i = 0; i < pool_size_; ++i) {
    //  // add thread to thread pool.
    //  thread_pool_.create_thread(
    //      boost::bind(&boost::asio::io_service::run, &io_service_));
    //}

    dedicated_threads_.resize(dedicated_thread_count_);
  }

  void Shutdown() {
    // always join lastly created threads first.
    for (size_t i = 0; i < current_thread_count_; ++i) {
      dedicated_threads_[(current_thread_count_ - 1 - i)]->join();
    }
    //thread_pool_.join_all();
  }

  // submit task to thread pool.
  // it accepts a function and a set of function parameters as parameters.
  template <typename FunctionType, typename... ParamTypes>
  void SubmitTask(FunctionType &&func, const ParamTypes &&... params) {
    // add task to thread pool.
  }

  // submit task to a dedicated thread.
  // it accepts a function and a set of function parameters as parameters.
  template <typename FunctionType, typename... ParamTypes>
  void SubmitDedicatedTask(FunctionType &&func, const ParamTypes &&... params) {
    size_t thread_id =
        current_thread_count_.fetch_add(1, std::memory_order_relaxed);
    // assign task to dedicated thread.
    dedicated_threads_[thread_id].reset(
        new std::thread(std::thread(func, params...)));
  }

 private:
  ThreadPool(const ThreadPool &);
  ThreadPool &operator=(const ThreadPool &);

 private:
  // number of threads in the thread pool.
  size_t pool_size_;
  // max number of dedicated threads.
  size_t dedicated_thread_count_;
  // current number of dedicated threads.
  std::atomic<size_t> current_thread_count_ = ATOMIC_VAR_INIT(0);

  std::vector<std::unique_ptr<std::thread>> dedicated_threads_;
};

}  // namespace peloton
