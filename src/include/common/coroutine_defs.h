#pragma once

#include <utility>
#include <boost/coroutine2/all.hpp>
#include "common/object_pool.h"

namespace terrier::common {

using pull_type = boost::coroutines2::coroutine<void>::pull_type;
using push_type = boost::coroutines2::coroutine<void>::push_type;

class PoolContext {
 public:
  void SetFunction(const std::function<void(PoolContext *)> &f) {
    TERRIER_ASSERT(func_ == nullptr, "function should be null");
    this->func_ = f;
  }

  void YieldToPool() {
    TERRIER_ASSERT(sink_ != nullptr, "must have initialized sink_ before yielding to it");
    (*sink_)();
  }

  bool YieldToFunc() {
    TERRIER_ASSERT(func_ != nullptr, "must have called SetFunction before yielding to function");
    TERRIER_ASSERT(sink_ != nullptr, "must have initialized sink_ before yielding to function");
    TERRIER_ASSERT(in_, "in_ should always have yielded");
    in_();
    TERRIER_ASSERT(in_, "in_ should always have yielded");
    return func_finished_;
  }

  ~PoolContext() = default;
  PoolContext() = default;

  class Allocator {
   public:
    PoolContext *New() { return new PoolContext(); }
    void Reuse(PoolContext *const reuse) {}
    void Delete(PoolContext *const ptr) { delete ptr; }
  };

 private:
  std::function<void(PoolContext *)> func_ = nullptr;
  push_type *sink_ = nullptr;
  pull_type in_ = pull_type([&](push_type &s) {
    this->sink_ = &s;
    while (true) {
      this->YieldToPool();
      TERRIER_ASSERT(this->func_ != nullptr, "should have initialized function before yielding to function");
      this->func_finished_ = false;
      this->func_(this);
      this->func_finished_ = true;
      this->func_ = nullptr;
    }
  });
  bool func_finished_ = false;
};

using PoolContextPool = ObjectPool<common::PoolContext, common::PoolContext::Allocator>;

}  // namespace terrier::common
