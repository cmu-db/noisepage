#pragma once

#include <boost/coroutine2/all.hpp>
#include <utility>

namespace terrier::common {

  using pull_type = boost::coroutines2::coroutine<void>::pull_type;
  using push_type = boost::coroutines2::coroutine<void>::push_type;

  class PoolContext {
   public:
    void SetFunction(const std::function<void(PoolContext *)> &f) {
      func_ = f;
      in_ = pull_type([&] (push_type &s) {
        this->sink_ = &s;
        this->func_(this);
      });
    }

    void YieldToPool() {
      TERRIER_ASSERT(func_ != nullptr && sink_ != nullptr, "must have called SetFunction before suspend");
      (*sink_)();
    }

    bool YieldToFunc() {
      TERRIER_ASSERT(func_ != nullptr && sink_ != nullptr, "must have called SetFunction before yeilding to function");
      in_();
      return !in_;
    }

    ~PoolContext() = default;
    PoolContext() = default;

    class Allocator {
     public:
      PoolContext *New() { return new PoolContext(); }
      void Reuse(PoolContext *const reuse) {
        reuse->func_ = nullptr;
        reuse->in_ = pull_type([&] (push_type &s) {});
        reuse->sink_ = nullptr;
      }
      void Delete(PoolContext *const ptr) { delete ptr; }
    };

   private:
    std::function<void(PoolContext *)> func_ = nullptr;
    pull_type in_ = pull_type([&] (push_type &s) {});
    push_type *sink_ = nullptr;
  };


  using PoolContextPool = common::ObjectPool<PoolContext, PoolContext::Allocator>;

}  // namespace terrier::common
