#pragma once

#include <boost/coroutine2/all.hpp>

namespace terrier::common {

  class PoolContext {
   public:
    void WaitForFinsh() { promise_.get_future().get(); }

    void SetFunction(std::function<void(PoolContext *)> f) {
      func_ = f;
      in_ = boost::coroutines2::coroutine<void>::pull_type([&] (boost::coroutines2::coroutine<void>::push_type &s) {
        this->sink_ = &s;
        this->func_(this);
      });
    }

    void YieldToPool() {
      (*sink_)();
    }

    bool YieldToFunc() {
      in_();
      return !in_;
    }

    ~PoolContext() = default;
    PoolContext(std::function<void(PoolContext *)> f) {
      in_ = boost::coroutines2::coroutine<void>::pull_type([&] (boost::coroutines2::coroutine<void>::push_type &s) {
        this->sink_ = &s;
        this->func_(this);
      });
    }
   private:
    std::function<void(PoolContext *)> func_ = nullptr;
    boost::coroutines2::coroutine<void>::pull_type in_ = boost::coroutines2::coroutine<void>::pull_type([&] (boost::coroutines2::coroutine<void>::push_type &s) {
      this->sink_ = &s;
      this->func_(this);
    });
    boost::coroutines2::coroutine<void>::push_type *&sink_ = nullptr;
    std::promise<void> promise_;
  };



}  // namespace terrier::common
