#include <queue>
#include <random>
#include <vector>

namespace noisepage::common {

template <class Value>
class ReservoirSampling {
 public:
  template <class ValueKey>
  class Key {
   public:
    double priority_;
    ValueKey value_;

    Key(double priority, const ValueKey &value) : priority_(priority), value_(value) {}
  };

  template <class ValueKey>
  struct KeyCmp {
    constexpr bool operator()(const Key<ValueKey> &lhs, const Key<ValueKey> &rhs) {
      return lhs.priority_ > rhs.priority_;
    }
  };

  explicit ReservoirSampling(size_t k) : limit_(k), dist_(0, 1) {}

  void AddSample(Value val) {
    double priority = dist_(generator_);
    if (queue_.size() < limit_) {
      queue_.emplace(priority, val);
    } else if (priority > queue_.top().priority_) {
      queue_.pop();
      queue_.emplace(priority, val);
    }
  }

  void AddSample(Key<Value> key) {
    double priority = key.priority_;
    if (queue_.size() < limit_) {
      queue_.emplace(priority, key.value_);
    } else if (priority > queue_.top().priority_) {
      queue_.pop();
      queue_.emplace(priority, key.value_);
    }
  }

  void Merge(ReservoirSampling<Value> &other) {
    while (!other.queue_.empty()) {
      AddSample(queue_.top());
      queue_.pop();
    }
  }

  std::vector<Value> GetSamples() {
    std::vector<Value> samples;
    samples.reserve(limit_);
    while (!queue_.empty()) {
      samples.emplace_back(queue_.top().value_);
      queue_.pop();
    }

    return samples;
  }

 private:
  size_t limit_;
  std::priority_queue<Key<Value>, std::vector<Key<Value>>, KeyCmp<Value>> queue_;
  std::mt19937 generator_;
  std::uniform_real_distribution<double> dist_;
};

}  // namespace noisepage::common
