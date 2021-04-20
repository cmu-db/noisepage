#include <queue>
#include <random>
#include <utility>
#include <vector>

namespace noisepage::common {

/**
 * Implementation of reservoir sampling. This is done in a "streaming" style where
 * we want to sample k elements from an unknown population, where elements are
 * being inserted "streaming" style.
 */
template <class Value>
class ReservoirSampling {
 public:
  /**
   * Describes a key (comprising a priority and a data element)
   */
  template <class ValueKey>
  class Key {
   public:
    /** Internal priority counter of the key */
    double priority_;
    /** Data member being stored */
    ValueKey value_;

    /**
     * Constructs a key element for the reservoir
     * @param priority Priority of the given element (higher is more likely to stay)
     * @param value Value of the element
     */
    Key(double priority, ValueKey value) : priority_(priority), value_(std::move(value)) {}
  };

  /**
   * Comparator for key
   */
  template <class ValueKey>
  struct KeyCmp {
    /** Implements the greater than operator */
    constexpr bool operator()(const Key<ValueKey> &lhs, const Key<ValueKey> &rhs) {
      return lhs.priority_ > rhs.priority_;
    }
  };

  /**
   * Initializes a reservoir sample
   * @param k Number of elements to sample
   */
  explicit ReservoirSampling(size_t k) : limit_(k), dist_(0, 1) {}

  /**
   * Adds an element for consideration
   * @param val Value being considered
   */
  void AddSample(Value val) {
    double priority = dist_(generator_);
    if (queue_.size() < limit_) {
      queue_.emplace(priority, val);
    } else if (priority > queue_.top().priority_) {
      queue_.pop();
      queue_.emplace(priority, val);
    }
  }

  /**
   * Adds an element for consideration
   * @param key Key being considered
   */
  void AddSample(Key<Value> key) {
    double priority = key.priority_;
    if (queue_.size() < limit_) {
      queue_.emplace(priority, key.value_);
    } else if (priority > queue_.top().priority_) {
      queue_.pop();
      queue_.emplace(priority, key.value_);
    }
  }

  /**
   * Merge another reservoir sample into this one
   * @param other Other reservoir to merge
   */
  void Merge(ReservoirSampling<Value> *other) {
    while (!other->queue_.empty()) {
      AddSample(other->queue_.top());
      other->queue_.pop();
    }
  }

  /**
   * Get all samples. This is a destructive function.
   * The reservoir will be depleted after this.
   *
   * @return samples
   */
  std::vector<Value> TakeSamples() {
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
