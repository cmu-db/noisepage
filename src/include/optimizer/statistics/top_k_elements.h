#pragma once

#include <cmath>
#include <cassert>
#include <cinttypes>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <stack>
#include <queue>
#include <vector>
#include <utility>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/statistics/count_min_sketch.h"


namespace terrier::optimizer {

/**
 * Providing Top K Elements retrieval functions
 * and bookkeeping of Top K Elements
 */
template <typename KeyType>
class TopKElements {
  /**
   *
   */
  using ValueFrequencyPair = std::pair<KeyType, double>;

 public:
  /**
   * Class Declarations
   */
  class ApproxTopEntry;
  template <class T, class Container, class Compare>
  class UpdatableQueue;

  /**
   * An entry of the TopKQueue data structure
   * Working as a pair of element and count
   */
  class ApproxTopEntry {
   public:
    ApproxTopEntry(KeyType key, int64_t freq)
        : key_{key}, count_{freq} {}

    /**
     * Overriding operator ==
     * @param other
     * @return
     */
    bool operator==(const ApproxTopEntry& other) const {
      // just check the elem, not the count
      return key_ == other.key_;
    }

    /**
     *
     * @return
     */
    KeyType GetKey() const { return key_; }

    /**
     *
     * @return
     */
    int64_t GetCount() const { return count_; }

    /**
     * Pretty Print!
     * @param os
     * @param ate
     * @return
     */
    friend std::ostream &operator<<(std::ostream &os, const ApproxTopEntry &ate) {
      os << "{key=" << ate.key_ << ", count=" << ate.count_ << "}";
      return os;
    }

   private:
    KeyType key_;
    int64_t count_;

  };  // end of class ApproxTopEntry

  /**
   * Customized priority_queue.
   * Containing functions with access to the underlying
   * container (for e.g. vector).
   * @tparam T
   * @tparam Container
   * @tparam Compare
   */
  template <class T, class Container = std::vector<T>,
            class Compare = std::less<typename Container::value_type> >
  class UpdatableQueue : public std::priority_queue<T, Container, Compare> {
   public:
    typedef typename std::priority_queue<
        T, Container, Compare>::container_type::const_iterator const_iterator;

    /**
     * Check if a value exists in the queue
     * @param val
     * @return
     */
    bool Exists(const T& val) {
      auto first = this->c.cbegin();
      auto last = this->c.cend();
      while (first != last) {
        if (*first == val) {
          return true;
        }
        ++first;
      }
      return false;
    }

    /**
     * Remove a value from the queue
     * @param val
     * @return
     */
    bool Remove(const T& val) {
      // find the former entry first
      auto first = this->c.begin();
      auto last = this->c.end();
      auto idx_iter = last;

      while (first != last) {
        if (*first == val) {
          idx_iter = first;
        }
        ++first;
      }
      // if existing, remove it
      if (idx_iter != this->c.end()) {
        this->c.erase(idx_iter);
        std::make_heap(this->c.begin(), this->c.end(), this->comp);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Retrieve all the items in the queue, unordered
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveAll() const {
      std::vector<ApproxTopEntry> vec;
      const_iterator first = this->c.cbegin(), last = this->c.cend();
      while (first != last) {
        vec.push_back(*first);
        ++first;
      }
      return vec;
    }

    /**
     * Retrieve all the items in the queue, ordered, queue is maintained
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveAllOrdered() {
      std::vector<ApproxTopEntry> vec;
      while (!this->empty()) {
        vec.push_back(this->top());
        this->pop();
      }
      auto first = this->c.begin(), last = this->c.end();
      while (first != last) {
        this->push(*first);
        ++first;
      }
      return vec;
    }

    /**
     * Retrieve all elements, ordered.
     * Max first.
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() const {
      std::vector<ApproxTopEntry> vec;
      const_iterator first = this->c.cbegin(), last = this->c.cend();
      while (first != last) {
        vec.push_back(*first);
        ++first;
      }
      return vec;

//      std::stack<ApproxTopEntry> stack;
//      std::vector<ApproxTopEntry> vec_ret;
//      while (!this->empty()) {
//        stack.push(this->top());
//        this->pop();
//      }
//
//      while (stack.size() != 0) {
//        this->push(stack.top());
//        vec_ret.push_back(stack.top());
//        stack.pop();
//      }
//      return vec_ret;
    }

    /**
     * Retrieve given number of elements, ordered
     * Max first
     * @param num
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) const {
      std::vector<ApproxTopEntry> vec;
      const_iterator first = this->c.cbegin(), last = this->c.cend();
      int i = 0;
      while (first != last) {
        vec.push_back(*first);
        ++first;
        if (i++ == num) break;
      }
      return vec;

      // This shit is crazy!
      // I have no idea why we are doing it like this!
//      std::stack<ApproxTopEntry> stack;
//      std::vector<ApproxTopEntry> vec_ret;
//      int i = 0;
//      while (!this->empty()) {
//        stack.push(this->top());
//        this->pop();
//      }
//
//      while (stack.size() != 0) {
//        this->push(stack.top());
//        if (i < num) {
//          ++i;
//          vec_ret.push_back(stack.top());
//        }
//        stack.pop();
//      }
    }

    /**
     * Retrieve all elements, ordered
     * Max first
     * Return a vector of ValueFrequencyPair
     * @return
     */
    std::vector<ValueFrequencyPair> GetAllOrderedMaxFirst() {
      std::stack<ApproxTopEntry> stack;
      std::vector<ValueFrequencyPair> vec_ret;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        ApproxTopEntry t = stack.top();
        this->push(t);
        vec_ret.push_back(std::make_pair(t.GetKey(), (double)t.GetCount()));
        stack.pop();
      }
      return vec_ret;
    }

    /*
     * Retrieve given number of elements, ordered
     * Max first
     * Return a vector of ValueFrequencyPair
     */
    std::vector<ValueFrequencyPair> GetOrderedMaxFirst(int num) {
      std::stack<ApproxTopEntry> stack;
      std::vector<ValueFrequencyPair> vec_ret;
      int i = 0;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        ApproxTopEntry t = stack.top();
        this->push(t);
        if (i < num) {
          ++i;
          vec_ret.push_back(std::make_pair(t.GetKey(), (double)t.GetCount()));
        }
        stack.pop();
      }
      return vec_ret;
    }

  };  // end of UpdatableQueue

  /**
   * TopKElements Constructor
   * @param k
   * @param width the size of the underlying sketch
   */
  explicit TopKElements(int k, uint64_t width) : numk_{k}, size_{0} {
    sketch_ = new CountMinSketch<KeyType>(width);
  }

  ~TopKElements() { delete sketch_; }

  /**
   * Add an item into this bookkeeping data structure as well as
   * the sketch
   * @param item
   * @param count
   */
  void Add(KeyType item, int count) {
    // Increment the count for this item in the Count-Min sketch
    sketch_->Add(item, count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntry e(item, sketch_->EstimateItemCount(item));
    AddFreqItem(e);
  }

  /**
   * Remove an item from the TopKQueue (as well as the sketch)
   * @param item
   * @param count
   */
  void Remove(KeyType key, int count) {
    sketch_->Remove(key, count);
    ApproxTopEntry e(key, sketch_->EstimateItemCount(key));
    DecrFreqItem(e);
  }

  /**
   *
   * @param key
   * @return
   */
  uint64_t EstimateItemCount(const KeyType &key) const {
    return sketch_->EstimateItemCount(key);
  }

  /**
   * @return
   */
  int GetK() const { return numk_; }

  /**
   * @return
   */
  int GetSize() const { return size_; }

  /**
   * Retrieve all the items in the queue, unordered
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveAll() const { return queue_.RetrieveAll(); }

  /**
   * Retrieve all the items in the queue, ordered, queue is maintained
   * small count to large count (min first)
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveAllOrdered() const {
    return queue_.RetrieveAllOrdered();
  }

  /**
   * Retrieve all the items in the queue, ordered, queue is maintained
   * large count to small count (max first)
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() const {
    return queue_.RetrieveAllOrderedMaxFirst();
  }

  /**
   * Retrieve given number of elements, ordered
   * Max first
   * @param num
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) const {
    if (num >= numk_) return queue_.RetrieveAllOrderedMaxFirst();
    return queue_.RetrieveOrderedMaxFirst(num);
  }

  /**
   * Retrieve all the items in the queue, ordered, queue is maintained
   * large count to small count (max first)
   * @return
   */
  std::vector<ValueFrequencyPair> GetAllOrderedMaxFirst() {
    return queue_.GetAllOrderedMaxFirst();
  }

  /**
   * Retrieve given number of elements, ordered
   * Max first
   * @param num
   * @return
   */
  std::vector<ValueFrequencyPair> GetOrderedMaxFirst(int num) {
    if (num >= numk_) return queue_.GetAllOrderedMaxFirst();
    return queue_.GetOrderedMaxFirst(num);
  }

  /**
   *
   * @param os
   * @param topk
   * @return
   */
  friend std::ostream &operator<<(std::ostream &os, const TopKElements<KeyType> &topK) {
    std::vector<ApproxTopEntry> vec = topK.RetrieveOrderedMaxFirst(topK.GetK());

    os << "Top-" << topK.GetK() << " [size=" << topK.GetSize() << "]";
    int i = 0;
    for (auto const& e : vec) {
      os << std::endl << "  (" << i++ << ") " << e;
    }

    return os;
  }

//  /*
//   * Print the entire queue, unordered
//   */
//  void PrintTopKQueue() const {
//    std::vector<ApproxTopEntry> vec = RetrieveAll();
//    for (auto const& e : vec) {
//      OPTIMIZER_LOG_INFO("\n [PrintTopKQueue Entries] %s", e.print().c_str());
//    }
//  }
//
//  /*
//   * Print the entire queue, ordered
//   * Min count first
//   * Queue is empty afterwards
//   */
//  void PrintTopKQueuePops() {
//    while (queue_.GetSize() != 0) {
//      OPTIMIZER_LOG_INFO("\n [PrintTopKQueuePops Entries] %s", queue_.Pop().print().c_str());
//    }
//  }
//
//  /*
//   * Print the entire queue, ordered
//   * Min count first
//   * Queue is intact
//   */
//  void PrintTopKQueueOrdered() {
//    std::vector<ApproxTopEntry> vec = RetrieveAllOrdered();
//    for (auto const& e : vec) {
//      OPTIMIZER_LOG_INFO("\n [Print k Entries] %s", e.print().c_str());
//    }
//  }

  /*
   * Print the entire queue, ordered
   * Max count first
   * Queue is intact
   */
//  void PrintTopKQueueOrderedMaxFirst() {
//    std::vector<ApproxTopEntry> vec = RetrieveAllOrderedMaxFirst();
//    for (auto const& e : vec) {
//      OPTIMIZER_LOG_INFO("\n [Print k Entries MaxFirst] %s", e.print().c_str());
//    }
//  }

//  /*
//   * Print the entire queue, ordered
//   * Max count first
//   * Queue is intact
//   */
//  void PrintAllOrderedMaxFirst() {
//    std::vector<ValueFrequencyPair> vec = GetAllOrderedMaxFirst();
//    for (auto const& p : vec) {
//      OPTIMIZER_LOG_INFO("\n [Print k Values MaxFirst] %s, %f", p.first.GetInfo().c_str(),
//               p.second);
//    }
//  }
//
//  /*
//   * Print a given number of elements in the top k queue, ordered
//   * Max count first
//   * Queue is intact
//   * Retriving a vector of ValueFrequencyPair
//   */
//  void PrintOrderedMaxFirst(int num) {
//    std::vector<ValueFrequencyPair> vec = GetOrderedMaxFirst(num);
//    for (auto const& p : vec) {
//      OPTIMIZER_LOG_INFO("\n [Print %d Values MaxFirst] %s, %f", num,
//               p.first.GetInfo().c_str(), p.second);
//    }
//  }

 private:

  /**
   * struct for comparison in the queue
   * lowest count comes in the top (MinHeap)
   */
  struct compare {
    bool operator()(const ApproxTopEntry& l, const ApproxTopEntry& r) {
      return l.GetCount() > r.GetCount();
    }
  };

  /**
   *
   */
  CountMinSketch<KeyType> *sketch_;

  /**
   * the K as in "top K"
   */
  int numk_;

  /**
   * the priority queue managing the top entries
   */
  UpdatableQueue<ApproxTopEntry, std::vector<ApproxTopEntry>, compare> queue_;

  /**
   * number of entries in the priority queue
   */
  int size_;


  /**
   * Push an entry onto the queue
   * @param entry
   */
  void Push(ApproxTopEntry entry) {
    // here we suppose the is_exist returns false
    // if less than K-items, just insert, increase the size
    if (size_ < numk_) {
      queue_.push(entry);
      size_++;
    }
      // if we have more than K-items (including K),
      // remove the item with the lowest frequency from our data structure
    else {
      // if and only if the lowest frequency is lower than the new one
      if (queue_.top().GetCount() < entry.GetCount()) {
        queue_.pop();
        queue_.push(entry);
      }
    }
  }

  /**
   * Update a designated entry
   * @param entry
   */
  void Update(ApproxTopEntry entry) {
    // remove the former one (with the same entry elem) and insert the new
    // entry
    // first remove the former entry
    if (queue_.Remove(entry)) {
      queue_.push(entry);
    }
  }

  /**
   * Remove a designated entry
   * @param entry
   * @return
   */
  bool Remove(ApproxTopEntry entry) {
    auto result = queue_.remove(entry);
    if (result) size_--;
    return result;
  }

  /**
   * Pop the top element of the queue
   */
  ApproxTopEntry Pop() {
    // This is oh-so *not* thread safe!
    auto entry = std::move(queue_.top());
    queue_.pop();
    size_--;
    return entry;
  }

  /**
   * Add the frequency (approx count) and item (Element) pair (ApproxTopEntry)
   * to the queue / update tkq structure
   * @param entry
   */
  void AddFreqItem(ApproxTopEntry& entry) {
    // If we have more than K-items, remove the item with the
    // lowest frequency from our data structure.
    // If freq_item was already in our data structure, just
    // update it instead.

    if (!queue_.Exists(entry)) {
      // not in the structure
      // insert it
      Push(entry);
    } else {
      // if in the structure update
      Update(entry);
    }
  }

  /**
   * Decrease / Remove
   * @param entry
   */
  void DecrFreqItem(ApproxTopEntry& entry) {
    if (!queue_.Exists(entry)) {
      // not in the structure
      // do nothing
    } else {
      // if in the structure
      // update
      if (entry.GetCount() == 0) {
        queue_.Remove(entry);
      } else {
        queue_.update(entry);
      }
    }
  }

};

}  // namespace terrier::optimizer
