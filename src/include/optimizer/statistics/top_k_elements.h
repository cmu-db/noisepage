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
  class TopKQueue;

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
    std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() {
      std::stack<ApproxTopEntry> stack;
      std::vector<ApproxTopEntry> vec_ret;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        this->push(stack.top());
        vec_ret.push_back(stack.top());
        stack.pop();
      }
      return vec_ret;
    }

    /**
     * Retrieve given number of elements, ordered
     * Max first
     * @param num
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) {
      std::stack<ApproxTopEntry> stack;
      std::vector<ApproxTopEntry> vec_ret;
      int i = 0;
      while (!this->empty()) {
        stack.push(this->top());
        this->pop();
      }

      while (stack.size() != 0) {
        this->push(stack.top());
        if (i < num) {
          ++i;
          vec_ret.push_back(stack.top());
        }
        stack.pop();
      }
      return vec_ret;
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
        vec_ret.push_back(std::make_pair(t.key_, (double)t.count_));
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
          vec_ret.push_back(std::make_pair(t.key_, (double)t.count_));
        }
        stack.pop();
      }
      return vec_ret;
    }

  };  // end of UpdatableQueue

  /**
   * TopKQueue
   * This wraps a customized priority queue
   * for the top-k approximate entries
   */
  class TopKQueue {
   public:

    /**
     * struct for comparison in the queue
     * lowest count comes in the top (MinHeap)
     */
    struct compare {
      bool operator()(const ApproxTopEntry& l, const ApproxTopEntry& r) {
        return l.count_ > r.count_;
      }
    };

    /**
     * Constructor
     * @param param_k
     */
    TopKQueue(int param_k)
        : k{param_k},
          size{0} {
    }

    /**
     *
     * @return
     */
    int GetK() { return this->k; }

    /**
     *
     * @return
     */
    int GetSize() { return this->size; }

    /**
     *
     */
    void IncreaseSize() { size++; }

    /**
     *
     */
    void DecreaseSize() { size--; }

    /**
     * Check if the entry exists in the queue
     * @param entry
     * @return
     */
    bool Exists(ApproxTopEntry entry) { return queue.Exists(entry); }

    /**
     *
     * @param entry
     */
    void Push(ApproxTopEntry entry) {
      // here we suppose the Exists returns false
      // if less than K-items, just insert, increase the size
      if (size < k) {
        queue.push(entry);
        IncreaseSize();
      }
      // if we have more than K-items (including K),
      // remove the item with the lowest frequency from our data structure
      else {
        // if and only if the lowest frequency is lower than the new one
        if (queue.top().count_ < entry.count_) {
          queue.pop();
          queue.push(entry);
        }
      }
    }

    /**
     * Update a designated entry
     * @param entry
     */
    void Update(ApproxTopEntry entry) {
      // remove the former one (with the same entry elem) and
      // insert the new entry
      // first remove the former entry
      if (queue.remove(entry)) {
        queue.push(entry);
      }
    }

    /**
     * Remove a designated entry
     * @param entry
     * @return
     */
    bool Remove(ApproxTopEntry entry) {
      DecreaseSize();
      return queue.remove(entry);
    }

    /**
     * Pop the top element of the queue
     * @return
     */
    ApproxTopEntry Pop() {
      DecreaseSize();
      ApproxTopEntry e = std::move(queue.top());
      queue.pop();
      return e;
    }

    /**
     * Retrieve all the items in the queue, unordered
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveAll() const {
      return queue.RetrieveAll();
    }

    /**
     * Retrieve all the items in the queue, ordered, queue is maintained
     * Min first (smallest count first)
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveAllOrdered() {
      return queue.RetrieveAllOrdered();
    }

    /**
     * Retrieve all elements, ordered
     * Max first
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() {
      return queue.RetrieveAllOrderedMaxFirst();
    }

    /**
     * Retrieve given number of elements, ordered
     * Max first
     * @param num
     * @return
     */
    std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) {
      return queue.RetrieveOrderedMaxFirst(num);
    }

    /**
     * Retrieve all elements, ordered
     * Max first
     * @return
     */
    std::vector<ValueFrequencyPair> GetAllOrderedMaxFirst() {
      return queue.GetAllOrderedMaxFirst();
    }

    /**
     * Retrieve given number of elements, ordered
     * Max first
     * @param num
     * @return
     */
    std::vector<ValueFrequencyPair> GetOrderedMaxFirst(int num) {
      return queue.GetOrderedMaxFirst(num);
    }

   private:
    // the K as in "top K"
    int k;
    // the priority queue managing the top entries
    UpdatableQueue<ApproxTopEntry, std::vector<ApproxTopEntry>, compare> queue;
    // number of entries in the priority queue
    int size;

  };  // end of class TopKQueue

  /**
   * TopKElements Constructor
   * @param k
   */
  explicit TopKElements(int k) : tkq{k} {
    cmsketch = new CountMinSketch<KeyType>(20);
  }

  ~TopKElements() { delete cmsketch; }

  /**
   * Add an item into this bookkeeping data structure as well as
   * the sketch
   * @param item
   * @param count
   */
  void Add(KeyType item, int count) {
    // Increment the count for this item in the Count-Min sketch
    cmsketch.Add(item, count);

    // Estimate the frequency of this item using the sketch
    // Add it to the queue
    ApproxTopEntry e(item, cmsketch.EstimateItemCount(item));
    AddFreqItem(e);
  }

  /**
   * Remove an item from the TopKQueue (as well as the sketch)
   * @param item
   * @param count
   */
  void Remove(KeyType key, int count) {
    cmsketch.Remove(key, count);
    ApproxTopEntry e(key, cmsketch.EstimateItemCount(key));
    DecrFreqItem(e);
  }

  uint64_t EstimateItemCount(const KeyType &key) {
    return cmsketch->EstimateItemCount(key);
  }

  /**
   * @return
   */
  int GetK() { return tkq.k; }

  /**
   * @return
   */
  int GetSize() { return tkq.size; }

  /**
   * Retrieve all the items in the queue, unordered
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveAll() { return tkq.RetrieveAll(); }

  /**
   * Retrieve all the items in the queue, ordered, queue is maintained
   * small count to large count (min first)
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveAllOrdered() {
    return tkq.RetrieveAllOrdered();
  }

  /**
   * Retrieve all the items in the queue, ordered, queue is maintained
   * large count to small count (max first)
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() {
    return tkq.RetrieveAllOrderedMaxFirst();
  }

  /**
   * Retrieve given number of elements, ordered
   * Max first
   * @param num
   * @return
   */
  std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) {
    if (num >= tkq.GetK()) return tkq.RetrieveAllOrderedMaxFirst();
    return tkq.RetrieveOrderedMaxFirst(num);
  }

  /**
   * Retrieve all the items in the queue, ordered, queue is maintained
   * large count to small count (max first)
   * @return
   */
  std::vector<ValueFrequencyPair> GetAllOrderedMaxFirst() {
    return tkq.GetAllOrderedMaxFirst();
  }

  /**
   * Retrieve given number of elements, ordered
   * Max first
   * @param num
   * @return
   */
  std::vector<ValueFrequencyPair> GetOrderedMaxFirst(int num) {
    if (num >= tkq.GetK()) return tkq.GetAllOrderedMaxFirst();
    return tkq.GetOrderedMaxFirst(num);
  }

  /**
   *
   * @param os
   * @param topk
   * @return
   */
  friend std::ostream &operator<<(std::ostream &os, const TopKElements<KeyType> &topk) {

  }

  /*
   * Print the entire queue, unordered
   */
  void PrintTopKQueue() const {
    std::vector<ApproxTopEntry> vec = tkq.RetrieveAll();
    for (auto const& e : vec) {
      OPTIMIZER_LOG_INFO("\n [PrintTopKQueue Entries] %s", e.print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Min count first
   * Queue is empty afterwards
   */
  void PrintTopKQueuePops() {
    while (tkq.GetSize() != 0) {
      OPTIMIZER_LOG_INFO("\n [PrintTopKQueuePops Entries] %s", tkq.Pop().print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Min count first
   * Queue is intact
   */
  void PrintTopKQueueOrdered() {
    std::vector<ApproxTopEntry> vec = tkq.RetrieveAllOrdered();
    for (auto const& e : vec) {
      OPTIMIZER_LOG_INFO("\n [Print k Entries] %s", e.print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Max count first
   * Queue is intact
   */
  void PrintTopKQueueOrderedMaxFirst() {
    std::vector<ApproxTopEntry> vec = tkq.RetrieveAllOrderedMaxFirst();
    for (auto const& e : vec) {
      OPTIMIZER_LOG_INFO("\n [Print k Entries MaxFirst] %s", e.print().c_str());
    }
  }

  /*
   * Print a given number of elements in the top k queue, ordered
   * Max count first
   * Queue is intact
   * Retriving a vector of ValueFrequencyPair
   */
  void PrintTopKQueueOrderedMaxFirst(int num) {
    std::vector<ApproxTopEntry> vec = tkq.RetrieveOrderedMaxFirst(num);
    for (auto const& e : vec) {
      OPTIMIZER_LOG_INFO("\n [Print top %d Entries MaxFirst] %s", num, e.print().c_str());
    }
  }

  /*
   * Print the entire queue, ordered
   * Max count first
   * Queue is intact
   */
  void PrintAllOrderedMaxFirst() {
    std::vector<ValueFrequencyPair> vec = GetAllOrderedMaxFirst();
    for (auto const& p : vec) {
      OPTIMIZER_LOG_INFO("\n [Print k Values MaxFirst] %s, %f", p.first.GetInfo().c_str(),
               p.second);
    }
  }

  /*
   * Print a given number of elements in the top k queue, ordered
   * Max count first
   * Queue is intact
   * Retriving a vector of ValueFrequencyPair
   */
  void PrintOrderedMaxFirst(int num) {
    std::vector<ValueFrequencyPair> vec = GetOrderedMaxFirst(num);
    for (auto const& p : vec) {
      OPTIMIZER_LOG_INFO("\n [Print %d Values MaxFirst] %s, %f", num,
               p.first.GetInfo().c_str(), p.second);
    }
  }

 private:

  /**
   *
   */
  TopKQueue tkq;

  /**
   *
   */
  CountMinSketch<KeyType> *cmsketch;


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

    if (!tkq.Exists(entry)) {
      // not in the structure
      // insert it
      tkq.Push(entry);
    } else {
      // if in the structure
      // update
      tkq.Update(entry);
    }
  }

  /**
   * Decrease / Remove
   * @param entry
   */
  void DecrFreqItem(ApproxTopEntry& entry) {
    if (!tkq.Exists(entry)) {
      // not in the structure
      // do nothing
    } else {
      // if in the structure
      // update
      if (entry.count_ == 0) {
        tkq.Remove(entry);
      } else {
        tkq.Update(entry);
      }
    }
  }

};

}  // namespace terrier::optimizer
