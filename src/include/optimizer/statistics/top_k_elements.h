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
#include <set>
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
   * TopKElements Constructor
   * @param k
   * @param width the size of the underlying sketch
   */
  explicit TopKElements(size_t k, uint64_t width) : numk_{k} {
    entries_.reserve(numk_);
    sketch_ = new CountMinSketch<KeyType>(width);
  }

  ~TopKElements() { delete sketch_; }

  /**
   *
   * @param key
   * @param count
   */
  void Add(KeyType key, uint32_t count) {
    TERRIER_ASSERT(count >= 0, "Invalid count");

    // Increment the count for this item in the sketch
    // and then get the new total count
    sketch_->Add(key, count);
    auto total_cnt = sketch_->EstimateItemCount(key);

    // If this key already exists in our top-k list, then
    // we need to update its entry
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      OPTIMIZER_LOG_TRACE("Update Key[{0}] => {1} // [size={2}]", key, total_cnt, GetSize());
      entries_[key] += count;

      // If this key is the current min key, then we need
      // to go through to see whether after this update it
      // is still the min key. This is crappy, but if the
      // # of elements that we need to keep track of is low,
      // this shouldn;t be too bad.
      if (key == min_key_) {
        ComputeNewMinKey();
      }
    }
    // If we have no elements, then throw the key in
    else if (entries_.empty()) {
      entries_[key] = count;
      OPTIMIZER_LOG_TRACE("Insert Key[{0}] => {1} // [size={2}]", key, total_cnt, GetSize());

      // And then it becomes our new min key
      min_key_ = key;
      min_count_ = total_cnt;
    }
    // This key does not exist yet in our top-k list, so we need
    // to figure out whether to promote it as a new entry
    // If the total estimated count for this key is
    // greater than the current min or internal vector size is less than
    // our 'k' limit, then add it to our vector.
    else if (entries_.size() < numk_ || total_cnt > min_count_) {
      if (entries_.size() == numk_) {
        // Remove the current min key
        entries_.erase(min_key_);
        OPTIMIZER_LOG_TRACE("Remove Key[{0}] => {1} // [size={2}]", min_key_, min_count_, GetSize());
      }

      // Then add our new key
      entries_[key] = total_cnt;
      OPTIMIZER_LOG_TRACE("Insert Key[{0}] => {1} // [size={2}]", key, total_cnt, GetSize());

      // But then we need to figure out what the new
      // min key is in our current top-k list
      // TODO: We don't always have to fire this off!
      ComputeNewMinKey();
    }
  }

  /**
   * Remove an item from the TopKQueue (as well as the sketch)
   * @param item
   * @param count
   */
  void Remove(KeyType key, uint32_t count) {
    TERRIER_ASSERT(count >= 0, "Invalid count");

    // Decrement the count for this item in the sketch
    // and then get the new total count
    sketch_->Remove(key, count);
    auto total_cnt = sketch_->EstimateItemCount(key);

    // This is where things get dicey on us.
    // So if this mofo key is in our top-k vector and its count is
    // now less than the current min entry, then we need to make it
    // the new min entry. But there may be another key that is
    // actually greater than this entry, but we don't know what it
    // is because we can't get that information from the sketch.
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      total_cnt = entries_[key] -= count;
      if (total_cnt < min_count_) {
        min_key_ = key;
        min_count_ = total_cnt;
      }
    }
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
  size_t GetK() const { return numk_; }

  /**
   * @return
   */
  size_t GetSize() const { return entries_.size(); }

//  /**
//   * Retrieve all the items in the queue, unordered
//   * @return
//   */
//  std::vector<ApproxTopEntry> RetrieveAll() const { return queue_.RetrieveAll(); }
//
//  /**
//   * Retrieve all the items in the queue, ordered, queue is maintained
//   * small count to large count (min first)
//   * @return
//   */
//  std::vector<ApproxTopEntry> RetrieveAllOrdered() const {
//    return queue_.RetrieveAllOrdered();
//  }
//
//  /**
//   * Retrieve all the items in the queue, ordered, queue is maintained
//   * large count to small count (max first)
//   * @return
//   */
//  std::vector<ApproxTopEntry> RetrieveAllOrderedMaxFirst() const {
//    return queue_.RetrieveAllOrderedMaxFirst();
//  }
//
//  /**
//   * Retrieve given number of elements, ordered
//   * Max first
//   * @param num
//   * @return
//   */
//  std::vector<ApproxTopEntry> RetrieveOrderedMaxFirst(int num) const {
//    if (num >= numk_) return queue_.RetrieveAllOrderedMaxFirst();
//    return queue_.RetrieveOrderedMaxFirst(num);
//  }
//
//  /**
//   * Retrieve all the items in the queue, ordered, queue is maintained
//   * large count to small count (max first)
//   * @return
//   */
//  std::vector<ValueFrequencyPair> GetAllOrderedMaxFirst() {
//    return queue_.GetAllOrderedMaxFirst();
//  }
//
//  /**
//   * Retrieve given number of elements, ordered
//   * Max first
//   * @param num
//   * @return
//   */
//  std::vector<ValueFrequencyPair> GetOrderedMaxFirst(int num) {
//    if (num >= numk_) return queue_.GetAllOrderedMaxFirst();
//    return queue_.GetOrderedMaxFirst(num);
//  }

  /**
   *
   * @param os
   * @param topk
   * @return
   */
  friend std::ostream &operator<<(std::ostream &os, const TopKElements<KeyType> &topK) {
    typedef std::function<bool(std::pair<KeyType, uint64_t>, std::pair<KeyType, uint64_t>)> Comparator;

    // Defining a lambda function to compare two pairs.
    // It will compare two pairs using second field
    Comparator compFunctor = [](std::pair<KeyType, uint64_t> elem1 ,std::pair<KeyType, uint64_t> elem2) {
      return elem1.second > elem2.second;
    };

    // Declaring a set that will store the pairs using above comparision logic
    std::set<std::pair<KeyType, uint64_t>, Comparator> sorted_keys(
        topK.entries_.begin(), topK.entries_.end(), compFunctor);

    os << "Top-" << topK.GetK() << " [size=" << topK.GetSize() << "]";
    int i = 0;
    for (const std::pair<KeyType, uint64_t> &element : sorted_keys) {
      os << std::endl << "  (" << i++ << ") Key[" << element.first << "] => " << element.second;
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
   *
   */
  std::unordered_map<KeyType, int64_t> entries_;

  /**
   *
   */
  KeyType min_key_;

  /**
   *
   */
  uint64_t min_count_;

  /**
   *
   */
  CountMinSketch<KeyType> *sketch_;

  /**
   * the K as in "top K"
   */
  size_t numk_;

  /**
   *
   */
  void ComputeNewMinKey() {
    KeyType new_min_key;
    auto new_min_count = INT64_MAX;
    for (auto other : entries_) {
      if (other.second < new_min_count) {
        new_min_key = other.first;
        new_min_count = other.second;
      }
    }
    min_key_ = new_min_key;
    min_count_ = new_min_count;
    OPTIMIZER_LOG_TRACE("MinKey[{0}] => {1}", min_key_, min_count_);
  }


};

}  // namespace terrier::optimizer
