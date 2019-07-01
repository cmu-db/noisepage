#pragma once

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <queue>
#include <set>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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
  void Increment(const KeyType &key, const uint32_t count) { Increment(key, sizeof(key), count); }

  /**
   *
   * @param key
   * @param key_size
   * @param count
   */
  void Increment(const KeyType &key, const size_t key_size, const uint32_t delta) {
    TERRIER_ASSERT(delta >= 0, "Invalid delta");

    // Increment the count for this item in the sketch
    // FIXME: Change the sketch API to use the key_size.
    sketch_->Add(key, delta);

    // If this key already exists in our top-k list, then
    // we need to update its entry
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      entries_[key] += delta;
      OPTIMIZER_LOG_TRACE("Increment Key[{0}] => {1} // [size={2}]", key, entries_[key], GetSize());

      // If this key is the current min key, then we need
      // to go through to see whether after this update it
      // is still the min key. This is crappy, but if the
      // # of elements that we need to keep track of is low,
      // this shouldn't be too bad.
      if (key == min_key_) ComputeNewMinKey();

      // All done!
      return;
    }

    // The key is *not* in our top-k entries list.
    // This means that we have to ask the sketch the current count
    // for it to determine whether it should be promoted into our
    // top-k list.
    auto total_cnt = sketch_->EstimateItemCount(key);

    // If the total estimated count for this key is greater than the
    // current min and our top-k is at its max capacity, then we know
    // that we need to take out the current min key and put in this key.
    auto size = entries_.size();
    if (size == numk_ && total_cnt > min_count_) {
      // Remove the current min key
      entries_.erase(min_key_);
      OPTIMIZER_LOG_TRACE("Remove Key[{0}] => {1} // [size={2}]", min_key_, min_count_, GetSize());

      // Then add our new key
      entries_[key] = total_cnt;
      OPTIMIZER_LOG_TRACE("Insert Key[{0}] => {1} // [size={2}]", key, total_cnt, GetSize());

      // But then we need to figure out what the new
      // min key is in our current top-k list. We don't
      // know whether our key is the new min key or whether
      // it's another existing key in our top-k list
      ComputeNewMinKey();

      // All done!
      return;
    }

    // If we have fewer keys in our top-k list than we are
    // allowed to have, then we can always add this key.
    if (size < numk_) {
      // Important: Since the number keys right now is less than
      // the max amount and because this key did not already exist
      // in our top-k class (otherwise we would have saw it up above),
      // we know that we can the exact count here and not the
      // estimated count.
      entries_[key] = delta;
      OPTIMIZER_LOG_TRACE("Insert Key[{0}] => {1} // [size={2}]", key, delta, GetSize());

      // We only need to do a direct comparison here to see
      // whether our key is the new min key.
      if (delta < min_count_) {
        min_key_ = key;
        min_count_ = delta;
        OPTIMIZER_LOG_TRACE("Direct MinKey[{0}] => {1}", min_key_, min_count_);
      }
    }

    // All done!
    return;
  }

  /**
   * Remove an item from the TopKQueue (as well as the sketch)
   * @param item
   * @param delta
   */
  void Decrement(KeyType key, uint32_t delta) {
    TERRIER_ASSERT(delta >= 0, "Invalid delta");
    OPTIMIZER_LOG_TRACE("Decrement(key={0}, delta={1}) // [size={2}]", key, delta, GetSize());

    // Decrement the count for this item in the sketch
    // and then get the new total count
    sketch_->Remove(key, delta);
    auto total_cnt = sketch_->EstimateItemCount(key);

    // This is where things get dicey on us.
    // So if this mofo key is in our top-k vector and its count is
    // now less than the current min entry, then we need to make it
    // the new min entry. But there may be another key that is
    // actually greater than this entry, but we don't know what it
    // is because we can't get that information from the sketch.
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      total_cnt = entries_[key] -= delta;

      // If our count is now negative, then we need to remove
      // this key completely. Otherwise this will cause problems
      // with out min count stuff
      if (total_cnt <= 0) {
        entries_.erase(key);
        // If this key was the min key, then we need to recompute
        if (key == min_key_) ComputeNewMinKey();

        // If this key's count is less than the current min count,
        // then it becomes the new min key
      } else if (total_cnt < min_count_) {
        min_key_ = key;
        min_count_ = total_cnt;
        OPTIMIZER_LOG_TRACE("MinKey[{0}] => {1}", min_key_, min_count_);
      }
    }
  }

  /**
   *
   * @param key
   * @return
   */
  uint64_t EstimateItemCount(const KeyType &key) const {
    // If the key is in our top-k entries list, then
    // we'll give them that value.
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      return entry->second;
    }
    // Otherwise give them whatever the sketch thinks is the
    // the count.
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

  const std::vector<KeyType> GetSortedTopKeys() const {
    typedef std::function<bool(std::pair<KeyType, uint64_t>, std::pair<KeyType, uint64_t>)> Comparator;

    // Defining a lambda function to compare two pairs.
    // It will compare two pairs using second field
    Comparator compFunctor = [](std::pair<KeyType, uint64_t> elem1, std::pair<KeyType, uint64_t> elem2) {
      return elem1.second < elem2.second;
    };

    // Copy the pairs into a vector sorted on their value
    std::vector<std::pair<KeyType, uint64_t>> sorted_vec;
    std::copy(entries_.begin(), entries_.end(),
              std::back_inserter<std::vector<std::pair<KeyType, uint64_t>>>(sorted_vec));
    std::sort(sorted_vec.begin(), sorted_vec.end(), compFunctor);

    // FIXME: Is there a way that we can just use the sorted_keys directly
    // without having to copy it into a vector first.
    std::vector<KeyType> keys;
    for (const std::pair<KeyType, uint64_t> &element : sorted_vec) {
      keys.push_back(element.first);
    }

    return keys;
  }

  /**
   *
   * @param os
   * @param topk
   * @return
   */
  friend std::ostream &operator<<(std::ostream &os, const TopKElements<KeyType> &topK) {
    os << "Top-" << topK.GetK() << " [size=" << topK.GetSize() << "]";
    int i = 0;
    //    for (const std::pair<KeyType, uint64_t> &element : topK.GetSortedTopKeys()) {
    //      os << std::endl << "  (" << i++ << ") Key[" << element.first << "] => " << element.second;
    //    }
    for (KeyType key : topK.GetSortedTopKeys()) {
      auto count = topK.EstimateItemCount(key);
      os << std::endl << "  (" << i++ << ") Key[" << key << "] => " << count;
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
  uint64_t min_count_ = INT64_MAX;

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
    OPTIMIZER_LOG_TRACE("Compute MinKey[{0}] => {1}", min_key_, min_count_);
  }
};

}  // namespace terrier::optimizer
