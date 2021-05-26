#pragma once

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <queue>
#include <set>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/json.h"
#include "common/macros.h"
#include "optimizer/statistics/count_min_sketch.h"

namespace noisepage::optimizer {

/**
 * This class keeps track of the top-k elements for a given value set.
 * You have to tell the class how many elements (k) to keep track of
 * as the heavyhitters. There is an underlying CountMinSketch that
 * keeps track of the counts for all keys.
 */
template <typename KeyType>
class TopKElements {
  /**
   * The internal type that we use to keep track of the counts for keys.
   */
  using KeyCountPair = std::pair<KeyType, uint32_t>;
  static constexpr size_t DEFAULT_K = 16;
  static constexpr uint64_t DEFAULT_WIDTH = 64;

 public:
  /**
   * TopKElements Constructor using DEFAULT_K for number of keys and DEFAULT_WIDTH for the sketch width
   */
  TopKElements() : numk_{DEFAULT_K}, sketch_(DEFAULT_WIDTH) { entries_.reserve(numk_); }

  /**
   * TopKElements Constructor
   * @param k the number of keys to keep track of in the top-k list
   * @param width the size of the underlying sketch
   */
  explicit TopKElements(size_t k, uint64_t width) : numk_{k}, sketch_(width) { entries_.reserve(numk_); }

  /**
   * Copy constructor
   * @param other TopKElements to copy
   */
  TopKElements(const TopKElements &other) = default;

  /**
   * Move constructor
   * @param other TopKElements to move
   */
  TopKElements(TopKElements &&other) noexcept = default;

  /**
   * Copy assignment operator
   * @param other TopKElements to copy
   * @return
   */
  TopKElements &operator=(const TopKElements &other) = default;

  /**
   * Move assignment operator
   * @param other TopKElements to move
   * @return
   */
  TopKElements &operator=(TopKElements &&other) noexcept = default;

  /**
   * Increase the count for the given key by the specified delta.
   * @param key the key to target
   * @param delta the amount to increase the key's count
   */
  void Increment(const KeyType &key, const uint32_t delta) {
    NOISEPAGE_ASSERT(delta >= 0, "Invalid delta");

    // Increment the count for this item in the sketch
    sketch_.Increment(key, delta);

    // If this key already exists in our top-k list, then
    // we need to update its entry
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      entries_[key] += delta;

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
    auto total_cnt = sketch_.EstimateItemCount(key);

    // If the total estimated count for this key is greater than the
    // current min and our top-k is at its max capacity, then we know
    // that we need to take out the current min key and put in this key.
    auto size = entries_.size();
    if (size == numk_ && total_cnt > min_count_) {
      // Remove the current min key
      entries_.erase(min_key_);

      // Then add our new key
      entries_[key] = total_cnt;

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

      // We only need to do a direct comparison here to see
      // whether our key is the new min key.
      if (delta < min_count_) {
        min_key_ = key;
        min_count_ = delta;
      }
    }
  }

  /**
   * Decrease the count for the given key by the specified delta.
   * @param key the key to target
   * @param delta the amount to increase the key's count
   */
  void Decrement(const KeyType &key, uint32_t delta) {
    NOISEPAGE_ASSERT(delta >= 0, "Invalid delta");

    // Decrement the count for this item in the sketch
    sketch_.Decrement(key, delta);

    // This is where things get dicey on us.
    // So if this mofo key is in our top-k vector and its count is
    // now less than the current min entry, then we need to make it
    // the new min entry. But there may be another key that is
    // actually greater than this entry, but we don't know what it
    // is because we can't get that information from the sketch.
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      uint64_t total_cnt = entries_[key] -= delta;

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
      }
    }
  }

  /**
   * Remove a key from the top-k tracker as well as the sketch.
   * @param key
   */
  void Remove(const KeyType &key) {
    // Always remove the key from the sketch
    sketch_.Remove(key);

    // Then check to see whether it exists in our top-k list
    auto entry = entries_.find(key);
    if (entry != entries_.end()) {
      // Remove this key
      entries_.erase(key);

      // If this key was the min key, then we need to recompute
      if (key == min_key_) ComputeNewMinKey();
    }
  }

  /**
   * Compute the approximate count for the given key.
   * If the key is in the top-k list, then you will get the amount
   * from that list. Otherwise it will come from the underlying CountMinSketch.
   * Note that just because the key is in the top-k list does <b>not</b>
   * mean the count returned by this method will be 100% accurate.
   * @param key key the key to get the count for.
   * @return the approximate count number for the key.
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
    return sketch_.EstimateItemCount(key);
  }

  /**
   * @return the number of keys to keep track of in the top-k list
   */
  size_t GetK() const { return numk_; }

  /**
   * @return the current size of the top-k list. This can be less than k.
   */
  size_t GetSize() const { return entries_.size(); }

  /**
   * Generate a vector of the top-k keys sorted by their current counts
   * @return the vector of the top-k keys
   */
  std::vector<KeyType> GetSortedTopKeys() const {
    using Comparator = std::function<bool(KeyCountPair, KeyCountPair)>;

    // Defining a lambda function to compare two pairs.
    // It will compare two pairs using second field
    Comparator comp_functor = [](KeyCountPair elem1, KeyCountPair elem2) { return elem1.second < elem2.second; };

    // Copy the pairs into a vector sorted on their value
    std::vector<KeyCountPair> sorted_vec;
    sorted_vec.reserve(entries_.size());
    std::copy(entries_.begin(), entries_.end(), std::back_inserter<std::vector<KeyCountPair>>(sorted_vec));
    std::sort(sorted_vec.begin(), sorted_vec.end(), comp_functor);

    // FIXME: Is there a way that we can just use the sorted_keys directly
    // without having to copy it into a vector first.
    std::vector<KeyType> keys;
    keys.reserve(sorted_vec.size());
    for (const KeyCountPair &element : sorted_vec) {
      keys.push_back(element.first);
    }

    return keys;
  }

  /**
   * Merge Top K Elements with another Top K Elements
   * @param top_k_elements Top K Elements to merge with this
   */
  void Merge(const TopKElements<KeyType> &top_k_elements) {
    sketch_.Merge(top_k_elements.sketch_);

    for (auto &[key, count] : top_k_elements.entries_) {
      Increment(key, count);
    }
  }

  /**
   * Clear the Top K object
   */
  void Clear() {
    entries_.clear();
    sketch_.Clear();
    ComputeNewMinKey();
  }

  /**
   * Convert TopKElements to json
   * @return json representation of a TopKElements
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["numk"] = numk_;
    j["entries"] = nlohmann::json(entries_);
    j["min_key"] = min_key_;
    j["min_count"] = min_count_;
    j["sketch"] = sketch_.ToJson();
    return j;
  }

  /**
   * Convert json to TopKElements
   * @param j json representation of a TopKElements
   * @return TopKElements object parsed from json
   */
  static TopKElements FromJson(const nlohmann::json &j) {
    auto numk = j.at("numk").get<size_t>();
    // Width doesn't actually matter because we overwrite the underlying sketch
    TopKElements top_k_elements(numk, 1);
    top_k_elements.entries_ = j.at("entries").get<std::unordered_map<KeyType, int64_t>>();
    top_k_elements.min_key_ = j.at("min_key").get<KeyType>();
    top_k_elements.min_count_ = j.at("min_count").get<uint64_t>();
    top_k_elements.sketch_ = CountMinSketch<KeyType>::FromJson(j.at("sketch"));
    return top_k_elements;
  }

  /**
   * Serialize TopKElements object into byte array
   * @param[out] size length of byte array
   * @return byte array representation of TopKElements
   */
  std::unique_ptr<byte[]> Serialize(size_t *size) const {
    const std::string json_str = ToJson().dump();
    *size = json_str.size();
    auto buffer = std::make_unique<byte[]>(*size);
    std::memcpy(buffer.get(), json_str.c_str(), *size);
    return buffer;
  }

  /**
   * Deserialize TopKElements object from byte array
   * @param buffer byte array representation of TopKElements
   * @param size length of byte array
   * @return Deserialized TopKElements object
   */
  static TopKElements Deserialize(const byte *buffer, size_t size) {
    std::string json_str(reinterpret_cast<const char *>(buffer), size);
    auto json = nlohmann::json::parse(json_str);
    return TopKElements::FromJson(json);
  }

  /**
   * Pretty Print!
   * @param os the output target
   * @param top_k the top-k object to print
   * @return representation of sorted keys and their counts
   */
  friend std::ostream &operator<<(std::ostream &os, const TopKElements<KeyType> &top_k) {
    os << "Top-" << top_k.GetK() << " [size=" << top_k.GetSize() << "]";
    int i = 0;
    for (const KeyType &key : top_k.GetSortedTopKeys()) {
      auto count = top_k.EstimateItemCount(key);
      os << std::endl << "  (" << i++ << ") Key[" << key << "] => " << count;
    }
    return os;
  }

 private:
  /**
   * the K as in "top K"
   */
  size_t numk_;

  /**
   * The internal top-k key list. The size of this vector
   * will not exceed k.
   */
  std::unordered_map<KeyType, int64_t> entries_;

  /**
   * The key with the smallest count that we've seen so far.
   * This key must exist in the internal entries list.
   */
  KeyType min_key_;

  /**
   * The count of the smallest key that we've seen so far.
   */
  uint64_t min_count_ = INT64_MAX;

  /**
   * Internal sketch to keep track of all keys in the tracker.
   */
  CountMinSketch<KeyType> sketch_;

  /**
   * Internal helper method that figures out what the smallest
   * key that we currently have in our entries list. This method
   * is not efficient because it just does a brute force search.
   * But this is fine as long as 'k' us not large.
   */
  void ComputeNewMinKey() {
    KeyType new_min_key = min_key_;
    auto new_min_count = INT64_MAX;
    for (const auto &other : entries_) {
      if (other.second < new_min_count) {
        new_min_key = other.first;
        new_min_count = other.second;
      }
    }
    min_key_ = new_min_key;
    min_count_ = new_min_count;
  }
};

}  // namespace noisepage::optimizer
