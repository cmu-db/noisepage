#pragma once

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
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

 public:
  /**
   * TopKElements Constructor
   * @param k the number of keys to keep track of in the top-k list
   * @param width the size of the underlying sketch
   */
  explicit TopKElements(size_t k, uint64_t width) : numk_{k} {
    entries_.reserve(numk_);
    sketch_ = new CountMinSketch<KeyType>(width);
  }

  /**
   * Deconstructor
   */
  ~TopKElements() { delete sketch_; }

  /**
   * Increase the count for the given key by the specified delta.
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method
   * @param key the key to target
   * @param delta the amount to increase the key's count
   */
  void Increment(const KeyType &key, const uint32_t delta) { Increment(key, sizeof(key), delta); }

  /**
   * Increase the count for the given key by the specified delta.
   * @param key the key to target
   * @param key_size the length of the key's data
   * @param delta the amount to increase the key's count
   */
  void Increment(const KeyType &key, const size_t key_size, const uint32_t delta) {
    TERRIER_ASSERT(delta >= 0, "Invalid delta");

    // Increment the count for this item in the sketch
    sketch_->Increment(key, key_size, delta);

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
    auto total_cnt = sketch_->EstimateItemCount(key, key_size);

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
  }

  /**
   * Decrease the count for the given key by the specified delta.
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method.
   * @param key the key to target
   * @param delta the amount to increase the key's count
   */
  void Decrement(const KeyType &key, const uint32_t delta) { Decrement(key, sizeof(key), delta); }

  /**
   * Decrease the count for the given key by the specified delta.
   * @param key the key to target
   * @param key_size the length of the key's data
   * @param delta the amount to increase the key's count
   */
  void Decrement(const KeyType &key, const size_t key_size, uint32_t delta) {
    TERRIER_ASSERT(delta >= 0, "Invalid delta");
    OPTIMIZER_LOG_TRACE("Decrement(key={0}, delta={1}) // [size={2}]", key, delta, GetSize());

    // Decrement the count for this item in the sketch
    sketch_->Decrement(key, key_size, delta);

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
        OPTIMIZER_LOG_TRACE("MinKey[{0}] => {1}", min_key_, min_count_);
      }
    }
  }

  /**
   * Remove a key from the top-k tracker as well as the sketch.
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method
   * @param key the key to target
   */
  void Remove(const KeyType &key) { Remove(key, sizeof(key)); }

  /**
   * Remove a key from the top-k tracker as well as the sketch.
   * @param key
   * @param key_size
   */
  void Remove(const KeyType &key, const size_t key_size) {
    OPTIMIZER_LOG_TRACE("Remove(key={0}) // [size={2}]", key, GetSize());

    // Always remove the key from the sketch
    sketch_->Remove(key, key_size);

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
    return sketch_->EstimateItemCount(key);
  }

  /**
   * @return the sketch object of the top-K list
   */
  const CountMinSketch<KeyType> &GetSketch() const { return (*sketch_); }

  /**
   * @return the Entries-map object of the top-K list
   */
  const std::unordered_map<KeyType, int64_t> &GetEntries() const { return entries_; }

  /*
    Merge two TopK Elments objects
  */
  void Merge(const terrier::optimizer::TopKElements<KeyType> &hist) {
    // merge the sketches
    sketch_->Merge((hist.GetSketch()));

    std::unordered_map<KeyType, int64_t> temp_entries;
    for (auto entry : entries_) {
      temp_entries[entry.first] = entry.second;
    }
    for (auto entry : hist.GetEntries()) {
      if (temp_entries.find(entry.first) != temp_entries.end()) {
        temp_entries[entry.first] += entry.second;
      } else {
        temp_entries[entry.first] = entry.second;
      }
    }

    std::vector<std::pair<int64_t, KeyType>> temp_entries_vector;
    temp_entries_vector.reserve(temp_entries.size());
    for (auto entry : temp_entries) {
      temp_entries_vector.push_back(std::make_pair(entry.second, entry.first));
    }
    sort(temp_entries_vector.begin(), temp_entries_vector.end());

    entries_.clear();
    auto vector_reverse_iterator = temp_entries_vector.rbegin();
    for (unsigned i = 0; i < std::min(temp_entries_vector.size(), numk_); i++) {
      entries_[vector_reverse_iterator->second] = vector_reverse_iterator->first;
      ++vector_reverse_iterator;
    }

    --vector_reverse_iterator;
    min_key_ = vector_reverse_iterator->second;
    min_count_ = vector_reverse_iterator->first;
  }

  // Clear the topK elements object

  void Clear() {
    sketch_->Clear();
    entries_.clear();
    min_count_ = INT64_MAX;
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
   * Serializes the TopKElements object into a char array.
   *
   * The format contains a header of 32-bit words, the top K elements data, and then the sketch data.
   * We place the top K elements data first since it may have alignment requirements (whereas the
   * sketch data does not) and we don't want to think about alignment.
   *
   * Header contents:
   *   Bytes 0-3: entries_size (in bytes, not elements)
   *   Bytes 4-7: sketch_size (in bytes)
   *   Bytes 8-11: numk_
   *   Bytes 12-15: total_count_ (from optimizer::CountMinSketch)
   *
   * @return A serialized representation of this TopKElements object.
   */
  std::unique_ptr<byte[]> GetSerializedData(size_t *size) {
    const auto &sketch = sketch_->GetSketch();

    // Compute size of each portion
    constexpr size_t header_size = 4 * sizeof(uint32_t);
    size_t entries_size = entries_.size() * sizeof(KeyCountPair);
    size_t sketch_size = sketch.file_size();

    static_assert(header_size % alignof(KeyCountPair) == 0, "Alignment of KeyCountPair is too large");
    TERRIER_ASSERT(entries_.size() <= std::numeric_limits<uint32_t>::max() &&
                       sketch_size <= std::numeric_limits<uint32_t>::max() &&
                       numk_ <= std::numeric_limits<uint32_t>::max() &&
                       sketch_->GetTotalCount() <= std::numeric_limits<uint32_t>::max(),
                   "Sizes are too large to fit in 32 bits");

    // Resize backing vector
    size_t overall_size = header_size + entries_size + sketch_size;
    auto buffer = std::unique_ptr<byte[]>(common::AllocationUtil::AllocateAligned(overall_size));
    byte *data = buffer.get();

    // Write serialized header
    *reinterpret_cast<uint32_t *>(data) = uint32_t(entries_size);
    *reinterpret_cast<uint32_t *>(data + sizeof(uint32_t)) = uint32_t(sketch_size);
    *reinterpret_cast<uint32_t *>(data + 2 * sizeof(uint32_t)) = uint32_t(numk_);
    *reinterpret_cast<uint32_t *>(data + 3 * sizeof(uint32_t)) = uint32_t(sketch_->GetTotalCount());

    // Serialize top K elements, stored in entries_
    // Note that we don't care about the ordering of the top K elements. The TopKElements object,
    // when deserialized, will take care of that.
    // TODO(khg): KeyCountPair does not match the type of entries_, ???
    size_t index = 0;
    for (const KeyCountPair pair : entries_) {
      byte *offset = data + header_size + index * sizeof(KeyCountPair);
      *reinterpret_cast<KeyCountPair *>(offset) = pair;
      index++;
    }

    // Serialize CountMinSketch
    sketch.serialize(data + header_size + entries_size, sketch_size);

    // Return buffer by value
    *size = overall_size;
    return buffer;
  }

  static TopKElements<KeyType> Deserialize(const byte *buffer, size_t size) {
    // Compute header size
    constexpr size_t header_size = 4 * sizeof(uint32_t);
    TERRIER_ASSERT(size >= header_size, "Deserialization failed: size is not large enough to hold header");

    // Read header data
    const auto entries_size = *reinterpret_cast<const uint32_t *>(buffer);
    const auto sketch_size = *reinterpret_cast<const uint32_t *>(buffer + sizeof(uint32_t));
    const auto numk = *reinterpret_cast<const uint32_t *>(buffer + 2 * sizeof(uint32_t));
    const auto total_count = *reinterpret_cast<const uint32_t *>(buffer + 3 * sizeof(uint32_t));
    TERRIER_ASSERT(size == header_size + entries_size + sketch_size,
                   "Deserialization failed: size does not match input data");
    TERRIER_ASSERT(entries_size % sizeof(KeyCountPair) == 0,
                   "entries_size is invalid: must be a multiple of sizeof(KeyCountPair)");

    // Create new object
    TopKElements<KeyType> topk{numk};
    topk.sketch_ = new CountMinSketch<KeyType>(total_count, buffer + header_size + entries_size, sketch_size);

    // Read entries into object
    size_t num_entries = entries_size / sizeof(KeyCountPair);
    for (size_t i = 0; i < num_entries; i++) {
      KeyCountPair pair = *reinterpret_cast<const KeyCountPair *>(buffer + header_size + i * sizeof(KeyCountPair));
      topk.entries_[pair.first] = pair.second;
    }

    // Update min key from entries
    topk.ComputeNewMinKey();

    return topk;
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
   * Private constructor for deserialization only
   * @param k the number of keys to keep track of in the top-k list
   */
  explicit TopKElements(size_t k) : numk_{k}, sketch_{nullptr} { entries_.reserve(numk_); }

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
  CountMinSketch<KeyType> *sketch_;

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
    OPTIMIZER_LOG_TRACE("Compute MinKey[{0}] => {1}", min_key_, min_count_);
  }
};

}  // namespace terrier::optimizer
