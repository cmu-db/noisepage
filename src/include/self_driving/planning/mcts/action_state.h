#pragma once

#include <set>
#include <string>
#include <unordered_map>

#include "common/hash_util.h"
#include "self_driving/planning/action/action_defs.h"

namespace noisepage::selfdriving::pilot {

/**
 * Represent a unique action state during the search (used for caching the action rollout cost)
 */
class ActionState {
 public:
  /**
   * Set the start and end intervals for the action state
   * @param start_interval The start interval
   * @param end_interval The end interval
   */
  void SetIntervals(uint64_t start_interval, uint64_t end_interval) {
    start_interval_ = start_interval;
    end_interval_ = end_interval;
  }

  /**
   * Update a knob value in the action state
   * @param knob The name of the knob
   * @param value The value to update (cast to int64_t)
   */
  void UpdateKnobValue(const std::string &knob, int64_t value) { knob_values_[knob] = value; }

  /**
   * Add an index in the action state
   * @param name The name of the index
   * @param id The action id that created the index
   */
  void AddIndex(const std::string &name, action_id_t id) {
    if (dropped_indexes_.find(name) == dropped_indexes_.end()) {
      created_indexes_.insert(name);
      index_action_map_[name] = id;
    } else {
      dropped_indexes_.erase(name);
      index_action_map_.erase(name);
    }
  }

  /**
   * Remove an index from the action state
   * @param name The name of the index
   * @param id The action id that dropped the index
   */
  void RemoveIndex(const std::string &name, action_id_t id) {
    if (created_indexes_.find(name) == created_indexes_.end()) {
      dropped_indexes_.insert(name);
      index_action_map_[name] = id;
    } else {
      created_indexes_.erase(name);
      index_action_map_.erase(name);
    }
  }

  /** @brief Define the equality operator */
  bool operator==(const ActionState &a) const {
    // We don't need to consider index_action_map_ to evaluate equivalence
    if (start_interval_ != a.start_interval_) return false;
    if (end_interval_ != a.end_interval_) return false;
    if (knob_values_ != a.knob_values_) return false;
    if (created_indexes_ != a.created_indexes_) return false;
    if (dropped_indexes_ != a.dropped_indexes_) return false;
    return true;
  }

  /** @return The hash value of this action state */
  hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(start_interval_);
    hash = common::HashUtil::CombineHashes(hash, end_interval_);

    for (const auto &[knob, value] : knob_values_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(knob));
      hash = common::HashUtil::CombineHashes(hash, value);
    }

    for (const auto &name : created_indexes_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(name));
    }

    for (const auto &name : dropped_indexes_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(name));
    }
    return hash;
  }

  /** @return The set of created indexes */
  const std::set<std::string> &GetCreatedIndexes() const { return created_indexes_; }
  /** @return The set of dropped indexes */
  const std::set<std::string> &GetDroppedIndexes() const { return dropped_indexes_; }
  /** @return Map from an index name to the action id that created/dropped that index */
  const std::unordered_map<std::string, action_id_t> &GetIndexActionMap() const { return index_action_map_; }

 private:
  uint64_t start_interval_;
  uint64_t end_interval_;
  std::unordered_map<std::string, int64_t> knob_values_;
  // Because the same index may be created/dropped with different oids during the search, we use the index name as an
  // identifier
  std::set<std::string> created_indexes_;
  std::set<std::string> dropped_indexes_;
  std::unordered_map<std::string, action_id_t> index_action_map_;
};

/** Hasher for ActionState used in STL containers */
class ActionStateHasher {
 public:
  /** @brief Hash operator */
  size_t operator()(const ActionState &a) const { return a.Hash(); }
};

}  // namespace noisepage::selfdriving::pilot
