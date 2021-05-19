#pragma once

#include <set>
#include <unordered_map>

#include "common/hash_util.h"
#include "self_driving/planning/action/action_defs.h"

namespace noisepage::selfdriving::pilot {

/**
 * Represent a unique action state during the search (used for caching the action rollout cost)
 */
class ActionState {
 public:
  void SetIntervals(uint64_t start_interval, uint64_t end_interval) {
    start_interval_ = start_interval;
    end_interval_ = end_interval;
  }
  void UpdateKnobValue(const std::string &knob, int64_t value) { knob_values_[knob] = value; }

  void AddIndex(const std::string &name, action_id_t id) {
    if (dropped_indexes_.find(name) == dropped_indexes_.end()) {
      created_indexes_.insert(name);
      index_action_map_[name] = id;
    } else {
      dropped_indexes_.erase(name);
      index_action_map_.erase(name);
    }
  }

  void RemoveIndex(const std::string &name, action_id_t id) {
    if (created_indexes_.find(name) == created_indexes_.end()) {
      dropped_indexes_.insert(name);
      index_action_map_[name] = id;
    } else {
      created_indexes_.erase(name);
      index_action_map_.erase(name);
    }
  }

  bool operator==(const ActionState &a) const {
    // We don't need to consider index_action_map_ to evaluate equivalence
    if (start_interval_ != a.start_interval_) return false;
    if (end_interval_ != a.end_interval_) return false;
    if (knob_values_ != a.knob_values_) return false;
    if (created_indexes_ != a.created_indexes_) return false;
    if (dropped_indexes_ != a.dropped_indexes_) return false;
    return true;
  }

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

class ActionStateHasher {
 public:
  size_t operator()(const std::unique_ptr<ActionState> &a) const { return a->Hash(); }
};

}  // namespace noisepage::selfdriving::pilot
