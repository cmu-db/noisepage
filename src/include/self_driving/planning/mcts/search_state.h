#pragma once

#include <unordered_map>

#include "self_driving/planning/action/action_defs.h"

namespace noisepage::selfdriving::pilot {

/**
 * Represent a unique action state during the search (used for caching the action rollout cost)
 */
class SearchState {
  void UpdateKnobValue(const std::string &knob, int64_t value) { knob_values_[knob] = value; }

  void AddIndex(const std::string &name, actino_id id) {
    if (dropped_indexes_.find(name) == dropped_indexes_.end())
      created_indexes_[name] = id;
    else
      dropped_indexes_.erase(name);
  }

  void RemoveIndex(const std::string &name) {
    if (created_indexes_.find(name) == created_indexes_.end())
      dropped_indexes_[name] = id;
    else
      created_indexes_.erase(name);
  }

  bool operator==(const SearchState &p) const {}

 private:
  uint64_t invernal_{0};
  std::unordered_map<std::string, int64_t> knob_values_;
  std::unordered_map<std::string, action_id> created_indexes_;
  std::unordered_map<std::string, action_id> dropped_indexes_;
};

}  // namespace noisepage::selfdriving::pilot
