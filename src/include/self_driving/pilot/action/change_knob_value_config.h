#pragma once

#include <map>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "settings/settings_param.h"

namespace noisepage::selfdriving::pilot {

/**
 * Config file of the change values of each self-driving change knob action
 * The values are stored in maps based on the value type (e.g., bool, int64), which map from the knob param type to the
 * vector of change values. Each element in the vector is a pair of reverse values to change for that knob (e.g.,
 * increment and decrement a knob value by 10).
 */
class ChangeKnobValueConfig {
 public:
  /** @return Map from a bool knob param to the vector of its candidate change values */
  static common::ManagedPointer<std::map<settings::Param, std::vector<std::pair<bool, bool>>>> GetBoolChangeValueMap() {
    return common::ManagedPointer(&bool_change_value_map);
  }

  /** @return Map from a int knob param to the vector of its candidate change values */
  static common::ManagedPointer<std::map<settings::Param, std::vector<std::pair<int32_t, int32_t>>>>
  GetIntChangeValueMap() {
    return common::ManagedPointer(&int_change_value_map);
  }

  /** @return Map from a int64 knob param to the vector of its candidate change values */
  static common::ManagedPointer<std::map<settings::Param, std::vector<std::pair<int64_t, int64_t>>>>
  GetInt64ChangeValueMap() {
    return common::ManagedPointer(&int64_change_value_map);
  }

 private:
  static std::map<settings::Param, std::vector<std::pair<bool, bool>>> bool_change_value_map;
  static std::map<settings::Param, std::vector<std::pair<int32_t, int32_t>>> int_change_value_map;
  static std::map<settings::Param, std::vector<std::pair<int64_t, int64_t>>> int64_change_value_map;
};

}  // namespace noisepage::selfdriving::pilot
