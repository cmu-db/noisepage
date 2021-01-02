#pragma once

#include "settings/settings_param.h"

namespace noisepage::selfdriving::pilot {

/**
 * Config file of the change values of each self-driving change knob action
 * The values are stored in maps based on the value type (e.g., bool, int64), which map from the knob param type to the
 * vector of change values. Each element in the vector is a pair of reverse values to change for that knob (e.g.,
 * increment and decrement a knob value by 10).
 */
class ChangeKnobValueConfig {
  /** @return Map from a bool knob param to the vector of its candidate change values */
  static const map<settings::Param, std::vector<std::pair<bool, bool>>> &GetBoolChangeValueMap() const {
    return bool_change_value_map;
  }

  /** @return Map from a int64 knob param to the vector of its candidate change values */
  static const map<settings::Param, std::vector<std::pair<int64_t, int64_t>>> &GetInt64ChangeValueMap() const {
    return int64_change_value_map;
  }

 private:
  static map<settings::Param, std::vector<std::pair<bool, bool>>> bool_change_value_map;
  static map<settings::Param, std::vector<std::pair<int64_t, int64_t>>> int64_change_value_map;
};

map<settings::Param, std::vector<std::pair<bool, bool>>> ChangeKnobValueConfig::bool_change_value_map = {
    {settings::Param::compiled_query_execution, {{true, false}}},
};

map<settings::Param, std::vector<std::pair<int64_t, int64_t>>> ChangeKnobValueConfig::int64_change_value_map = {{
    settings::Param::wal_persist_threshold,
    {{1 << 10, -(1 << 10)}, {1 << 12, -(1 << 12)}},
};

}  // namespace noisepage::selfdriving::pilot
