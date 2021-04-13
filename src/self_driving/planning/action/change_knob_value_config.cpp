#include "self_driving/planning/action/change_knob_value_config.h"

namespace noisepage::selfdriving::pilot {

// For bool knobs, the reverse actions are the same (^ true)
std::map<settings::Param, std::vector<std::pair<bool, bool>>> ChangeKnobValueConfig::bool_change_value_map = {
    {settings::Param::compiled_query_execution, {{true, true}}},
};

std::map<settings::Param, std::vector<std::pair<int32_t, int32_t>>> ChangeKnobValueConfig::int_change_value_map = {
    {settings::Param::wal_serialization_interval, {{10, -10}, {100, -100}}},
};

std::map<settings::Param, std::vector<std::pair<int64_t, int64_t>>> ChangeKnobValueConfig::int64_change_value_map = {};

}  // namespace noisepage::selfdriving::pilot
