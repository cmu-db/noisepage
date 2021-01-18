#include "self_driving/pilot/action/change_knob_value_config.h"

namespace noisepage::selfdriving::pilot {

// For bool knobs, the reverse actions are the same (^ true)
std::map<settings::Param, std::vector<std::pair<bool, bool>>> ChangeKnobValueConfig::bool_change_value_map = {
    {settings::Param::compiled_query_execution, {{true, true}}},
};

std::map<settings::Param, std::vector<std::pair<int64_t, int64_t>>> ChangeKnobValueConfig::int64_change_value_map = {
    {settings::Param::wal_persist_threshold, {{1 << 10, -(1 << 10)}, {1 << 12, -(1 << 12)}}},
};

}  // namespace noisepage::selfdriving::pilot
