#pragma once

#include "common/strong_typedef.h"

namespace noisepage::selfdriving::pilot {

/**
 * typedef for action ID
 */
STRONG_TYPEDEF_HEADER(action_id_t, int32_t);

/**
 * Metric types
 */
enum class ActionType : uint8_t {
  CREATE_INDEX,
  DROP_INDEX,
  CHANGE_KNOB,
};

}  // namespace noisepage::selfdriving::pilot
