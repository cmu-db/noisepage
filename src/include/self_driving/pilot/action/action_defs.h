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
enum class ActionFamily : uint8_t {
  CHANGE_INDEX,
  CHANGE_EXECUTION_MODE,
};

}  // namespace noisepage::selfdriving::pilot
