#pragma once

#include <string>

#include "self_driving/brain_defs.h"

namespace noisepage::selfdriving {

/**
 * Utility class for helper functions
 */
class BrainUtil {
 public:
  /**
   * Converts an ExecutionOperatingUnitType enum to string representation
   * @param f ExecutionOperatingUnitType to convert
   */
  static std::string ExecutionOperatingUnitTypeToString(ExecutionOperatingUnitType f);
};

}  // namespace noisepage::selfdriving
