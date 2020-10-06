#pragma once

#include <string>

#include "brain/brain_defs.h"

namespace terrier::brain {

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

}  // namespace terrier::brain
