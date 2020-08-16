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

  /**
   * Determines whether an ExecutionOperatingUnitType can be merged
   *
   * Extracting features from pipelines can result in multiple features
   * of the same ExecutionOperatingUnitType. This function determines
   * whether the features can be merged (i.e arithmetic ops).
   *
   * @param f ExecutionOperatingUnitType to coalesce
   */
  static bool IsExecutionOperatingUnitTypeMergeable(ExecutionOperatingUnitType f);
};

}  // namespace terrier::brain
