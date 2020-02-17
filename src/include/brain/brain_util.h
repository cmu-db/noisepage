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
  static std::string ExecutionOperatingUnitTypeToString(ExecutionOperatingUnitType f) {
    switch (f) {
      case ExecutionOperatingUnitType::INVALID:
        return "INVALID";
      case ExecutionOperatingUnitType::AGGREGATE_BUILD:
        return "AGG_BUILD";
      case ExecutionOperatingUnitType::AGGREGATE_ITERATE:
        return "AGG_ITERATE";
      case ExecutionOperatingUnitType::HASHJOIN_BUILD:
        return "HASHJOIN_BUILD";
      case ExecutionOperatingUnitType::HASHJOIN_PROBE:
        return "HASHJOIN_PROBE";
      case ExecutionOperatingUnitType::NLJOIN_LEFT:
        return "NLJOIN_LEFT";
      case ExecutionOperatingUnitType::NLJOIN_RIGHT:
        return "NLJOIN_RIGHT";
      case ExecutionOperatingUnitType::IDXJOIN:
        return "IDXJOIN";
      case ExecutionOperatingUnitType::SORT_BUILD:
        return "SORT_BUILD";
      case ExecutionOperatingUnitType::SORT_ITERATE:
        return "SORT_ITERATE";
      case ExecutionOperatingUnitType::SEQ_SCAN:
        return "SEQ_SCAN";
      case ExecutionOperatingUnitType::IDX_SCAN:
        return "IDX_SCAN";
      case ExecutionOperatingUnitType::INSERT:
        return "INSERT";
      case ExecutionOperatingUnitType::UPDATE:
        return "UPDATE";
      case ExecutionOperatingUnitType::DELETE:
        return "DELETE";
      case ExecutionOperatingUnitType::PROJECTION:
        return "PROJECTION";
      case ExecutionOperatingUnitType::OUTPUT:
        return "OUTPUT";
      case ExecutionOperatingUnitType::OP_ADD:
        return "OP_ADD";
      case ExecutionOperatingUnitType::OP_SUBTRACT:
        return "OP_SUBTRACT";
      case ExecutionOperatingUnitType::OP_MULTIPLY:
        return "OP_MULTIPLY";
      case ExecutionOperatingUnitType::OP_DIVIDE:
        return "OP_DIVIDE";
      case ExecutionOperatingUnitType::OP_COMPARE_EQ:
        return "OP_COMPARE_EQ";
      case ExecutionOperatingUnitType::OP_COMPARE_NEQ:
        return "OP_COMPARE_NEQ";
      case ExecutionOperatingUnitType::OP_COMPARE_LT:
        return "OP_COMPARE_LT";
      case ExecutionOperatingUnitType::OP_COMPARE_GT:
        return "OP_COMPARE_GT";
      case ExecutionOperatingUnitType::OP_COMPARE_LTE:
        return "OP_COMPARE_LTE";
      case ExecutionOperatingUnitType::OP_COMPARE_GTE:
        return "OP_COMPARE_GTE";
      default:
        UNREACHABLE("Undefined ExecutionOperatingUnitType encountered");
        break;
    }
  }
};

}  // namespace terrier::brain
