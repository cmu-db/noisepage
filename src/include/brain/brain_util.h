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
   * Converts an OperatingUnitFeatureType enum to string representation
   * @param f OperatingUnitFeatureType to convert
   */
  static std::string OperatingUnitFeatureTypeToString(OperatingUnitFeatureType f) {
    switch (f) {
      case OperatingUnitFeatureType::INVALID:
        return "INVALID";
      case OperatingUnitFeatureType::AGGREGATE_BUILD:
        return "AGG_BUILD";
      case OperatingUnitFeatureType::AGGREGATE_ITERATE:
        return "AGG_ITERATE";
      case OperatingUnitFeatureType::HASHJOIN_BUILD:
        return "HASHJOIN_BUILD";
      case OperatingUnitFeatureType::HASHJOIN_PROBE:
        return "HASHJOIN_PROBE";
      case OperatingUnitFeatureType::NLJOIN_LEFT:
        return "NLJOIN_LEFT";
      case OperatingUnitFeatureType::NLJOIN_RIGHT:
        return "NLJOIN_RIGHT";
      case OperatingUnitFeatureType::IDXJOIN:
        return "IDXJOIN";
      case OperatingUnitFeatureType::SORT_BUILD:
        return "SORT_BUILD";
      case OperatingUnitFeatureType::SORT_ITERATE:
        return "SORT_ITERATE";
      case OperatingUnitFeatureType::SEQ_SCAN:
        return "SEQ_SCAN";
      case OperatingUnitFeatureType::IDX_SCAN:
        return "IDX_SCAN";
      case OperatingUnitFeatureType::INSERT:
        return "INSERT";
      case OperatingUnitFeatureType::UPDATE:
        return "UPDATE";
      case OperatingUnitFeatureType::DELETE:
        return "DELETE";
      case OperatingUnitFeatureType::PROJECTION:
        return "PROJECTION";
      case OperatingUnitFeatureType::OUTPUT:
        return "OUTPUT";
      case OperatingUnitFeatureType::OP_ADD:
        return "OP_ADD";
      case OperatingUnitFeatureType::OP_SUBTRACT:
        return "OP_SUBTRACT";
      case OperatingUnitFeatureType::OP_MULTIPLY:
        return "OP_MULTIPLY";
      case OperatingUnitFeatureType::OP_DIVIDE:
        return "OP_DIVIDE";
      case OperatingUnitFeatureType::OP_COMPARE_EQ:
        return "OP_COMPARE_EQ";
      case OperatingUnitFeatureType::OP_COMPARE_NEQ:
        return "OP_COMPARE_NEQ";
      case OperatingUnitFeatureType::OP_COMPARE_LT:
        return "OP_COMPARE_LT";
      case OperatingUnitFeatureType::OP_COMPARE_GT:
        return "OP_COMPARE_GT";
      case OperatingUnitFeatureType::OP_COMPARE_LTE:
        return "OP_COMPARE_LTE";
      case OperatingUnitFeatureType::OP_COMPARE_GTE:
        return "OP_COMPARE_GTE";
      default:
        UNREACHABLE("Undefined OperatingUnitFeatureType encountered");
        break;
    }
  }
};

}  // namespace terrier::brain
