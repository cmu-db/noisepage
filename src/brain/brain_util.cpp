#include "brain/brain_util.h"
#include "execution/util/execution_common.h"

namespace terrier::brain {

std::string BrainUtil::ExecutionOperatingUnitTypeToString(ExecutionOperatingUnitType f) {
  // NOTE: Before adding any extra case to this switch statement,
  // please ensure that the output type is actually supported
  // by the mini-runner infrastructure.
  switch (f) {
    case ExecutionOperatingUnitType::AGGREGATE_BUILD:
      return "AGG_BUILD";
    case ExecutionOperatingUnitType::AGGREGATE_ITERATE:
      return "AGG_ITERATE";
    case ExecutionOperatingUnitType::HASHJOIN_BUILD:
      return "HASHJOIN_BUILD";
    case ExecutionOperatingUnitType::HASHJOIN_PROBE:
      return "HASHJOIN_PROBE";
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
    case ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS:
      return "OP_INTEGER_PLUS_OR_MINUS";
    case ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY:
      return "OP_INTEGER_MULTIPLY";
    case ExecutionOperatingUnitType::OP_INTEGER_DIVIDE:
      return "OP_INTEGER_DIVIDE";
    case ExecutionOperatingUnitType::OP_INTEGER_COMPARE:
      return "OP_INTEGER_COMPARE";
    case ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS:
      return "OP_DECIMAL_PLUS_OR_MINUS";
    case ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY:
      return "OP_DECIMAL_MULTIPLY";
    case ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE:
      return "OP_DECIMAL_DIVIDE";
    case ExecutionOperatingUnitType::OP_DECIMAL_COMPARE:
      return "OP_DECIMAL_COMPARE";
    case ExecutionOperatingUnitType::OP_BOOL_COMPARE:
      return "OP_BOOL_COMPARE";
    case ExecutionOperatingUnitType::OUTPUT:
      return "OUTPUT";
    case ExecutionOperatingUnitType::LIMIT:
      return "LIMIT";
    case ExecutionOperatingUnitType::CREATE_INDEX:
      return "CREATE_INDEX";
    default:
      UNREACHABLE("Undefined ExecutionOperatingUnitType encountered");
      break;
  }
}

}  // namespace terrier::brain
