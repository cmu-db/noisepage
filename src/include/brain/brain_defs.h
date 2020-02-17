#pragma once

namespace terrier::brain {

enum class ExecutionOperatingUnitType : uint32_t {
  INVALID,

  AGGREGATE_BUILD,
  AGGREGATE_ITERATE,

  HASHJOIN_BUILD,
  HASHJOIN_PROBE,

  NLJOIN_LEFT,
  NLJOIN_RIGHT,
  IDXJOIN,

  SORT_BUILD,
  SORT_ITERATE,

  SEQ_SCAN,
  IDX_SCAN,

  INSERT,
  UPDATE,
  DELETE,

  PROJECTION,
  OUTPUT,

  OP_PLUS_OR_MINUS,
  OP_MULTIPLY,
  OP_DIVIDE,
  OP_COMPARE,
};

}  // namespace terrier::brain
