#pragma once

namespace terrier::brain {

enum class ExecutionOperatingUnitType : uint32_t {
  INVALID,

  /**
   * AGGREGATE_BUILD
   * num_rows:
   * cardinality:
   */
  AGGREGATE_BUILD,
  /**
   * AGGREGATE_ITERATE
   * num_rows:
   * cardinality:
   */
  AGGREGATE_ITERATE,

  /**
   * HASHJOIN_BUILD
   * num_rows:
   * cardinality:
   */
  HASHJOIN_BUILD,
  /**
   * HASHJOIN_PROBE
   * num_rows: number of probes
   * cardinality: number of matches
   */
  HASHJOIN_PROBE,

  /**
   * IDXJOIN
   * num_rows:
   * cardinality:
   */
  IDXJOIN,

  /**
   * SORT_BUILD
   * num_rows:
   * cardinality:
   */
  SORT_BUILD,
  /**
   * SORT_ITERATE
   * num_rows:
   * cardinality:
   */
  SORT_ITERATE,

  /**
   * SEQ_SCAN
   * num_rows:
   * cardinality:
   */
  SEQ_SCAN,
  /**
   * IDX_SCAN
   * num_rows: size of index
   * cardinality: size of scan
   */
  IDX_SCAN,

  /**
   * INSERT
   * num_rows:
   * cardinality:
   */
  INSERT,
  /**
   * UPDATE
   * num_rows:
   * cardinality:
   */
  UPDATE,
  /**
   * DELETE
   * num_rows:
   * cardinality:
   */
  DELETE,

  /**
   * PROJECTION
   * num_rows:
   * cardinality:
   */
  PROJECTION,
  /**
   * OUTPUT
   * num_rows:
   * cardinality:
   */
  OUTPUT,
  /**
   * LIMIT
   * num_rows:
   * cardinality:
   */
  LIMIT,

  /**
   * HASH_JOIN
   */
  HASH_JOIN,
  /**
   * HASH_AGGREGATE
   */
  HASH_AGGREGATE,
  /**
   * CSV_SCAN
   */
  CSV_SCAN,
  /**
   * NL_JOIN
   */
  NL_JOIN,
  /**
   * SORT
   */
  SORT,
  /**
   * STATIC_AGGREGATE
   */
  STATIC_AGGREGATE,

  /**
   * Use to demarcate plan and operations.
   * For all operations, cardinality = num_rows.
   */
  PLAN_OPS_DELIMITER,

  OP_INTEGER_PLUS_OR_MINUS,
  OP_INTEGER_MULTIPLY,
  OP_INTEGER_DIVIDE,
  OP_INTEGER_COMPARE,
  OP_DECIMAL_PLUS_OR_MINUS,
  OP_DECIMAL_MULTIPLY,
  OP_DECIMAL_DIVIDE,
  OP_DECIMAL_COMPARE,
  OP_BOOL_COMPARE
};

/** The attributes of an ExecutionOperatingUnitFeature that can be set from TPL. */
enum class ExecutionOperatingUnitFeatureAttribute : uint8_t { NUM_ROWS, CARDINALITY };

}  // namespace terrier::brain
