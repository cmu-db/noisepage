#pragma once

namespace terrier::brain {

enum class ExecutionOperatingUnitType : uint32_t {
  INVALID,

  /**
   * AGGREGATE_BUILD
   * num_rows: # input rows to aggregation
   * cardinality: # unique values (although probably more correct is # of unique values based on group by clause)
   */
  AGGREGATE_BUILD,
  /**
   * AGGREGATE_ITERATE
   * num_rows: # rows output by aggregation
   * cardinality: # unique values output (either 1 for non-group by or # unique values based on group by)
   */
  AGGREGATE_ITERATE,

  /**
   * HASHJOIN_BUILD
   * num_rows: # input rows
   * cardinality: # unique tuples
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
   * num_rows: N/A
   * cardinality: N/A
   * This feature doesn't actually exist.
   * The recorder emits a IDX_SCAN (with num_loops = # rows output by the outer loop)
   */
  IDXJOIN,

  /**
   * SORT_BUILD
   * num_rows: # input tuples
   * cardinality: # unique values
   */
  SORT_BUILD,
  /**
   * SORT_ITERATE
   * num_rows: # tuples output
   * cardinality: # unique values
   */
  SORT_ITERATE,

  /**
   * SEQ_SCAN
   * num_rows: # tuples output (uncertain whether it's # accessed vs # after applying the filters)
   * cardinality: # unique values
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
   * num_rows: # input tuples
   * cardinality: # unique values
   */
  INSERT,
  /**
   * UPDATE
   * num_rows: # input tuples
   * cardinality: # unique values
   */
  UPDATE,
  /**
   * DELETE
   * num_rows: # input tuples
   * cardinality: # unique values
   */
  DELETE,

  /**
   * PROJECTION
   * num_rows:
   * cardinality:
   * This shouldn't be emitted?
   */
  PROJECTION,
  /**
   * OUTPUT
   * num_rows: # rows being output
   * cardinality: 1 for network output TODO(WAN): will when is this not 1?
   */
  OUTPUT,
  /**
   * LIMIT
   * num_rows:
   * cardinality:
   * This gets dropped right now... :(
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
