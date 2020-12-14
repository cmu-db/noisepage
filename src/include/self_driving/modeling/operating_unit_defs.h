#pragma once

namespace noisepage::selfdriving {

enum class ExecutionOperatingUnitType : uint32_t {
  /** INVALID is associated with translators that are INVALID no matter what. */
  INVALID,

  /**
   * DUMMY is associated with translators that have no inherent type as the type will depend on other factors,
   * for example, the same translator may be used to create both HASHJOIN_BUILD and HASHJOIN_PROBE pipelines.
   */
  DUMMY,

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
   * SORT_BUILD
   * num_rows: # input tuples
   * cardinality: # unique values
   */
  SORT_BUILD,
  /**
   * TOPK_SORT_BUILD
   * num_rows: # input tuples
   * cardinality: K from TopK
   */
  SORT_TOPK_BUILD,
  /**
   * SORT_ITERATE
   * num_rows: # tuples output
   * cardinality: # unique values
   */
  SORT_ITERATE,

  /**
   * SEQ_SCAN
   * num_rows: the number of tuples being accessed (including tuples that do not pass filtering)
   * cardinality: same as num_rows
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
   * cardinality: same as num_rows
   */
  INSERT,
  /**
   * UPDATE
   * num_rows: # input tuples
   * cardinality: same as num_rows
   */
  UPDATE,
  /**
   * DELETE
   * num_rows: # input tuples
   * cardinality: same as num_rows
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
   * cardinality: 1 for network output, 0 for NoOpResultConsumer
   */
  OUTPUT,
  /**
   * LIMIT
   * num_rows:
   * cardinality:
   */
  LIMIT,

  /**
   * num_rows: index size
   * cardinality (training): batch number of indexes
   * cardinality (inference): number of index inserts or deletes
   */
  INDEX_INSERT,
  INDEX_DELETE,

  PARALLEL_MERGE_HASHJOIN,
  PARALLEL_MERGE_AGGBUILD,
  PARALLEL_SORT_STEP,
  PARALLEL_SORT_MERGE_STEP,
  PARALLEL_SORT_TOPK_STEP,
  PARALLEL_SORT_TOPK_MERGE_STEP,
  CREATE_INDEX,
  CREATE_INDEX_MAIN,

  /**
   * Use to demarcate plan and operations.
   * For all operations, cardinality = num_rows.
   */
  PLAN_OPS_DELIMITER,

  OP_INTEGER_PLUS_OR_MINUS,
  OP_INTEGER_MULTIPLY,
  OP_INTEGER_DIVIDE,
  OP_INTEGER_COMPARE,
  OP_REAL_PLUS_OR_MINUS,
  OP_REAL_MULTIPLY,
  OP_REAL_DIVIDE,
  OP_REAL_COMPARE,
  OP_BOOL_COMPARE,
  OP_VARCHAR_COMPARE
};

/** The attributes of an ExecutionOperatingUnitFeature that can be set from TPL. */
enum class ExecutionOperatingUnitFeatureAttribute : uint8_t { NUM_ROWS, CARDINALITY, NUM_LOOPS };

enum class ExecutionOperatingUnitFeatureUpdateMode : uint8_t { SET, ADD, MULT };

}  // namespace noisepage::selfdriving
