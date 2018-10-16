#pragma once

namespace terrier::sql::plannode {

/**
 * SQL PlanNode Types
 */
enum class PlanNodeType {
  INVALID = INVALID_TYPE_ID,  // invalid plan node type

  // Scan Nodes
  SEQSCAN = 10,
  INDEXSCAN = 11,
  CSVSCAN = 12,

  // Join Nodes
  NESTLOOP = 20,
  NESTLOOPINDEX = 21,
  MERGEJOIN = 22,
  HASHJOIN = 23,

  // Mutator Nodes
  UPDATE = 30,
  INSERT = 31,
  DELETE = 32,

  // DDL Nodes
  DROP = 33,
  CREATE = 34,
  POPULATE_INDEX = 35,
  ANALYZE = 36,

  // Communication Nodes
  SEND = 40,
  RECEIVE = 41,
  PRINT = 42,

  // Algebra Nodes
  AGGREGATE = 50,
  UNION = 52,
  ORDERBY = 53,
  PROJECTION = 54,
  MATERIALIZE = 55,
  LIMIT = 56,
  DISTINCT = 57,
  SETOP = 58,   // set operation
  APPEND = 59,  // append
  AGGREGATE_V2 = 61,
  HASH = 62,

  // Utility
  RESULT = 70,
  EXPORT_EXTERNAL_FILE = 71,
  CREATE_FUNC = 72,

  // Test
  MOCK = 80
};

/**
 * Set Operation Types
 * These are used by the SetOpPlanNode
 */
enum class SetOpType {
  // clang-format off
  INVALID = INVALID_TYPE_ID,
  INTERSECT = 1,
  INTERSECT_ALL = 2,
  EXCEPT = 3,
  EXCEPT_ALL = 4
  // clang-format on
};

}  // namespace terrier::sql::plannode
