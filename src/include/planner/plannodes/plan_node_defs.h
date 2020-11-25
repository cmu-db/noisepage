#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"

namespace noisepage::parser {
class AbstractExpression;
}  // namespace noisepage::parser

namespace noisepage::planner {

/**
 * typedef for PlanNodeID
 */
STRONG_TYPEDEF_HEADER(plan_node_id_t, int32_t);

/**
 * Definition for a UNDEFINED_PLAN_NODE
 */
const plan_node_id_t UNDEFINED_PLAN_NODE = plan_node_id_t(-1);

//===--------------------------------------------------------------------===//
// JSON (de)serialization declarations
//===--------------------------------------------------------------------===//

constexpr int INVALID_TYPE_ID = 0;

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType {
  INVALID = INVALID_TYPE_ID,

  // Scan Nodes
  SEQSCAN,
  INDEXSCAN,
  CSVSCAN,

  // Join Nodes
  NESTLOOP,
  HASHJOIN,
  INDEXNLJOIN,

  // Mutator Nodes
  UPDATE,
  INSERT,
  DELETE,

  // DDL Nodes
  CREATE_DATABASE,
  CREATE_NAMESPACE,
  CREATE_TABLE,
  CREATE_INDEX,
  CREATE_FUNC,
  CREATE_TRIGGER,
  CREATE_VIEW,
  DROP_DATABASE,
  DROP_NAMESPACE,
  DROP_TABLE,
  DROP_INDEX,
  DROP_TRIGGER,
  DROP_VIEW,
  ANALYZE,

  // Algebra Nodes
  AGGREGATE,
  ORDERBY,
  PROJECTION,
  LIMIT,
  DISTINCT,
  HASH,
  SETOP,

  // Utility
  EXPORT_EXTERNAL_FILE,
  RESULT,

  // Test
  MOCK
};

//===--------------------------------------------------------------------===//
// Aggregate Stragegies
//===--------------------------------------------------------------------===//
enum class AggregateStrategyType {
  INVALID = INVALID_TYPE_ID,
  SORTED = 1,
  HASH = 2,
  PLAIN = 3  // no group-by
};

//===--------------------------------------------------------------------===//
// Logical Join Types
//===--------------------------------------------------------------------===//
enum class LogicalJoinType {
  INVALID = INVALID_TYPE_ID,  // invalid join type
  LEFT = 1,                   // left
  RIGHT = 2,                  // right
  INNER = 3,                  // inner
  OUTER = 4,                  // outer
  SEMI = 5,                   // returns a row ONLY if it has a join partner, no duplicates
  ANTI = 6,                   // returns a row ONLY if it has NO join partner, no duplicates
  LEFT_SEMI = 7,              // Left semi join
  RIGHT_SEMI = 8,             // Right semi join
  RIGHT_ANTI = 9              // Right anti join
};

/**
 * @return A string representation for the provided node type.
 */
std::string PlanNodeTypeToString(PlanNodeType type);

/**
 * @return A string representation for the provided join type.
 */
std::string JoinTypeToString(LogicalJoinType type);

//===--------------------------------------------------------------------===//
// Set Operation Types
//===--------------------------------------------------------------------===//

enum class SetOpType { INVALID = INVALID_TYPE_ID, INTERSECT = 1, INTERSECT_ALL = 2, EXCEPT = 3, EXCEPT_ALL = 4 };

//===--------------------------------------------------------------------===//
// External File defaults
//===--------------------------------------------------------------------===//

#define DEFAULT_DELIMETER_CHAR ','
#define DEFAULT_QUOTE_CHAR '"'
#define DEFAULT_ESCAPE_CHAR '"'
#define DEFAULT_NULL_STRING ""

//===--------------------------------------------------------------------===//
// IndexScanPlanNode
//===--------------------------------------------------------------------===//

using IndexExpression = common::ManagedPointer<parser::AbstractExpression>;
/** Type of index scan. */
enum class IndexScanType : uint8_t {
  Exact,
  AscendingClosed,
  AscendingOpenHigh,
  AscendingOpenLow,
  AscendingOpenBoth,

  Descending,
  DescendingLimit
};

}  // namespace noisepage::planner
