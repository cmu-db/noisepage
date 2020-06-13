#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"

namespace terrier::parser {
class AbstractExpression;
}  // namespace terrier::parser

namespace terrier::planner {

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
  SEMI = 5,                   // IN+Subquery is SEMI
  LEFT_SEMI = 6               // LEFT SEMI join
};

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

}  // namespace terrier::planner
