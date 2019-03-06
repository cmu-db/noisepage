#pragma once

namespace terrier::plan_node {

//===--------------------------------------------------------------------===//
// JSON (de)serialization declarations
//===--------------------------------------------------------------------===//

#define DEFINE_JSON_DECLARATIONS(ClassName)                                                    \
  inline void to_json(nlohmann::json &j, const ClassName &c) { j = c.ToJson(); }  /* NOLINT */ \
  inline void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */

constexpr int INVALID_TYPE_ID = 0;

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType {

  INVALID = INVALID_TYPE_ID,

  ABSTRACTPLAN,

  // Scan Nodes
  SEQSCAN,
  INDEXSCAN,
  HYBRIDSCAN,
  CSVSCAN,

  // Join Nodes
  NESTLOOP,
  HASHJOIN,

  // Mutator Nodes
  UPDATE,
  INSERT,
  DELETE,

  // DDL Nodes
  DROP,
  CREATE,
  POPULATE_INDEX,
  ANALYZE,

  // Algebra Nodes
  AGGREGATE,
  ORDERBY,
  PROJECTION,
  LIMIT,
  DISTINCT,
  HASH,

  // Utility
  EXPORT_EXTERNAL_FILE,

  // Test
  MOCK
};

//===--------------------------------------------------------------------===//
// Aggregate Stragegies
//===--------------------------------------------------------------------===//
enum class AggregateStrategy {
  INVALID = INVALID_TYPE_ID,
  SORTED = 1,
  HASH = 2,
  PLAIN = 3  // no group-by
};

//===--------------------------------------------------------------------===//
// Hybrid Scan Types
//===--------------------------------------------------------------------===//
enum class HybridScanType { INVALID = INVALID_TYPE_ID, SEQUENTIAL = 1, INDEX = 2, HYBRID = 3 };

//===--------------------------------------------------------------------===//
// Order by Orderings
//===--------------------------------------------------------------------===//

enum class OrderByOrdering { ASC, DESC };

//===--------------------------------------------------------------------===//
// Logical Join Types
//===--------------------------------------------------------------------===//
enum class LogicalJoinType {
  INVALID = INVALID_TYPE_ID,  // invalid join type
  LEFT = 1,                   // left
  RIGHT = 2,                  // right
  INNER = 3,                  // inner
  OUTER = 4,                  // outer
  SEMI = 5                    // IN+Subquery is SEMI
};

}  // namespace terrier::plan_node