#pragma once

namespace terrier::plan_node {

constexpr int INVALID_TYPE_ID = 0;

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType {

  INVALID = INVALID_TYPE_ID,

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

}  // namespace terrier::plan_node