#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"

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

  // Scan Nodes
  SEQSCAN,
  INDEXSCAN,
  HYBRIDSCAN,
  CSVSCAN,

  // Join Nodes
  NESTLOOP,
  HASHJOIN,
  MERGEJOIN,

  // Mutator Nodes
  UPDATE,
  INSERT,
  DELETE,
  APPEND,

  // DDL Nodes
  DROP,
  CREATE,
  POPULATE_INDEX,
  ANALYZE,
  CREATE_FUNC,

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
  MATERIALIZE,
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
// Order by Orderings
//===--------------------------------------------------------------------===//

enum class OrderByOrderingType { ASC, DESC };

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

//===--------------------------------------------------------------------===//
// Create Types
//===--------------------------------------------------------------------===//
enum class CreateType {
  INVALID = INVALID_TYPE_ID,  // invalid create type
  DB = 1,                     // db create type
  TABLE = 2,                  // table create type
  INDEX = 3,                  // index create type
  CONSTRAINT = 4,             // constraint create type
  TRIGGER = 5,                // trigger create type
  SCHEMA = 6,                 // schema create type
};

//===--------------------------------------------------------------------===//
// Drop Types
//===--------------------------------------------------------------------===//

enum class DropType {
  INVALID = INVALID_TYPE_ID,  // invalid drop type
  DB = 1,                     // db drop type
  TABLE = 2,                  // table drop type
  INDEX = 3,                  // index drop type
  CONSTRAINT = 4,             // constraint drop type
  TRIGGER = 5,                // trigger drop type
  SCHEMA = 6,                 // trigger drop type
};

//===--------------------------------------------------------------------===//
// Set Operation Types
//===--------------------------------------------------------------------===//

enum class SetOpType { INVALID = INVALID_TYPE_ID, INTERSECT = 1, INTERSECT_ALL = 2, EXCEPT = 3, EXCEPT_ALL = 4 };

// TODO(Gus,Wen) Tuple as a concept does not exist yet, someone need to define it in the storage layer, possibly a
/**
 * i hate u doxygen
 */
class Tuple {
 public:
  Tuple() = default;
};

}  // namespace terrier::plan_node
