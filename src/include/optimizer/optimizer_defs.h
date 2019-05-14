#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "parser/expression_defs.h"

namespace terrier {

namespace parser {
class AbstractExpression;
}

namespace optimizer {
/**
 * Property types.
 */
enum class PropertyType : uint8_t {
  INVALID,
  COLUMNS,
  DISTINCT,
  SORT,
  LIMIT,
};

/**
 * Operator type
 */
enum class OpType {
  Undefined = 0,

  // Logical Operators
  LogicalGet,
  LogicalExternalFileGet,
  LogicalQueryDerivedGet,
  LogicalProjection,
  LogicalFilter,
  LogicalMarkJoin,
  LogicalDependentJoin,
  LogicalSingleJoin,
  LogicalInnerJoin,
  LogicalLeftJoin,
  LogicalRightJoin,
  LogicalOuterJoin,
  LogicalSemiJoin,
  LogicalAggregateAndGroupBy,
  LogicalInsert,
  LogicalInsertSelect,
  LogicalDelete,
  LogicalUpdate,
  LogicalLimit,
  LogicalDistinct,
  LogicalExportExternalFile,

  // Separate between logical and physical ops
  LogicalPhysicalDelimiter,

  // Physical Operators
  TableFreeScan,  // Scan Op for SELECT without FROM
  SeqScan,
  IndexScan,
  ExternalFileScan,
  QueryDerivedScan,
  OrderBy,
  Limit,
  Distinct,
  InnerNLJoin,
  LeftNLJoin,
  RightNLJoin,
  OuterNLJoin,
  InnerHashJoin,
  LeftHashJoin,
  RightHashJoin,
  OuterHashJoin,
  Insert,
  InsertSelect,
  Delete,
  Update,
  Aggregate,
  HashGroupBy,
  SortGroupBy,
  ExportExternalFile,
};

/**
 * Operator category, logical or physical
 */
enum OpCategory { LOGICAL, PHYSICAL };

// Augment abstract expression with a table OID set
struct AnnotatedExpression {
  AnnotatedExpression(std::shared_ptr<parser::AbstractExpression> expr,
                      std::unordered_set<catalog::table_oid_t> &&table_oid_set)
      : expr_(std::move(expr)), table_oid_set_(std::move(table_oid_set)) {}
  AnnotatedExpression(const AnnotatedExpression &ant_expr) = default;
  std::shared_ptr<parser::AbstractExpression> expr_;
  std::unordered_set<catalog::table_oid_t> table_oid_set_;
};

}  // namespace optimizer
}  // namespace terrier
