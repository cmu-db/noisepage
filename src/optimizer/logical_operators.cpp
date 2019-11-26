#include "optimizer/logical_operators.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "optimizer/operator_visitor.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::optimizer {
//===--------------------------------------------------------------------===//
// Logical Get
//===--------------------------------------------------------------------===//
Operator LogicalGet::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                          catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> predicates,
                          std::string table_alias, bool is_for_update) {
  auto get = std::make_unique<LogicalGet>();
  get->database_oid_ = database_oid;
  get->namespace_oid_ = namespace_oid;
  get->table_oid_ = table_oid;
  get->predicates_ = std::move(predicates);
  get->table_alias_ = std::move(table_alias);
  get->is_for_update_ = is_for_update;
  return Operator(std::move(get));
}

Operator LogicalGet::Make() {
  auto get = std::make_unique<LogicalGet>();
  get->database_oid_ = catalog::INVALID_DATABASE_OID;
  get->namespace_oid_ = catalog::INVALID_NAMESPACE_OID;
  get->table_oid_ = catalog::INVALID_TABLE_OID;
  get->is_for_update_ = false;
  return Operator(std::move(get));
}

common::hash_t LogicalGet::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, predicates_.begin(), predicates_.end());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
  return hash;
}

bool LogicalGet::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALGET) return false;
  const LogicalGet &node = *dynamic_cast<const LogicalGet *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (predicates_.size() != node.predicates_.size()) return false;
  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) return false;
  }
  if (table_alias_ != node.table_alias_) return false;
  return is_for_update_ == node.is_for_update_;
}

//===--------------------------------------------------------------------===//
// External file get
//===--------------------------------------------------------------------===//

Operator LogicalExternalFileGet::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
                                      char quote, char escape) {
  auto get = std::make_unique<LogicalExternalFileGet>();
  get->format_ = format;
  get->file_name_ = std::move(file_name);
  get->delimiter_ = delimiter;
  get->quote_ = quote;
  get->escape_ = escape;
  return Operator(std::move(get));
}

bool LogicalExternalFileGet::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALEXTERNALFILEGET) return false;
  const auto &get = *static_cast<const LogicalExternalFileGet *>(&r);
  return (format_ == get.format_ && file_name_ == get.file_name_ && delimiter_ == get.delimiter_ &&
          quote_ == get.quote_ && escape_ == get.escape_);
}

common::hash_t LogicalExternalFileGet::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
  return hash;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
Operator LogicalQueryDerivedGet::Make(
    std::string table_alias,
    std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map) {
  auto get = std::make_unique<LogicalQueryDerivedGet>();
  get->table_alias_ = std::move(table_alias);
  get->alias_to_expr_map_ = std::move(alias_to_expr_map);
  return Operator(std::move(get));
}

bool LogicalQueryDerivedGet::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALQUERYDERIVEDGET) return false;
  const LogicalQueryDerivedGet &node = *static_cast<const LogicalQueryDerivedGet *>(&r);
  if (table_alias_ != node.table_alias_) return false;
  return alias_to_expr_map_ == node.alias_to_expr_map_;
}

common::hash_t LogicalQueryDerivedGet::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  for (auto &iter : alias_to_expr_map_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(iter.first));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(iter.second));
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalFilter
//===--------------------------------------------------------------------===//

Operator LogicalFilter::Make(std::vector<AnnotatedExpression> &&predicates) {
  auto op = std::make_unique<LogicalFilter>();
  op->predicates_ = std::move(predicates);
  return Operator(std::move(op));
}

bool LogicalFilter::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALFILTER) return false;
  const LogicalFilter &node = *static_cast<const LogicalFilter *>(&r);

  // This is technically incorrect because the predicates
  // are supposed to be unsorted and this equals check is
  // comparing values at each offset.
  return (predicates_ == node.predicates_);
}

common::hash_t LogicalFilter::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  // Again, I think that this is wrong because the value of the hash is based
  // on the location order of the expressions.
  for (auto &pred : predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalProjection
//===--------------------------------------------------------------------===//

Operator LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&expressions) {
  auto op = std::make_unique<LogicalProjection>();
  op->expressions_ = std::move(expressions);
  return Operator(std::move(op));
}

bool LogicalProjection::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALPROJECTION) return false;
  const LogicalProjection &node = *static_cast<const LogicalProjection *>(&r);
  for (size_t i = 0; i < expressions_.size(); i++) {
    if (*(expressions_[i]) != *(node.expressions_[i])) return false;
  }
  return true;
}

common::hash_t LogicalProjection::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : expressions_) hash = common::HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalInsert
//===--------------------------------------------------------------------===//

Operator LogicalInsert::Make(
    catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
    std::vector<catalog::col_oid_t> &&columns,
    common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>> values) {
#ifndef NDEBUG
  // We need to check whether the number of values for each insert vector
  // matches the number of columns
  for (const auto &insert_vals : *values) {
    TERRIER_ASSERT(columns.size() == insert_vals.size(), "Mismatched number of columns and values");
  }
#endif

  auto op = std::make_unique<LogicalInsert>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  op->values_ = values;
  return Operator(std::move(op));
}

common::hash_t LogicalInsert::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());

  // Perform a deep hash of the values
  for (const auto &insert_vals : *values_) {
    hash = common::HashUtil::CombineHashInRange(hash, insert_vals.begin(), insert_vals.end());
  }

  return hash;
}

bool LogicalInsert::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALINSERT) return false;
  const LogicalInsert &node = *dynamic_cast<const LogicalInsert *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (columns_ != node.columns_) return false;
  if (values_ != node.values_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalInsertSelect
//===--------------------------------------------------------------------===//

Operator LogicalInsertSelect::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                   catalog::table_oid_t table_oid) {
  auto op = std::make_unique<LogicalInsertSelect>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  return Operator(std::move(op));
}

common::hash_t LogicalInsertSelect::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool LogicalInsertSelect::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALINSERTSELECT) return false;
  const LogicalInsertSelect &node = *dynamic_cast<const LogicalInsertSelect *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalDistinct
//===--------------------------------------------------------------------===//

Operator LogicalDistinct::Make() {
  // We don't have anything that we need to store, so we're just going
  // to throw this mofo out to the world as is...
  return Operator(std::make_unique<LogicalDistinct>());
}

bool LogicalDistinct::operator==(const BaseOperatorNode &r) {
  return (r.GetType() == OpType::LOGICALDISTINCT);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t LogicalDistinct::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  // I guess every LogicalDistinct object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalLimit
//===--------------------------------------------------------------------===//

Operator LogicalLimit::Make(size_t offset, size_t limit,
                            std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_exprs,
                            std::vector<optimizer::OrderByOrderingType> &&sort_directions) {
  TERRIER_ASSERT(sort_exprs.size() == sort_directions.size(), "Mismatched ORDER BY expressions + directions");
  auto op = std::make_unique<LogicalLimit>();
  op->offset_ = offset;
  op->limit_ = limit;
  op->sort_exprs_ = sort_exprs;
  op->sort_directions_ = std::move(sort_directions);
  return Operator(std::move(op));
}

bool LogicalLimit::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALLIMIT) return false;
  const LogicalLimit &node = *static_cast<const LogicalLimit *>(&r);
  if (offset_ != node.offset_) return false;
  if (limit_ != node.limit_) return false;
  if (sort_exprs_ != node.sort_exprs_) return false;
  if (sort_directions_ != node.sort_directions_) return false;
  return (true);
}

common::hash_t LogicalLimit::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));
  hash = common::HashUtil::CombineHashInRange(hash, sort_exprs_.begin(), sort_exprs_.end());
  hash = common::HashUtil::CombineHashInRange(hash, sort_directions_.begin(), sort_directions_.end());
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalDelete
//===--------------------------------------------------------------------===//

Operator LogicalDelete::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::table_oid_t table_oid) {
  auto op = std::make_unique<LogicalDelete>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  return Operator(std::move(op));
}

common::hash_t LogicalDelete::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool LogicalDelete::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALDELETE) return false;
  const LogicalDelete &node = *dynamic_cast<const LogicalDelete *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalUpdate
//===--------------------------------------------------------------------===//

Operator LogicalUpdate::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::table_oid_t table_oid,
                             std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates) {
  auto op = std::make_unique<LogicalUpdate>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->updates_ = std::move(updates);
  return Operator(std::move(op));
}

common::hash_t LogicalUpdate::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  for (const auto &clause : updates_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(clause->GetUpdateValue()));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(clause->GetColumnName()));
  }
  return hash;
}

bool LogicalUpdate::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALUPDATE) return false;
  const LogicalUpdate &node = *dynamic_cast<const LogicalUpdate *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (updates_.size() != node.updates_.size()) return false;
  for (size_t i = 0; i < updates_.size(); i++) {
    if (*(updates_[i]) != *(node.updates_[i])) return false;
  }
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalExportExternalFile
//===--------------------------------------------------------------------===//

Operator LogicalExportExternalFile::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
                                         char quote, char escape) {
  auto op = std::make_unique<LogicalExportExternalFile>();
  op->format_ = format;
  op->file_name_ = std::move(file_name);
  op->delimiter_ = delimiter;
  op->quote_ = quote;
  op->escape_ = escape;
  return Operator(std::move(op));
}

common::hash_t LogicalExportExternalFile::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
  return hash;
}

bool LogicalExportExternalFile::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALEXPORTEXTERNALFILE) return false;
  const LogicalExportExternalFile &node = *dynamic_cast<const LogicalExportExternalFile *>(&r);
  if (format_ != node.format_) return false;
  if (file_name_ != node.file_name_) return false;
  if (delimiter_ != node.delimiter_) return false;
  if (quote_ != node.quote_) return false;
  if (escape_ != node.escape_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// Logical Dependent Join
//===--------------------------------------------------------------------===//
Operator LogicalDependentJoin::Make() { return Operator(std::make_unique<LogicalDependentJoin>()); }

Operator LogicalDependentJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalDependentJoin>();
  join->join_predicates_ = std::move(join_predicates);
  return Operator(std::move(join));
}

common::hash_t LogicalDependentJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalDependentJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALDEPENDENTJOIN) return false;
  const LogicalDependentJoin &node = *static_cast<const LogicalDependentJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
Operator LogicalMarkJoin::Make() { return Operator(std::make_unique<LogicalMarkJoin>()); }

Operator LogicalMarkJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalMarkJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalMarkJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalMarkJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALMARKJOIN) return false;
  const LogicalMarkJoin &node = *static_cast<const LogicalMarkJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
Operator LogicalSingleJoin::Make() { return Operator(std::make_unique<LogicalSingleJoin>()); }

Operator LogicalSingleJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalSingleJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalSingleJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalSingleJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALSINGLEJOIN) return false;
  const LogicalSingleJoin &node = *static_cast<const LogicalSingleJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
Operator LogicalInnerJoin::Make() { return Operator(std::make_unique<LogicalInnerJoin>()); }

Operator LogicalInnerJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalInnerJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalInnerJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalInnerJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALINNERJOIN) return false;
  const LogicalInnerJoin &node = *static_cast<const LogicalInnerJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
Operator LogicalLeftJoin::Make() { return Operator(std::make_unique<LogicalLeftJoin>()); }

Operator LogicalLeftJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalLeftJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalLeftJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalLeftJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALLEFTJOIN) return false;
  const LogicalLeftJoin &node = *static_cast<const LogicalLeftJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
Operator LogicalRightJoin::Make() { return Operator(std::make_unique<LogicalRightJoin>()); }

Operator LogicalRightJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalRightJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalRightJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalRightJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALRIGHTJOIN) return false;
  const LogicalRightJoin &node = *static_cast<const LogicalRightJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalOuterJoin::Make() { return Operator(std::make_unique<LogicalOuterJoin>()); }

Operator LogicalOuterJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalOuterJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalOuterJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalOuterJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALOUTERJOIN) return false;
  const LogicalOuterJoin &node = *static_cast<const LogicalOuterJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
Operator LogicalSemiJoin::Make() { return Operator(std::make_unique<LogicalSemiJoin>()); }

Operator LogicalSemiJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto join = std::make_unique<LogicalSemiJoin>();
  join->join_predicates_ = join_predicates;
  return Operator(std::move(join));
}

common::hash_t LogicalSemiJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
    }
  }
  return hash;
}

bool LogicalSemiJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALSEMIJOIN) return false;
  const LogicalSemiJoin &node = *static_cast<const LogicalSemiJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
Operator LogicalAggregateAndGroupBy::Make() { return Operator(std::make_unique<LogicalAggregateAndGroupBy>()); }

Operator LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns) {
  auto group_by = std::make_unique<LogicalAggregateAndGroupBy>();
  group_by->columns_ = std::move(columns);
  return Operator(std::move(group_by));
}

Operator LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                                          std::vector<AnnotatedExpression> &&having) {
  auto group_by = std::make_unique<LogicalAggregateAndGroupBy>();
  group_by->columns_ = std::move(columns);
  group_by->having_ = std::move(having);
  return Operator(std::move(group_by));
}
bool LogicalAggregateAndGroupBy::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALAGGREGATEANDGROUPBY) return false;
  const LogicalAggregateAndGroupBy &node = *static_cast<const LogicalAggregateAndGroupBy *>(&r);
  if (having_.size() != node.having_.size() || columns_.size() != node.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i] != node.having_[i]) return false;
  }
  // originally throw all entries from each vector to one set,
  // and compare if the 2 sets are equal
  // this solves the problem of order of expression within the same hierarchy
  // however causing shared_ptrs to be compared by their memory location to which they points to
  // rather than the content of the object they point to.

  //  std::unordered_set<std::shared_ptr<parser::AbstractExpression>> l_set, r_set;
  //  for (auto &expr : columns_) l_set.emplace(expr);
  //  for (auto &expr : node.columns_) r_set.emplace(expr);
  //  return l_set == r_set;
  for (size_t i = 0; i < columns_.size(); i++) {
    if (*(columns_[i]) != *(node.columns_[i])) return false;
  }
  return true;
}

common::hash_t LogicalAggregateAndGroupBy::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
  }
  for (auto &expr : columns_) hash = common::HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalCreateDatabase
//===--------------------------------------------------------------------===//

Operator LogicalCreateDatabase::Make(std::string database_name) {
  auto op = std::make_unique<LogicalCreateDatabase>();
  op->database_name_ = std::move(database_name);
  return Operator(std::move(op));
}

common::hash_t LogicalCreateDatabase::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));
  return hash;
}

bool LogicalCreateDatabase::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALCREATEDATABASE) return false;
  const LogicalCreateDatabase &node = *dynamic_cast<const LogicalCreateDatabase *>(&r);
  return node.database_name_ == database_name_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateFunction
//===--------------------------------------------------------------------===//

Operator LogicalCreateFunction::Make(catalog::namespace_oid_t namespace_oid, std::string function_name, parser::PLType language, std::vector<std::string> &&function_body,  std::vector<std::string> &&function_param_names, std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types, parser::BaseFunctionParameter::DataType return_type, int param_count, bool replace) {
  auto op = std::make_unique<LogicalCreateFunction>();
  op->namespace_oid_ = namespace_oid;
  op->function_name_ = std::move(function_name);
  op->function_body_ = std::move(function_body);
  op->function_param_names_ = std::move(function_param_names);
  op->function_param_types_ = std::move(function_param_types);
  op->is_replace_ = replace;
  op->param_count_ = param_count;
  op->return_type_ = return_type;
  op->language_ = language;
  return Operator(std::move(op));
}

common::hash_t LogicalCreateFunction::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(param_count_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_replace_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(return_type_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(language_));
  hash = common::HashUtil::CombineHashInRange(hash, function_body_.begin(), function_body_.end());
  hash = common::HashUtil::CombineHashInRange(hash, function_param_names_.begin(), function_param_names_.end());
  hash = common::HashUtil::CombineHashInRange(hash, function_param_types_.begin(), function_param_types_.end());
  return hash;
}

bool LogicalCreateFunction::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALDELETE) return false;
  const LogicalCreateFunction &node = *dynamic_cast<const LogicalCreateFunction *>(&r);
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (function_name_ != node.function_name_) return false;
  if (function_body_ != node.function_body_) return false;
  if (param_count_ != node.param_count_) return false;
  if (return_type_ != node.return_type_) return false;
  if (function_param_types_ != node.function_param_types_) return false;
  if (function_param_names_ != node.function_param_names_) return false;
  if (is_replace_ != node.is_replace_) return false;
  return language_ == node.language_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateIndex
//===--------------------------------------------------------------------===//

Operator LogicalCreateIndex::Make(catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid, parser::IndexType index_type, bool unique, std::string index_name, std::vector<common::ManagedPointer<parser::AbstractExpression>> index_attrs) {
  auto op = std::make_unique<LogicalCreateIndex>();
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->index_type_ = index_type;
  op->unique_index_ = unique;
  op->index_name_ = std::move(index_name);
  op->index_attrs_ = std::move(index_attrs);
  return Operator(std::move(op));
}

common::hash_t LogicalCreateIndex::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_type_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(unique_index_));
  for (const auto &attr : index_attrs_) {
    hash = common::HashUtil::CombineHashes(hash, attr->Hash());
  }
  return hash;
}

bool LogicalCreateIndex::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALCREATEINDEX) return false;
  const LogicalCreateIndex &node = *dynamic_cast<const LogicalCreateIndex *>(&r);
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (index_type_ != node.index_type_) return false;
  if (index_name_ != node.index_name_) return false;
  if (unique_index_ != node.unique_index_) return false;
  if (index_attrs_.size() != node.index_attrs_.size()) return false;
  for (size_t i = 0; i < index_attrs_.size(); i++) {
    if (*(index_attrs_[i]) != *(node.index_attrs_[i])) return false;
  }
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalCreateTable
//===--------------------------------------------------------------------===//

Operator LogicalCreateTable::Make(catalog::namespace_oid_t namespace_oid, std::string table_name, std::vector<common::ManagedPointer<parser::ColumnDefinition>> && columns, std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&foreign_keys) {
  auto op = std::make_unique<LogicalCreateTable>();
  op->namespace_oid_ = namespace_oid;
  op->table_name_ = std::move(table_name);
  op->columns_ = std::move(columns);
  op->foreign_keys_ = std::move(foreign_keys);
  return Operator(std::move(op));
}

common::hash_t LogicalCreateTable::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
  for (const auto &col : columns_)
    hash = common::HashUtil::CombineHashes(hash, col->Hash());
  for (const auto &fk : foreign_keys_)
    hash = common::HashUtil::CombineHashes(hash, fk->Hash());
  return hash;
}

bool LogicalCreateTable::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALCREATETABLE) return false;
  const LogicalCreateTable &node = *dynamic_cast<const LogicalCreateTable *>(&r);
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_name_ != node.table_name_) return false;
  if (columns_.size() != node.columns_.size()) return false;
  for (size_t i = 0; i < columns_.size(); i++) {
    if (*(columns_[i]) != *(node.columns_[i])) return false;
  }
  if (foreign_keys_.size() != node.foreign_keys_.size()) return false;
  for (size_t i = 0; i < foreign_keys_.size(); i++) {
    if (*(foreign_keys_[i]) != *(node.foreign_keys_[i])) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LogicalCreateNamespace
//===--------------------------------------------------------------------===//

Operator LogicalCreateNamespace::Make(std::string namespace_name) {
  auto op = std::make_unique<LogicalCreateNamespace>();
  op->namespace_name_ = std::move(namespace_name);
  return Operator(std::move(op));
}

common::hash_t LogicalCreateNamespace::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
  return hash;
}

bool LogicalCreateNamespace::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALCREATENAMESPACE) return false;
  const LogicalCreateNamespace &node = *dynamic_cast<const LogicalCreateNamespace *>(&r);
  return node.namespace_name_ == namespace_name_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateTrigger
//===--------------------------------------------------------------------===//

Operator LogicalCreateTrigger::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                    catalog::table_oid_t table_oid, std::string trigger_name, std::vector<std::string> &&trigger_funcnames, std::vector<std::string> &&trigger_args, std::vector<catalog::col_oid_t> &&trigger_columns, common::ManagedPointer<parser::AbstractExpression> &&trigger_when, int16_t trigger_type) {
  auto op = std::make_unique<LogicalCreateTrigger>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->trigger_name_ = std::move(trigger_name);
  op->trigger_funcnames_ = std::move(trigger_funcnames);
  op->trigger_args_ = std::move(trigger_args);
  op->trigger_columns_ = std::move(trigger_columns);
  op->trigger_when_ = trigger_when;
  op->trigger_type_ = trigger_type;
  return Operator(std::move(op));
}

common::hash_t LogicalCreateTrigger::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_type_));
  hash = common::HashUtil::CombineHashes(hash, trigger_when_->Hash());
  hash = common::HashUtil::CombineHashInRange(hash, trigger_funcnames_.begin(), trigger_funcnames_.end());
  hash = common::HashUtil::CombineHashInRange(hash, trigger_args_.begin(), trigger_args_.end());
  hash = common::HashUtil::CombineHashInRange(hash, trigger_columns_.begin(), trigger_columns_.end());
  return hash;
}

bool LogicalCreateTrigger::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALCREATETRIGGER) return false;
  const LogicalCreateTrigger &node = *dynamic_cast<const LogicalCreateTrigger *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (trigger_name_ != node.trigger_name_) return false;
  if (trigger_funcnames_ != node.trigger_funcnames_) return false;
  if (trigger_args_ != node.trigger_args_) return false;
  if (trigger_columns_ != node.trigger_columns_) return false;
  if (trigger_type_ != node.trigger_type_) return false;
  if (trigger_when_ == nullptr) return node.trigger_when_ == nullptr;
  else return *trigger_when_ == *(node.trigger_when_);
}

//===--------------------------------------------------------------------===//
// LogicalCreateView
//===--------------------------------------------------------------------===//

Operator LogicalCreateView::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid)
  auto *op = new LogicalCreateView;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  return Operator(op);
}

common::hash_t LogicalCreateView::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  return hash;
}

bool LogicalCreateView::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALDELETE) return false;
  const LogicalCreateView &node = *dynamic_cast<const LogicalCreateView *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  return (true);
}


//===--------------------------------------------------------------------===//
template <typename T>
void OperatorNode<T>::Accept(common::ManagedPointer<OperatorVisitor> v) const {
  v->Visit(reinterpret_cast<const T *>(this));
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNode<LogicalGet>::name = "LogicalGet";
template <>
const char *OperatorNode<LogicalExternalFileGet>::name = "LogicalExternalFileGet";
template <>
const char *OperatorNode<LogicalQueryDerivedGet>::name = "LogicalQueryDerivedGet";
template <>
const char *OperatorNode<LogicalFilter>::name = "LogicalFilter";
template <>
const char *OperatorNode<LogicalProjection>::name = "LogicalProjection";
template <>
const char *OperatorNode<LogicalMarkJoin>::name = "LogicalMarkJoin";
template <>
const char *OperatorNode<LogicalSingleJoin>::name = "LogicalSingleJoin";
template <>
const char *OperatorNode<LogicalDependentJoin>::name = "LogicalDependentJoin";
template <>
const char *OperatorNode<LogicalInnerJoin>::name = "LogicalInnerJoin";
template <>
const char *OperatorNode<LogicalLeftJoin>::name = "LogicalLeftJoin";
template <>
const char *OperatorNode<LogicalRightJoin>::name = "LogicalRightJoin";
template <>
const char *OperatorNode<LogicalOuterJoin>::name = "LogicalOuterJoin";
template <>
const char *OperatorNode<LogicalSemiJoin>::name = "LogicalSemiJoin";
template <>
const char *OperatorNode<LogicalAggregateAndGroupBy>::name = "LogicalAggregateAndGroupBy";
template <>
const char *OperatorNode<LogicalInsert>::name = "LogicalInsert";
template <>
const char *OperatorNode<LogicalInsertSelect>::name = "LogicalInsertSelect";
template <>
const char *OperatorNode<LogicalUpdate>::name = "LogicalUpdate";
template <>
const char *OperatorNode<LogicalDelete>::name = "LogicalDelete";
template <>
const char *OperatorNode<LogicalLimit>::name = "LogicalLimit";
template <>
const char *OperatorNode<LogicalDistinct>::name = "LogicalDistinct";
template <>
const char *OperatorNode<LogicalExportExternalFile>::name = "LogicalExportExternalFile";
template <>
const char *OperatorNode<LogicalCreateDatabase>::name = "LogicalCreateDatabase";
template <>
const char *OperatorNode<LogicalCreateTable>::name = "LogicalCreateTable";
template <>
const char *OperatorNode<LogicalCreateIndex>::name = "LogicalCreateIndex";
template <>
const char *OperatorNode<LogicalCreateFunction>::name = "LogicalCreateFunction";
template <>
const char *OperatorNode<LogicalCreateNamespace>::name = "LogicalCreateNamespace";
template <>
const char *OperatorNode<LogicalCreateTrigger>::name = "LogicalCreateTrigger";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<LogicalGet>::type = OpType::LOGICALGET;
template <>
OpType OperatorNode<LogicalExternalFileGet>::type = OpType::LOGICALEXTERNALFILEGET;
template <>
OpType OperatorNode<LogicalQueryDerivedGet>::type = OpType::LOGICALQUERYDERIVEDGET;
template <>
OpType OperatorNode<LogicalFilter>::type = OpType::LOGICALFILTER;
template <>
OpType OperatorNode<LogicalProjection>::type = OpType::LOGICALPROJECTION;
template <>
OpType OperatorNode<LogicalMarkJoin>::type = OpType::LOGICALMARKJOIN;
template <>
OpType OperatorNode<LogicalSingleJoin>::type = OpType::LOGICALSINGLEJOIN;
template <>
OpType OperatorNode<LogicalDependentJoin>::type = OpType::LOGICALDEPENDENTJOIN;
template <>
OpType OperatorNode<LogicalInnerJoin>::type = OpType::LOGICALINNERJOIN;
template <>
OpType OperatorNode<LogicalLeftJoin>::type = OpType::LOGICALLEFTJOIN;
template <>
OpType OperatorNode<LogicalRightJoin>::type = OpType::LOGICALRIGHTJOIN;
template <>
OpType OperatorNode<LogicalOuterJoin>::type = OpType::LOGICALOUTERJOIN;
template <>
OpType OperatorNode<LogicalSemiJoin>::type = OpType::LOGICALSEMIJOIN;
template <>
OpType OperatorNode<LogicalAggregateAndGroupBy>::type = OpType::LOGICALAGGREGATEANDGROUPBY;
template <>
OpType OperatorNode<LogicalInsert>::type = OpType::LOGICALINSERT;
template <>
OpType OperatorNode<LogicalInsertSelect>::type = OpType::LOGICALINSERTSELECT;
template <>
OpType OperatorNode<LogicalUpdate>::type = OpType::LOGICALUPDATE;
template <>
OpType OperatorNode<LogicalDelete>::type = OpType::LOGICALDELETE;
template <>
OpType OperatorNode<LogicalDistinct>::type = OpType::LOGICALDISTINCT;
template <>
OpType OperatorNode<LogicalLimit>::type = OpType::LOGICALLIMIT;
template <>
OpType OperatorNode<LogicalExportExternalFile>::type = OpType::LOGICALEXPORTEXTERNALFILE;
template <>
OpType OperatorNode<LogicalCreateDatabase>::type = OpType::LOGICALCREATEDATABASE;
template <>
OpType OperatorNode<LogicalCreateTable>::type = OpType::LOGICALCREATETABLE;
template <>
OpType OperatorNode<LogicalCreateIndex>::type = OpType::LOGICALCREATEINDEX;
template <>
OpType OperatorNode<LogicalCreateFunction>::type = OpType::LOGICALCREATEFUNCTION;
template <>
OpType OperatorNode<LogicalCreateNamespace>::type = OpType::LOGICALCREATENAMESPACE;
template <>
OpType OperatorNode<LogicalCreateTrigger>::type = OpType::LOGICALCREATETRIGGER;

template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return type < OpType::LOGICALPHYSICALDELIMITER;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return type > OpType::LOGICALPHYSICALDELIMITER;
}

}  // namespace terrier::optimizer
