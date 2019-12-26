#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_visitor.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::optimizer {

BaseOperatorNode *LeafOperator::Copy() const { return new LeafOperator(*this); }

Operator LeafOperator::Make(group_id_t group) {
  auto leaf = std::make_unique<LeafOperator>();
  leaf->origin_group_ = group;
  return Operator(std::move(leaf));
}

common::hash_t LeafOperator::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(origin_group_));
  return hash;
}

bool LeafOperator::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LEAF) return false;
  const LeafOperator &node = *dynamic_cast<const LeafOperator *>(&r);
  return origin_group_ == node.origin_group_;
}

//===--------------------------------------------------------------------===//
// Logical Get
//===--------------------------------------------------------------------===//
BaseOperatorNode *LogicalGet::Copy() const { return new LogicalGet(*this); }

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
BaseOperatorNode *LogicalExternalFileGet::Copy() const { return new LogicalExternalFileGet(*this); }

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
BaseOperatorNode *LogicalQueryDerivedGet::Copy() const { return new LogicalQueryDerivedGet(*this); }

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
BaseOperatorNode *LogicalFilter::Copy() const { return new LogicalFilter(*this); }

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
BaseOperatorNode *LogicalProjection::Copy() const { return new LogicalProjection(*this); }

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
BaseOperatorNode *LogicalInsert::Copy() const { return new LogicalInsert(*this); }

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
BaseOperatorNode *LogicalInsertSelect::Copy() const { return new LogicalInsertSelect(*this); }

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
// LogicalLimit
//===--------------------------------------------------------------------===//
BaseOperatorNode *LogicalLimit::Copy() const { return new LogicalLimit(*this); }

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
BaseOperatorNode *LogicalDelete::Copy() const { return new LogicalDelete(*this); }

Operator LogicalDelete::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             std::string table_alias, catalog::table_oid_t table_oid) {
  auto op = std::make_unique<LogicalDelete>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_alias_ = std::move(table_alias);
  op->table_oid_ = table_oid;
  return Operator(std::move(op));
}

common::hash_t LogicalDelete::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
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
BaseOperatorNode *LogicalUpdate::Copy() const { return new LogicalUpdate(*this); }

Operator LogicalUpdate::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             std::string table_alias, catalog::table_oid_t table_oid,
                             std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates) {
  auto op = std::make_unique<LogicalUpdate>();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_alias_ = std::move(table_alias);
  op->table_oid_ = table_oid;
  op->updates_ = std::move(updates);
  return Operator(std::move(op));
}

common::hash_t LogicalUpdate::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
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
  return table_alias_ == node.table_alias_;
}

//===--------------------------------------------------------------------===//
// LogicalExportExternalFile
//===--------------------------------------------------------------------===//
BaseOperatorNode *LogicalExportExternalFile::Copy() const { return new LogicalExportExternalFile(*this); }

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
BaseOperatorNode *LogicalDependentJoin::Copy() const { return new LogicalDependentJoin(*this); }

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
BaseOperatorNode *LogicalMarkJoin::Copy() const { return new LogicalMarkJoin(*this); }

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
BaseOperatorNode *LogicalSingleJoin::Copy() const { return new LogicalSingleJoin(*this); }

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
BaseOperatorNode *LogicalInnerJoin::Copy() const { return new LogicalInnerJoin(*this); }

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
BaseOperatorNode *LogicalLeftJoin::Copy() const { return new LogicalLeftJoin(*this); }

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
BaseOperatorNode *LogicalRightJoin::Copy() const { return new LogicalRightJoin(*this); }

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
BaseOperatorNode *LogicalOuterJoin::Copy() const { return new LogicalOuterJoin(*this); }

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
BaseOperatorNode *LogicalSemiJoin::Copy() const { return new LogicalSemiJoin(*this); }

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
BaseOperatorNode *LogicalAggregateAndGroupBy::Copy() const { return new LogicalAggregateAndGroupBy(*this); }

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
template <typename T>
void OperatorNode<T>::Accept(common::ManagedPointer<OperatorVisitor> v) const {
  v->Visit(reinterpret_cast<const T *>(this));
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNode<LeafOperator>::name = "LeafOperator";
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
const char *OperatorNode<LogicalExportExternalFile>::name = "LogicalExportExternalFile";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<LeafOperator>::type = OpType::LEAF;
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
OpType OperatorNode<LogicalLimit>::type = OpType::LOGICALLIMIT;
template <>
OpType OperatorNode<LogicalExportExternalFile>::type = OpType::LOGICALEXPORTEXTERNALFILE;

template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return type < OpType::LOGICALPHYSICALDELIMITER;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return type > OpType::LOGICALPHYSICALDELIMITER;
}

}  // namespace terrier::optimizer
