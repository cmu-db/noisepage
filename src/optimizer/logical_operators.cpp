#include "optimizer/logical_operators.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "optimizer/operator_visitor.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::optimizer {
//===--------------------------------------------------------------------===//
// Logical Get
//===--------------------------------------------------------------------===//
Operator LogicalGet::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                          catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> predicates,
                          std::string table_alias, bool is_for_update) {
  auto *get = new LogicalGet;
  get->database_oid_ = database_oid;
  get->namespace_oid_ = namespace_oid;
  get->table_oid_ = table_oid;
  get->predicates_ = std::move(predicates);
  get->table_alias_ = std::move(table_alias);
  get->is_for_update_ = is_for_update;
  return Operator(get);
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
  auto *get = new LogicalExternalFileGet();
  get->format_ = format;
  get->file_name_ = std::move(file_name);
  get->delimiter_ = delimiter;
  get->quote_ = quote;
  get->escape_ = escape;
  return Operator(get);
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
  auto *get = new LogicalQueryDerivedGet;
  get->table_alias_ = std::move(table_alias);
  get->alias_to_expr_map_ = std::move(alias_to_expr_map);
  return Operator(get);
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
  auto *op = new LogicalFilter;
  op->predicates_ = std::move(predicates);
  return Operator(op);
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
  auto *op = new LogicalProjection;
  op->expressions_ = std::move(expressions);
  return Operator(op);
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

Operator LogicalInsert::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&columns,
                             std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &&values) {
#ifndef NDEBUG
  // We need to check whether the number of values for each insert vector
  // matches the number of columns
  for (const auto &insert_vals : values) {
    TERRIER_ASSERT(columns.size() == insert_vals.size(), "Mismatched number of columns and values");
  }
#endif

  auto *op = new LogicalInsert;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  op->values_ = std::move(values);
  return Operator(op);
}

common::hash_t LogicalInsert::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());

  // Perform a deep hash of the values
  for (const auto &insert_vals : values_) {
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
  auto *op = new LogicalInsertSelect;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  return Operator(op);
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
  auto *op = new LogicalDistinct;
  // We don't have anything that we need to store, so we're just going
  // to throw this mofo out to the world as is...
  return Operator(op);
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
                            std::vector<planner::OrderByOrderingType> &&sort_directions) {
  TERRIER_ASSERT(sort_exprs.size() == sort_directions.size(), "Mismatched ORDER BY expressions + directions");
  auto *op = new LogicalLimit;
  op->offset_ = offset;
  op->limit_ = limit;
  op->sort_exprs_ = sort_exprs;
  op->sort_directions_ = std::move(sort_directions);
  return Operator(op);
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
  auto *op = new LogicalDelete;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  return Operator(op);
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
  auto *op = new LogicalUpdate;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->updates_ = updates;
  return Operator(op);
}

common::hash_t LogicalUpdate::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, updates_.begin(), updates_.end());
  return hash;
}

bool LogicalUpdate::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALUPDATE) return false;
  const LogicalUpdate &node = *dynamic_cast<const LogicalUpdate *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (updates_ != node.updates_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalExportExternalFile
//===--------------------------------------------------------------------===//

Operator LogicalExportExternalFile::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
                                         char quote, char escape) {
  auto *op = new LogicalExportExternalFile;
  op->format_ = format;
  op->file_name_ = std::move(file_name);
  op->delimiter_ = delimiter;
  op->quote_ = quote;
  op->escape_ = escape;
  return Operator(op);
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
Operator LogicalDependentJoin::Make() {
  auto *join = new LogicalDependentJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalDependentJoin::Make(std::vector<AnnotatedExpression> &&conditions) {
  auto *join = new LogicalDependentJoin;
  join->join_predicates_ = std::move(conditions);
  return Operator(join);
}

bool LogicalDependentJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALDEPENDENTJOIN) return false;
  const LogicalDependentJoin &node = *static_cast<const LogicalDependentJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

common::hash_t LogicalDependentJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
Operator LogicalMarkJoin::Make() {
  auto *join = new LogicalMarkJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalMarkJoin::Make(std::vector<AnnotatedExpression> &&conditions) {
  auto *join = new LogicalMarkJoin;
  join->join_predicates_ = std::move(conditions);
  return Operator(join);
}

common::hash_t LogicalMarkJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
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
Operator LogicalSingleJoin::Make() {
  auto *join = new LogicalSingleJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalSingleJoin::Make(std::vector<AnnotatedExpression> &&conditions) {
  auto *join = new LogicalSingleJoin;
  join->join_predicates_ = std::move(conditions);
  return Operator(join);
}

common::hash_t LogicalSingleJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
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
Operator LogicalInnerJoin::Make() {
  auto *join = new LogicalInnerJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalInnerJoin::Make(std::vector<AnnotatedExpression> &&conditions) {
  auto *join = new LogicalInnerJoin;
  join->join_predicates_ = std::move(conditions);
  return Operator(join);
}

common::hash_t LogicalInnerJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
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
Operator LogicalLeftJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LogicalLeftJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t LogicalLeftJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LogicalLeftJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALLEFTJOIN) return false;
  const LogicalLeftJoin &node = *static_cast<const LogicalLeftJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
Operator LogicalRightJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LogicalRightJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t LogicalRightJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LogicalRightJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALRIGHTJOIN) return false;
  const LogicalRightJoin &node = *static_cast<const LogicalRightJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalOuterJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LogicalOuterJoin;
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t LogicalOuterJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LogicalOuterJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALOUTERJOIN) return false;
  const LogicalOuterJoin &node = *static_cast<const LogicalOuterJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
Operator LogicalSemiJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LogicalSemiJoin;
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t LogicalSemiJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LogicalSemiJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LOGICALSEMIJOIN) return false;
  const LogicalSemiJoin &node = *static_cast<const LogicalSemiJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
Operator LogicalAggregateAndGroupBy::Make() {
  auto *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns_ = {};
  group_by->having_ = {};
  return Operator(group_by);
}

Operator LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns) {
  auto *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns_ = std::move(columns);
  group_by->having_ = {};
  return Operator(group_by);
}

Operator LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                                          std::vector<AnnotatedExpression> &&having) {
  auto *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns_ = std::move(columns);
  group_by->having_ = std::move(having);
  return Operator(group_by);
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
void OperatorNode<T>::Accept(OperatorVisitor *v) const {
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

template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return type < OpType::LOGICALPHYSICALDELIMITER;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return type > OpType::LOGICALPHYSICALDELIMITER;
}

}  // namespace terrier::optimizer
