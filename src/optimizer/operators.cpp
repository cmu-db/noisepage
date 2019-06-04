#include "optimizer/operators.h"
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
Operator LogicalGet::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
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

Operator LogicalExternalFileGet::make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
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
Operator LogicalQueryDerivedGet::make(
    std::string table_alias,
    std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> alias_to_expr_map) {
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

Operator LogicalFilter::make(std::vector<AnnotatedExpression> &&predicates) {
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

Operator LogicalProjection::make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&expressions) {
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

Operator LogicalInsert::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
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

Operator LogicalInsertSelect::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
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

Operator LogicalDistinct::make() {
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

Operator LogicalLimit::make(size_t offset, size_t limit,
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

Operator LogicalDelete::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
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

Operator LogicalUpdate::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
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

Operator LogicalExportExternalFile::make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
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
Operator LogicalDependentJoin::make() {
  auto *join = new LogicalDependentJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalDependentJoin::make(std::vector<AnnotatedExpression> &&conditions) {
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
Operator LogicalMarkJoin::make() {
  auto *join = new LogicalMarkJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalMarkJoin::make(std::vector<AnnotatedExpression> &&conditions) {
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
Operator LogicalSingleJoin::make() {
  auto *join = new LogicalSingleJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalSingleJoin::make(std::vector<AnnotatedExpression> &&conditions) {
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
Operator LogicalInnerJoin::make() {
  auto *join = new LogicalInnerJoin;
  join->join_predicates_ = {};
  return Operator(join);
}

Operator LogicalInnerJoin::make(std::vector<AnnotatedExpression> &&conditions) {
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
Operator LogicalLeftJoin::make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
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
Operator LogicalRightJoin::make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
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
Operator LogicalOuterJoin::make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
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
Operator LogicalSemiJoin::make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
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
Operator LogicalAggregateAndGroupBy::make() {
  auto *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns_ = {};
  group_by->having_ = {};
  return Operator(group_by);
}

Operator LogicalAggregateAndGroupBy::make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns) {
  auto *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns_ = std::move(columns);
  group_by->having_ = {};
  return Operator(group_by);
}

Operator LogicalAggregateAndGroupBy::make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
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
// TableFreeScan
//===--------------------------------------------------------------------===//
Operator TableFreeScan::make() {
  auto *table_free_scan = new TableFreeScan;
  return Operator(table_free_scan);
}

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
Operator SeqScan::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::string table_alias,
                       std::vector<AnnotatedExpression> predicates, bool is_for_update) {
  auto *scan = new SeqScan;
  scan->database_oid_ = database_oid;
  scan->namespace_oid_ = namespace_oid;
  scan->table_oid_ = table_oid;
  scan->predicates_ = std::move(predicates);
  scan->is_for_update_ = is_for_update;
  scan->table_alias_ = std::move(table_alias);
  return Operator(scan);
}

bool SeqScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::SEQSCAN) return false;

  const SeqScan &node = *dynamic_cast<const SeqScan *>(&r);

  if (predicates_.size() != node.predicates_.size()) return false;

  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) return false;
  }

  return true;
}

common::hash_t SeqScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : predicates_) hash = common::HashUtil::CombineHashes(hash, pred.GetExpr()->Hash());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
Operator IndexScan::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                         catalog::index_oid_t index_oid, std::string table_alias,
                         std::vector<AnnotatedExpression> predicates, bool is_for_update,
                         std::vector<catalog::col_oid_t> key_column_oid_list,
                         std::vector<parser::ExpressionType> expr_type_list,
                         std::vector<type::TransientValue> value_list) {
  auto *scan = new IndexScan;
  scan->database_oid_ = database_oid;
  scan->namespace_oid_ = namespace_oid;
  scan->index_oid_ = index_oid;
  scan->table_alias_ = std::move(table_alias);
  scan->is_for_update_ = is_for_update;
  scan->predicates_ = std::move(predicates);
  scan->key_column_oid_list_ = std::move(key_column_oid_list);
  scan->expr_type_list_ = std::move(expr_type_list);
  scan->value_list_ = std::move(value_list);

  return Operator(scan);
}

bool IndexScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INDEXSCAN) return false;
  const IndexScan &node = *dynamic_cast<const IndexScan *>(&r);
  if (database_oid_ != node.database_oid_ || namespace_oid_ != node.namespace_oid_ || index_oid_ != node.index_oid_ ||
      key_column_oid_list_ != node.key_column_oid_list_ || expr_type_list_ != node.expr_type_list_ ||
      predicates_.size() != node.predicates_.size())
    return false;

  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) return false;
  }
  return true;
}

common::hash_t IndexScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&index_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update_));
  for (auto &pred : predicates_) hash = common::HashUtil::CombineHashes(hash, pred.GetExpr()->Hash());
  for (auto &col_oid : key_column_oid_list_)
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&col_oid));
  for (auto &expr_type : expr_type_list_)
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&expr_type));
  for (auto &val : value_list_) hash = common::HashUtil::CombineHashes(hash, val.Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// External file scan
//===--------------------------------------------------------------------===//
Operator ExternalFileScan::make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                                char escape) {
  auto *get = new ExternalFileScan();
  get->format_ = format;
  get->file_name_ = std::move(file_name);
  get->delimiter_ = delimiter;
  get->quote_ = quote;
  get->escape_ = escape;
  return Operator(get);
}

bool ExternalFileScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::EXTERNALFILESCAN) return false;
  const auto &get = *dynamic_cast<const ExternalFileScan *>(&r);
  return (format_ == get.format_ && file_name_ == get.file_name_ && delimiter_ == get.delimiter_ &&
          quote_ == get.quote_ && escape_ == get.escape_);
}

common::hash_t ExternalFileScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&format_));
  hash = common::HashUtil::CombineHashes(
      hash, common::HashUtil::HashBytes(reinterpret_cast<const byte *>(file_name_.data()), file_name_.length()));
  hash = common::HashUtil::CombineHashes(hash,
                                         common::HashUtil::HashBytes(reinterpret_cast<const byte *>(&delimiter_), 1));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::HashBytes(reinterpret_cast<const byte *>(&quote_), 1));
  hash =
      common::HashUtil::CombineHashes(hash, common::HashUtil::HashBytes(reinterpret_cast<const byte *>(&escape_), 1));
  return hash;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
Operator QueryDerivedScan::make(
    std::string table_alias,
    std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> &&alias_to_expr_map) {
  auto *get = new QueryDerivedScan;
  get->table_alias_ = std::move(table_alias);
  get->alias_to_expr_map_ = std::move(alias_to_expr_map);

  return Operator(get);
}

bool QueryDerivedScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::QUERYDERIVEDSCAN) return false;
  const QueryDerivedScan &node = *static_cast<const QueryDerivedScan *>(&r);
  if (table_alias_ != node.table_alias_) return false;
  return alias_to_expr_map_ == node.alias_to_expr_map_;
}

common::hash_t QueryDerivedScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  for (auto &iter : alias_to_expr_map_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(iter.first));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(iter.second));
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// OrderBy
//===--------------------------------------------------------------------===//
Operator OrderBy::make() {
  auto *order_by = new OrderBy;

  return Operator(order_by);
}

//===--------------------------------------------------------------------===//
// PhysicalLimit
//===--------------------------------------------------------------------===//
Operator Limit::make(int64_t offset, int64_t limit, std::vector<parser::AbstractExpression *> sort_columns,
                     std::vector<bool> sort_ascending) {
  auto *limit_op = new Limit;
  limit_op->offset_ = offset;
  limit_op->limit_ = limit;
  limit_op->sort_exprs_ = std::move(sort_columns);
  limit_op->sort_acsending_ = std::move(sort_ascending);
  return Operator(limit_op);
}

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
Operator InnerNLJoin::make(std::vector<AnnotatedExpression> join_predicates,
                           std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                           std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys) {
  auto *join = new InnerNLJoin();
  join->join_predicates_ = std::move(join_predicates);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);

  return Operator(join);
}

common::hash_t InnerNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) hash = common::HashUtil::CombineHashes(hash, pred.GetExpr()->Hash());
  return hash;
}

bool InnerNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INNERNLJOIN) return false;
  const InnerNLJoin &node = *dynamic_cast<const InnerNLJoin *>(&r);
  if (join_predicates_.size() != node.join_predicates_.size() || left_keys_.size() != node.left_keys_.size() ||
      right_keys_.size() != node.right_keys_.size())
    return false;
  for (size_t i = 0; i < left_keys_.size(); i++) {
    if (left_keys_[i] != node.left_keys_[i]) return false;
  }
  for (size_t i = 0; i < right_keys_.size(); i++) {
    if (right_keys_[i] != node.right_keys_[i]) return false;
  }
  for (size_t i = 0; i < join_predicates_.size(); i++) {
    if (join_predicates_[i].GetExpr() != node.join_predicates_[i].GetExpr()) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
Operator LeftNLJoin::make(std::shared_ptr<parser::AbstractExpression> join_predicate) {
  auto *join = new LeftNLJoin();
  join->join_predicate_ = std::move(join_predicate);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
Operator RightNLJoin::make(std::shared_ptr<parser::AbstractExpression> join_predicate) {
  auto *join = new RightNLJoin();
  join->join_predicate_ = std::move(join_predicate);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
Operator OuterNLJoin::make(std::shared_ptr<parser::AbstractExpression> join_predicate) {
  auto *join = new OuterNLJoin();
  join->join_predicate_ = std::move(join_predicate);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
Operator InnerHashJoin::make(std::vector<AnnotatedExpression> join_predicates,
                             std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                             std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys) {
  auto *join = new InnerHashJoin();
  join->join_predicates_ = std::move(join_predicates);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);
  return Operator(join);
}

common::hash_t InnerHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) hash = common::HashUtil::CombineHashes(hash, pred.GetExpr()->Hash());
  return hash;
}

bool InnerHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INNERHASHJOIN) return false;
  const InnerHashJoin &node = *dynamic_cast<const InnerHashJoin *>(&r);
  if (join_predicates_.size() != node.join_predicates_.size() || left_keys_.size() != node.left_keys_.size() ||
      right_keys_.size() != node.right_keys_.size())
    return false;
  for (size_t i = 0; i < left_keys_.size(); i++) {
    if (left_keys_[i] != node.left_keys_[i]) return false;
  }
  for (size_t i = 0; i < right_keys_.size(); i++) {
    if (right_keys_[i] != node.right_keys_[i]) return false;
  }
  for (size_t i = 0; i < join_predicates_.size(); i++) {
    if (join_predicates_[i].GetExpr() != node.join_predicates_[i].GetExpr()) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
Operator LeftHashJoin::make(std::shared_ptr<parser::AbstractExpression> join_predicate) {
  auto *join = new LeftHashJoin();
  join->join_predicate_ = std::move(join_predicate);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
Operator RightHashJoin::make(std::shared_ptr<parser::AbstractExpression> join_predicate) {
  auto *join = new RightHashJoin();
  join->join_predicate_ = std::move(join_predicate);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
Operator OuterHashJoin::make(std::shared_ptr<parser::AbstractExpression> join_predicate) {
  auto *join = new OuterHashJoin();
  join->join_predicate_ = std::move(join_predicate);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
Operator Insert::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid, const std::vector<catalog::col_oid_t> *columns,
                      const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values) {
  auto *insert_op = new Insert;
  insert_op->database_oid_ = database_oid;
  insert_op->namespace_oid = namespace_oid;
  insert_op->table_oid_ = table_oid;
  insert_op->columns_ = columns;
  insert_op->values_ = values;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// InsertSelect
//===--------------------------------------------------------------------===//
Operator InsertSelect::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                            catalog::table_oid_t table_oid) {
  auto *insert_op = new InsertSelect;
  insert_op->database_oid_ = database_oid;
  insert_op->namespace_oid_ = namespace_oid;
  insert_op->table_oid_ = table_oid;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
Operator Delete::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid, std::shared_ptr<parser::AbstractExpression> delete_condition) {
  auto *delete_op = new Delete;
  delete_op->database_oid_ = database_oid;
  delete_op->namespace_oid_ = namespace_oid;
  delete_op->table_oid_ = table_oid;
  delete_op->delete_condition_ = std::move(delete_condition);
  return Operator(delete_op);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
Operator Update::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid,
                      const std::vector<std::unique_ptr<parser::UpdateClause>> *updates) {
  auto *update_op = new Update;
  update_op->database_oid_ = database_oid;
  update_op->namespace_oid = namespace_oid;
  update_op->table_oid_ = table_oid;
  update_op->updates = updates;
  return Operator(update_op);
}

//===--------------------------------------------------------------------===//
// ExportExternalFile
//===--------------------------------------------------------------------===//
Operator ExportExternalFile::make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                                  char escape) {
  auto *export_op = new ExportExternalFile();
  export_op->format_ = format;
  export_op->file_name_ = std::move(file_name);
  export_op->delimiter_ = delimiter;
  export_op->quote_ = quote;
  export_op->escape_ = escape;
  return Operator(export_op);
}

bool ExportExternalFile::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::EXPORTEXTERNALFILE) return false;
  const auto &export_op = *dynamic_cast<const ExportExternalFile *>(&r);
  return (format_ == export_op.format_ && file_name_ == export_op.file_name_ && delimiter_ == export_op.delimiter_ &&
          quote_ == export_op.quote_ && escape_ == export_op.escape_);
}

common::hash_t ExportExternalFile::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&format_));
  hash = common::HashUtil::CombineHashes(
      hash, common::HashUtil::HashBytes(reinterpret_cast<const byte *>(file_name_.data()), file_name_.length()));
  hash = common::HashUtil::CombineHashes(hash,
                                         common::HashUtil::HashBytes(reinterpret_cast<const byte *>(&delimiter_), 1));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::HashBytes(reinterpret_cast<const byte *>(&quote_), 1));
  hash =
      common::HashUtil::CombineHashes(hash, common::HashUtil::HashBytes(reinterpret_cast<const byte *>(&escape_), 1));
  return hash;
}

//===--------------------------------------------------------------------===//
// HashGroupBy
//===--------------------------------------------------------------------===//
Operator HashGroupBy::make(std::vector<std::shared_ptr<parser::AbstractExpression>> columns,
                           std::vector<AnnotatedExpression> having) {
  auto *agg = new HashGroupBy;
  agg->columns_ = std::move(columns);
  agg->having_ = std::move(having);
  return Operator(agg);
}

bool HashGroupBy::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::HASHGROUPBY) return false;
  const HashGroupBy &hash_op = *dynamic_cast<const HashGroupBy *>(&r);
  if (having_.size() != hash_op.having_.size() || columns_.size() != hash_op.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i].GetExpr() != hash_op.having_[i].GetExpr()) return false;
  }

  std::unordered_set<std::shared_ptr<parser::AbstractExpression>> l_set, r_set;
  for (auto &expr : columns_) l_set.emplace(expr);
  for (auto &expr : hash_op.columns_) r_set.emplace(expr);
  return l_set == r_set;
}

common::hash_t HashGroupBy::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having_) hash = common::HashUtil::CombineHashes(hash, pred.GetExpr()->Hash());
  for (auto &expr : columns_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// SortGroupBy
//===--------------------------------------------------------------------===//
Operator SortGroupBy::make(std::vector<std::shared_ptr<parser::AbstractExpression>> columns,
                           std::vector<AnnotatedExpression> having) {
  auto *agg = new SortGroupBy;
  agg->columns_ = std::move(columns);
  agg->having_ = move(having);
  return Operator(agg);
}

bool SortGroupBy::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::SORTGROUPBY) return false;
  const SortGroupBy &sort_op = *dynamic_cast<const SortGroupBy *>(&r);
  if (having_.size() != sort_op.having_.size() || columns_.size() != sort_op.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i].GetExpr() != sort_op.having_[i].GetExpr()) return false;
  }
  std::unordered_set<std::shared_ptr<parser::AbstractExpression>> l_set, r_set;
  for (auto &expr : columns_) l_set.emplace(expr);
  for (auto &expr : sort_op.columns_) r_set.emplace(expr);
  return l_set == r_set;
}

common::hash_t SortGroupBy::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having_) hash = common::HashUtil::CombineHashes(hash, pred.GetExpr()->Hash());
  for (auto &expr : columns_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
Operator Aggregate::make() {
  auto *agg = new Aggregate;
  return Operator(agg);
}

//===--------------------------------------------------------------------===//
// Hash
//===--------------------------------------------------------------------===//
Operator Distinct::make() {
  auto *hash = new Distinct;
  return Operator(hash);
}

//===--------------------------------------------------------------------===//
template <typename T>
void OperatorNode<T>::Accept(OperatorVisitor *v) const {
  v->Visit(reinterpret_cast<const T *>(this));
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNode<LogicalGet>::name_ = "LogicalGet";
template <>
const char *OperatorNode<LogicalExternalFileGet>::name_ = "LogicalExternalFileGet";
template <>
const char *OperatorNode<LogicalQueryDerivedGet>::name_ = "LogicalQueryDerivedGet";
template <>
const char *OperatorNode<LogicalFilter>::name_ = "LogicalFilter";
template <>
const char *OperatorNode<LogicalProjection>::name_ = "LogicalProjection";
template <>
const char *OperatorNode<LogicalMarkJoin>::name_ = "LogicalMarkJoin";
template <>
const char *OperatorNode<LogicalSingleJoin>::name_ = "LogicalSingleJoin";
template <>
const char *OperatorNode<LogicalDependentJoin>::name_ = "LogicalDependentJoin";
template <>
const char *OperatorNode<LogicalInnerJoin>::name_ = "LogicalInnerJoin";
template <>
const char *OperatorNode<LogicalLeftJoin>::name_ = "LogicalLeftJoin";
template <>
const char *OperatorNode<LogicalRightJoin>::name_ = "LogicalRightJoin";
template <>
const char *OperatorNode<LogicalOuterJoin>::name_ = "LogicalOuterJoin";
template <>
const char *OperatorNode<LogicalSemiJoin>::name_ = "LogicalSemiJoin";
template <>
const char *OperatorNode<LogicalAggregateAndGroupBy>::name_ = "LogicalAggregateAndGroupBy";
template <>
const char *OperatorNode<LogicalInsert>::name_ = "LogicalInsert";
template <>
const char *OperatorNode<LogicalInsertSelect>::name_ = "LogicalInsertSelect";
template <>
const char *OperatorNode<LogicalUpdate>::name_ = "LogicalUpdate";
template <>
const char *OperatorNode<LogicalDelete>::name_ = "LogicalDelete";
template <>
const char *OperatorNode<LogicalLimit>::name_ = "LogicalLimit";
template <>
const char *OperatorNode<LogicalDistinct>::name_ = "LogicalDistinct";
template <>
const char *OperatorNode<LogicalExportExternalFile>::name_ = "LogicalExportExternalFile";
template <>
const char *OperatorNode<TableFreeScan>::name_ = "TableFreeScan";
template <>
const char *OperatorNode<SeqScan>::name_ = "SeqScan";
template <>
const char *OperatorNode<IndexScan>::name_ = "IndexScan";
template <>
const char *OperatorNode<ExternalFileScan>::name_ = "ExternalFileScan";
template <>
const char *OperatorNode<QueryDerivedScan>::name_ = "QueryDerivedScan";
template <>
const char *OperatorNode<OrderBy>::name_ = "OrderBy";
template <>
const char *OperatorNode<Limit>::name_ = "Limit";
template <>
const char *OperatorNode<InnerNLJoin>::name_ = "InnerNLJoin";
template <>
const char *OperatorNode<LeftNLJoin>::name_ = "LeftNLJoin";
template <>
const char *OperatorNode<RightNLJoin>::name_ = "RightNLJoin";
template <>
const char *OperatorNode<OuterNLJoin>::name_ = "OuterNLJoin";
template <>
const char *OperatorNode<InnerHashJoin>::name_ = "InnerHashJoin";
template <>
const char *OperatorNode<LeftHashJoin>::name_ = "LeftHashJoin";
template <>
const char *OperatorNode<RightHashJoin>::name_ = "RightHashJoin";
template <>
const char *OperatorNode<OuterHashJoin>::name_ = "OuterHashJoin";
template <>
const char *OperatorNode<Insert>::name_ = "Insert";
template <>
const char *OperatorNode<InsertSelect>::name_ = "InsertSelect";
template <>
const char *OperatorNode<Delete>::name_ = "Delete";
template <>
const char *OperatorNode<Update>::name_ = "Update";
template <>
const char *OperatorNode<HashGroupBy>::name_ = "HashGroupBy";
template <>
const char *OperatorNode<SortGroupBy>::name_ = "SortGroupBy";
template <>
const char *OperatorNode<Distinct>::name_ = "Distinct";
template <>
const char *OperatorNode<Aggregate>::name_ = "Aggregate";
template <>
const char *OperatorNode<ExportExternalFile>::name_ = "ExportExternalFile";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<LogicalGet>::type_ = OpType::LOGICALGET;
template <>
OpType OperatorNode<LogicalExternalFileGet>::type_ = OpType::LOGICALEXTERNALFILEGET;
template <>
OpType OperatorNode<LogicalQueryDerivedGet>::type_ = OpType::LOGICALQUERYDERIVEDGET;
template <>
OpType OperatorNode<LogicalFilter>::type_ = OpType::LOGICALFILTER;
template <>
OpType OperatorNode<LogicalProjection>::type_ = OpType::LOGICALPROJECTION;
template <>
OpType OperatorNode<LogicalMarkJoin>::type_ = OpType::LOGICALMARKJOIN;
template <>
OpType OperatorNode<LogicalSingleJoin>::type_ = OpType::LOGICALSINGLEJOIN;
template <>
OpType OperatorNode<LogicalDependentJoin>::type_ = OpType::LOGICALDEPENDENTJOIN;
template <>
OpType OperatorNode<LogicalInnerJoin>::type_ = OpType::LOGICALINNERJOIN;
template <>
OpType OperatorNode<LogicalLeftJoin>::type_ = OpType::LOGICALLEFTJOIN;
template <>
OpType OperatorNode<LogicalRightJoin>::type_ = OpType::LOGICALRIGHTJOIN;
template <>
OpType OperatorNode<LogicalOuterJoin>::type_ = OpType::LOGICALOUTERJOIN;
template <>
OpType OperatorNode<LogicalSemiJoin>::type_ = OpType::LOGICALSEMIJOIN;
template <>
OpType OperatorNode<LogicalAggregateAndGroupBy>::type_ = OpType::LOGICALAGGREGATEANDGROUPBY;
template <>
OpType OperatorNode<LogicalInsert>::type_ = OpType::LOGICALINSERT;
template <>
OpType OperatorNode<LogicalInsertSelect>::type_ = OpType::LOGICALINSERTSELECT;
template <>
OpType OperatorNode<LogicalUpdate>::type_ = OpType::LOGICALUPDATE;
template <>
OpType OperatorNode<LogicalDelete>::type_ = OpType::LOGICALDELETE;
template <>
OpType OperatorNode<LogicalDistinct>::type_ = OpType::LOGICALDISTINCT;
template <>
OpType OperatorNode<LogicalLimit>::type_ = OpType::LOGICALLIMIT;
template <>
OpType OperatorNode<LogicalExportExternalFile>::type_ = OpType::LOGICALEXPORTEXTERNALFILE;
template <>
OpType OperatorNode<TableFreeScan>::type_ = OpType::TABLEFREESCAN;
template <>
OpType OperatorNode<SeqScan>::type_ = OpType::SEQSCAN;
template <>
OpType OperatorNode<IndexScan>::type_ = OpType::INDEXSCAN;
template <>
OpType OperatorNode<ExternalFileScan>::type_ = OpType::EXTERNALFILESCAN;
template <>
OpType OperatorNode<QueryDerivedScan>::type_ = OpType::QUERYDERIVEDSCAN;
template <>
OpType OperatorNode<OrderBy>::type_ = OpType::ORDERBY;
template <>
OpType OperatorNode<Distinct>::type_ = OpType::DISTINCT;
template <>
OpType OperatorNode<Limit>::type_ = OpType::LIMIT;
template <>
OpType OperatorNode<InnerNLJoin>::type_ = OpType::INNERNLJOIN;
template <>
OpType OperatorNode<LeftNLJoin>::type_ = OpType::LEFTNLJOIN;
template <>
OpType OperatorNode<RightNLJoin>::type_ = OpType::RIGHTNLJOIN;
template <>
OpType OperatorNode<OuterNLJoin>::type_ = OpType::OUTERNLJOIN;
template <>
OpType OperatorNode<InnerHashJoin>::type_ = OpType::INNERHASHJOIN;
template <>
OpType OperatorNode<LeftHashJoin>::type_ = OpType::LEFTHASHJOIN;
template <>
OpType OperatorNode<RightHashJoin>::type_ = OpType::RIGHTHASHJOIN;
template <>
OpType OperatorNode<OuterHashJoin>::type_ = OpType::OUTERHASHJOIN;
template <>
OpType OperatorNode<Insert>::type_ = OpType::INSERT;
template <>
OpType OperatorNode<InsertSelect>::type_ = OpType::INSERTSELECT;
template <>
OpType OperatorNode<Delete>::type_ = OpType::DELETE;
template <>
OpType OperatorNode<Update>::type_ = OpType::UPDATE;
template <>
OpType OperatorNode<HashGroupBy>::type_ = OpType::HASHGROUPBY;
template <>
OpType OperatorNode<SortGroupBy>::type_ = OpType::SORTGROUPBY;
template <>
OpType OperatorNode<Aggregate>::type_ = OpType::AGGREGATE;
template <>
OpType OperatorNode<ExportExternalFile>::type_ = OpType::EXPORTEXTERNALFILE;

template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return type_ < OpType::LOGICALPHYSICALDELIMITER;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return type_ > OpType::LOGICALPHYSICALDELIMITER;
}

}  // namespace terrier::optimizer
