#include "optimizer/logical_operators.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "optimizer/operator_visitor.h"
#include "parser/expression/abstract_expression.h"

namespace noisepage::optimizer {

BaseOperatorNodeContents *LeafOperator::Copy() const { return new LeafOperator(*this); }

Operator LeafOperator::Make(group_id_t group) {
  auto *leaf = new LeafOperator();
  leaf->origin_group_ = group;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(leaf));
}

common::hash_t LeafOperator::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(origin_group_));
  return hash;
}

bool LeafOperator::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LEAF) return false;
  const LeafOperator &node = *dynamic_cast<const LeafOperator *>(&r);
  return origin_group_ == node.origin_group_;
}

//===--------------------------------------------------------------------===//
// Logical Get
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalGet::Copy() const { return new LogicalGet(*this); }

Operator LogicalGet::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                          std::vector<AnnotatedExpression> predicates, std::string table_alias, bool is_for_update) {
  auto *get = new LogicalGet();
  get->database_oid_ = database_oid;
  get->table_oid_ = table_oid;
  get->predicates_ = std::move(predicates);
  get->table_alias_ = std::move(table_alias);
  get->is_for_update_ = is_for_update;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

Operator LogicalGet::Make() {
  auto get = new LogicalGet();
  get->database_oid_ = catalog::INVALID_DATABASE_OID;
  get->table_oid_ = catalog::INVALID_TABLE_OID;
  get->is_for_update_ = false;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

common::hash_t LogicalGet::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, predicates_.begin(), predicates_.end());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
  return hash;
}

bool LogicalGet::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALGET) return false;
  const LogicalGet &node = *dynamic_cast<const LogicalGet *>(&r);
  if (database_oid_ != node.database_oid_) return false;
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
BaseOperatorNodeContents *LogicalExternalFileGet::Copy() const { return new LogicalExternalFileGet(*this); }

Operator LogicalExternalFileGet::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
                                      char quote, char escape) {
  auto get = new LogicalExternalFileGet();
  get->format_ = format;
  get->file_name_ = std::move(file_name);
  get->delimiter_ = delimiter;
  get->quote_ = quote;
  get->escape_ = escape;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

bool LogicalExternalFileGet::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALEXTERNALFILEGET) return false;
  const auto &get = *static_cast<const LogicalExternalFileGet *>(&r);
  return (format_ == get.format_ && file_name_ == get.file_name_ && delimiter_ == get.delimiter_ &&
          quote_ == get.quote_ && escape_ == get.escape_);
}

common::hash_t LogicalExternalFileGet::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
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
BaseOperatorNodeContents *LogicalQueryDerivedGet::Copy() const { return new LogicalQueryDerivedGet(*this); }

Operator LogicalQueryDerivedGet::Make(
    std::string table_alias,
    std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map) {
  auto *get = new LogicalQueryDerivedGet();
  get->table_alias_ = std::move(table_alias);
  get->alias_to_expr_map_ = std::move(alias_to_expr_map);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

bool LogicalQueryDerivedGet::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALQUERYDERIVEDGET) return false;
  const LogicalQueryDerivedGet &node = *static_cast<const LogicalQueryDerivedGet *>(&r);
  if (table_alias_ != node.table_alias_) return false;
  return alias_to_expr_map_ == node.alias_to_expr_map_;
}

common::hash_t LogicalQueryDerivedGet::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
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
BaseOperatorNodeContents *LogicalFilter::Copy() const { return new LogicalFilter(*this); }

Operator LogicalFilter::Make(std::vector<AnnotatedExpression> &&predicates) {
  auto *op = new LogicalFilter();
  op->predicates_ = std::move(predicates);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

bool LogicalFilter::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALFILTER) return false;
  const LogicalFilter &node = *static_cast<const LogicalFilter *>(&r);

  // This is technically incorrect because the predicates
  // are supposed to be unsorted and this equals check is
  // comparing values at each offset.
  return (predicates_ == node.predicates_);
}

common::hash_t LogicalFilter::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  // Again, I think that this is wrong because the value of the hash is based
  // on the location order of the expressions.
  for (auto &pred : predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalProjection
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalProjection::Copy() const { return new LogicalProjection(*this); }

Operator LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&expressions) {
  auto *op = new LogicalProjection();
  op->expressions_ = std::move(expressions);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

bool LogicalProjection::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALPROJECTION) return false;
  const LogicalProjection &node = *static_cast<const LogicalProjection *>(&r);
  for (size_t i = 0; i < expressions_.size(); i++) {
    if (*(expressions_[i]) != *(node.expressions_[i])) return false;
  }
  return true;
}

common::hash_t LogicalProjection::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &expr : expressions_) hash = common::HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalInsert
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalInsert::Copy() const { return new LogicalInsert(*this); }

Operator LogicalInsert::Make(
    catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&columns,
    common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>> values) {
#ifndef NDEBUG
  // We need to check whether the number of values for each insert vector
  // matches the number of columns
  for (const auto &insert_vals : *values) {
    NOISEPAGE_ASSERT(columns.size() == insert_vals.size(), "Mismatched number of columns and values");
  }
#endif

  auto *op = new LogicalInsert();
  op->database_oid_ = database_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  op->values_ = values;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalInsert::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());

  // Perform a deep hash of the values
  for (const auto &insert_vals : *values_) {
    hash = common::HashUtil::CombineHashInRange(hash, insert_vals.begin(), insert_vals.end());
  }

  return hash;
}

bool LogicalInsert::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALINSERT) return false;
  const LogicalInsert &node = *dynamic_cast<const LogicalInsert *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (columns_ != node.columns_) return false;
  if (values_ != node.values_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalInsertSelect
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalInsertSelect::Copy() const { return new LogicalInsertSelect(*this); }

Operator LogicalInsertSelect::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid) {
  auto *op = new LogicalInsertSelect();
  op->database_oid_ = database_oid;
  op->table_oid_ = table_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalInsertSelect::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool LogicalInsertSelect::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALINSERTSELECT) return false;
  const LogicalInsertSelect &node = *dynamic_cast<const LogicalInsertSelect *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalLimit
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalLimit::Copy() const { return new LogicalLimit(*this); }

Operator LogicalLimit::Make(size_t offset, size_t limit,
                            std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_exprs,
                            std::vector<optimizer::OrderByOrderingType> &&sort_directions) {
  NOISEPAGE_ASSERT(sort_exprs.size() == sort_directions.size(), "Mismatched ORDER BY expressions + directions");
  auto *op = new LogicalLimit();
  op->offset_ = offset;
  op->limit_ = limit;
  op->sort_exprs_ = sort_exprs;
  op->sort_directions_ = std::move(sort_directions);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

bool LogicalLimit::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALLIMIT) return false;
  const LogicalLimit &node = *static_cast<const LogicalLimit *>(&r);
  if (offset_ != node.offset_) return false;
  if (limit_ != node.limit_) return false;
  if (sort_exprs_ != node.sort_exprs_) return false;
  if (sort_directions_ != node.sort_directions_) return false;
  return (true);
}

common::hash_t LogicalLimit::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));
  hash = common::HashUtil::CombineHashInRange(hash, sort_exprs_.begin(), sort_exprs_.end());
  hash = common::HashUtil::CombineHashInRange(hash, sort_directions_.begin(), sort_directions_.end());
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalDelete
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDelete::Copy() const { return new LogicalDelete(*this); }

Operator LogicalDelete::Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid) {
  auto *op = new LogicalDelete();
  op->database_oid_ = database_oid;
  op->table_alias_ = std::move(table_alias);
  op->table_oid_ = table_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDelete::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool LogicalDelete::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDELETE) return false;
  const LogicalDelete &node = *dynamic_cast<const LogicalDelete *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// LogicalUpdate
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalUpdate::Copy() const { return new LogicalUpdate(*this); }

Operator LogicalUpdate::Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid,
                             std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates) {
  auto *op = new LogicalUpdate();
  op->database_oid_ = database_oid;
  op->table_alias_ = std::move(table_alias);
  op->table_oid_ = table_oid;
  op->updates_ = std::move(updates);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalUpdate::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  for (const auto &clause : updates_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(clause->GetUpdateValue()));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(clause->GetColumnName()));
  }
  return hash;
}

bool LogicalUpdate::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALUPDATE) return false;
  const LogicalUpdate &node = *dynamic_cast<const LogicalUpdate *>(&r);
  if (database_oid_ != node.database_oid_) return false;
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
BaseOperatorNodeContents *LogicalExportExternalFile::Copy() const { return new LogicalExportExternalFile(*this); }

Operator LogicalExportExternalFile::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter,
                                         char quote, char escape) {
  auto *op = new LogicalExportExternalFile();
  op->format_ = format;
  op->file_name_ = std::move(file_name);
  op->delimiter_ = delimiter;
  op->quote_ = quote;
  op->escape_ = escape;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalExportExternalFile::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
  return hash;
}

bool LogicalExportExternalFile::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALEXPORTEXTERNALFILE) return false;
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
BaseOperatorNodeContents *LogicalDependentJoin::Copy() const { return new LogicalDependentJoin(*this); }

Operator LogicalDependentJoin::Make() {
  auto *join = new LogicalDependentJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalDependentJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalDependentJoin();
  join->join_predicates_ = std::move(join_predicates);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalDependentJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalDependentJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDEPENDENTJOIN) return false;
  const LogicalDependentJoin &node = *static_cast<const LogicalDependentJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalMarkJoin::Copy() const { return new LogicalMarkJoin(*this); }

Operator LogicalMarkJoin::Make() {
  auto *join = new LogicalMarkJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalMarkJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalMarkJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalMarkJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalMarkJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALMARKJOIN) return false;
  const LogicalMarkJoin &node = *static_cast<const LogicalMarkJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalSingleJoin::Copy() const { return new LogicalSingleJoin(*this); }

Operator LogicalSingleJoin::Make() {
  auto *join = new LogicalSingleJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalSingleJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalSingleJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalSingleJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalSingleJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALSINGLEJOIN) return false;
  const LogicalSingleJoin &node = *static_cast<const LogicalSingleJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalInnerJoin::Copy() const { return new LogicalInnerJoin(*this); }

Operator LogicalInnerJoin::Make() {
  auto *join = new LogicalInnerJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalInnerJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalInnerJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalInnerJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalInnerJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALINNERJOIN) return false;
  const LogicalInnerJoin &node = *static_cast<const LogicalInnerJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalLeftJoin::Copy() const { return new LogicalLeftJoin(*this); }

Operator LogicalLeftJoin::Make() {
  auto *join = new LogicalLeftJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalLeftJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalLeftJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalLeftJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalLeftJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALLEFTJOIN) return false;
  const LogicalLeftJoin &node = *static_cast<const LogicalLeftJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalRightJoin::Copy() const { return new LogicalRightJoin(*this); }

Operator LogicalRightJoin::Make() {
  auto *join = new LogicalRightJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalRightJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalRightJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalRightJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalRightJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALRIGHTJOIN) return false;
  const LogicalRightJoin &node = *static_cast<const LogicalRightJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalOuterJoin::Copy() const { return new LogicalOuterJoin(*this); }

Operator LogicalOuterJoin::Make() {
  auto *join = new LogicalOuterJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalOuterJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalOuterJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalOuterJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalOuterJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALOUTERJOIN) return false;
  const LogicalOuterJoin &node = *static_cast<const LogicalOuterJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalSemiJoin::Copy() const { return new LogicalSemiJoin(*this); }

Operator LogicalSemiJoin::Make() {
  auto *join = new LogicalSemiJoin();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

Operator LogicalSemiJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new LogicalSemiJoin();
  join->join_predicates_ = join_predicates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LogicalSemiJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr) {
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    } else {
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
    }
  }
  return hash;
}

bool LogicalSemiJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALSEMIJOIN) return false;
  const LogicalSemiJoin &node = *static_cast<const LogicalSemiJoin *>(&r);
  return (join_predicates_ == node.join_predicates_);
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalAggregateAndGroupBy::Copy() const { return new LogicalAggregateAndGroupBy(*this); }

Operator LogicalAggregateAndGroupBy::Make() {
  auto *group_by = new LogicalAggregateAndGroupBy();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(group_by));
}

Operator LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns) {
  auto *group_by = new LogicalAggregateAndGroupBy();
  group_by->columns_ = std::move(columns);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(group_by));
}

Operator LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                                          std::vector<AnnotatedExpression> &&having) {
  auto *group_by = new LogicalAggregateAndGroupBy();
  group_by->columns_ = std::move(columns);
  group_by->having_ = std::move(having);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(group_by));
}
bool LogicalAggregateAndGroupBy::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALAGGREGATEANDGROUPBY) return false;
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
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : having_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  for (auto &expr : columns_) hash = common::HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// LogicalCreateDatabase
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalCreateDatabase::Copy() const { return new LogicalCreateDatabase(*this); }

Operator LogicalCreateDatabase::Make(std::string database_name) {
  auto *op = new LogicalCreateDatabase();
  op->database_name_ = std::move(database_name);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateDatabase::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));
  return hash;
}

bool LogicalCreateDatabase::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATEDATABASE) return false;
  const LogicalCreateDatabase &node = *dynamic_cast<const LogicalCreateDatabase *>(&r);
  return node.database_name_ == database_name_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateFunction
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalCreateFunction::Copy() const { return new LogicalCreateFunction(*this); }

Operator LogicalCreateFunction::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                     std::string function_name, parser::PLType language,
                                     std::vector<std::string> &&function_body,
                                     std::vector<std::string> &&function_param_names,
                                     std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types,
                                     parser::BaseFunctionParameter::DataType return_type, size_t param_count,
                                     bool replace) {
  auto *op = new LogicalCreateFunction();
  NOISEPAGE_ASSERT(function_param_names.size() == param_count && function_param_types.size() == param_count,
                   "Mismatched number of items in vector and number of function parameters");
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->function_name_ = std::move(function_name);
  op->function_body_ = std::move(function_body);
  op->function_param_names_ = std::move(function_param_names);
  op->function_param_types_ = std::move(function_param_types);
  op->is_replace_ = replace;
  op->param_count_ = param_count;
  op->return_type_ = return_type;
  op->language_ = language;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateFunction::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
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

bool LogicalCreateFunction::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATEFUNCTION) return false;
  const LogicalCreateFunction &node = *dynamic_cast<const LogicalCreateFunction *>(&r);
  if (database_oid_ != node.database_oid_) return false;
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
BaseOperatorNodeContents *LogicalCreateIndex::Copy() const { return new LogicalCreateIndex(*this); }

Operator LogicalCreateIndex::Make(catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                                  parser::IndexType index_type, bool unique, std::string index_name,
                                  std::vector<common::ManagedPointer<parser::AbstractExpression>> index_attrs) {
  auto *op = new LogicalCreateIndex();
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->index_type_ = index_type;
  op->unique_index_ = unique;
  op->index_name_ = std::move(index_name);
  op->index_attrs_ = std::move(index_attrs);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateIndex::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
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

bool LogicalCreateIndex::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATEINDEX) return false;
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
BaseOperatorNodeContents *LogicalCreateTable::Copy() const { return new LogicalCreateTable(*this); }

Operator LogicalCreateTable::Make(catalog::namespace_oid_t namespace_oid, std::string table_name,
                                  std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&columns,
                                  std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&foreign_keys) {
  auto *op = new LogicalCreateTable();
  op->namespace_oid_ = namespace_oid;
  op->table_name_ = std::move(table_name);
  op->columns_ = std::move(columns);
  op->foreign_keys_ = std::move(foreign_keys);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateTable::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
  for (const auto &col : columns_) hash = common::HashUtil::CombineHashes(hash, col->Hash());
  for (const auto &fk : foreign_keys_) hash = common::HashUtil::CombineHashes(hash, fk->Hash());
  return hash;
}

bool LogicalCreateTable::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATETABLE) return false;
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
BaseOperatorNodeContents *LogicalCreateNamespace::Copy() const { return new LogicalCreateNamespace(*this); }

Operator LogicalCreateNamespace::Make(std::string namespace_name) {
  auto *op = new LogicalCreateNamespace();
  op->namespace_name_ = std::move(namespace_name);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateNamespace::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
  return hash;
}

bool LogicalCreateNamespace::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATENAMESPACE) return false;
  const LogicalCreateNamespace &node = *dynamic_cast<const LogicalCreateNamespace *>(&r);
  return node.namespace_name_ == namespace_name_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateTrigger
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalCreateTrigger::Copy() const { return new LogicalCreateTrigger(*this); }

Operator LogicalCreateTrigger::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                    catalog::table_oid_t table_oid, std::string trigger_name,
                                    std::vector<std::string> &&trigger_funcnames,
                                    std::vector<std::string> &&trigger_args,
                                    std::vector<catalog::col_oid_t> &&trigger_columns,
                                    common::ManagedPointer<parser::AbstractExpression> &&trigger_when,
                                    int16_t trigger_type) {
  auto *op = new LogicalCreateTrigger();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->trigger_name_ = std::move(trigger_name);
  op->trigger_funcnames_ = std::move(trigger_funcnames);
  op->trigger_args_ = std::move(trigger_args);
  op->trigger_columns_ = std::move(trigger_columns);
  op->trigger_when_ = trigger_when;
  op->trigger_type_ = trigger_type;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateTrigger::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
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

bool LogicalCreateTrigger::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATETRIGGER) return false;
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
  return node.trigger_when_ != nullptr && *trigger_when_ == *node.trigger_when_;
}

//===--------------------------------------------------------------------===//
// LogicalCreateView
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalCreateView::Copy() const { return new LogicalCreateView(*this); }

Operator LogicalCreateView::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                                 std::string view_name, common::ManagedPointer<parser::SelectStatement> view_query) {
  auto *op = new LogicalCreateView();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->view_name_ = std::move(view_name);
  op->view_query_ = view_query;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalCreateView::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));
  if (view_query_ != nullptr) hash = common::HashUtil::CombineHashes(hash, view_query_->Hash());
  return hash;
}

bool LogicalCreateView::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALCREATEVIEW) return false;
  const LogicalCreateView &node = *dynamic_cast<const LogicalCreateView *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (view_name_ != node.view_name_) return false;
  if (view_query_ == nullptr) return node.view_query_ == nullptr;
  return node.view_query_ != nullptr && *view_query_ == *node.view_query_;
}

//===--------------------------------------------------------------------===//
// LogicalDropDatabase
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDropDatabase::Copy() const { return new LogicalDropDatabase(*this); }

Operator LogicalDropDatabase::Make(catalog::db_oid_t db_oid) {
  auto *op = new LogicalDropDatabase();
  op->db_oid_ = db_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDropDatabase::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(db_oid_));
  return hash;
}

bool LogicalDropDatabase::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDROPDATABASE) return false;
  const LogicalDropDatabase &node = *dynamic_cast<const LogicalDropDatabase *>(&r);
  return node.db_oid_ == db_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropTable
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDropTable::Copy() const { return new LogicalDropTable(*this); }

Operator LogicalDropTable::Make(catalog::table_oid_t table_oid) {
  auto *op = new LogicalDropTable();
  op->table_oid_ = table_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDropTable::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool LogicalDropTable::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDROPTABLE) return false;
  const LogicalDropTable &node = *dynamic_cast<const LogicalDropTable *>(&r);
  return node.table_oid_ == table_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropIndex
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDropIndex::Copy() const { return new LogicalDropIndex(*this); }

Operator LogicalDropIndex::Make(catalog::index_oid_t index_oid) {
  auto *op = new LogicalDropIndex();
  op->index_oid_ = index_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDropIndex::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));
  return hash;
}

bool LogicalDropIndex::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDROPINDEX) return false;
  const LogicalDropIndex &node = *dynamic_cast<const LogicalDropIndex *>(&r);
  return node.index_oid_ == index_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropNamespace
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDropNamespace::Copy() const { return new LogicalDropNamespace(*this); }

Operator LogicalDropNamespace::Make(catalog::namespace_oid_t namespace_oid) {
  auto *op = new LogicalDropNamespace();
  op->namespace_oid_ = namespace_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDropNamespace::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  return hash;
}

bool LogicalDropNamespace::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDROPNAMESPACE) return false;
  const LogicalDropNamespace &node = *dynamic_cast<const LogicalDropNamespace *>(&r);
  return node.namespace_oid_ == namespace_oid_;
}

//===--------------------------------------------------------------------===//
// LogicalDropTrigger
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDropTrigger::Copy() const { return new LogicalDropTrigger(*this); }

Operator LogicalDropTrigger::Make(catalog::db_oid_t database_oid, catalog::trigger_oid_t trigger_oid, bool if_exists) {
  auto *op = new LogicalDropTrigger();
  op->database_oid_ = database_oid;
  op->trigger_oid_ = trigger_oid;
  op->if_exists_ = if_exists;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDropTrigger::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
  return hash;
}

bool LogicalDropTrigger::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDROPTRIGGER) return false;
  const LogicalDropTrigger &node = *dynamic_cast<const LogicalDropTrigger *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (trigger_oid_ != node.trigger_oid_) return false;
  return if_exists_ == node.if_exists_;
}

//===--------------------------------------------------------------------===//
// LogicalDropView
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalDropView::Copy() const { return new LogicalDropView(*this); }

Operator LogicalDropView::Make(catalog::db_oid_t database_oid, catalog::view_oid_t view_oid, bool if_exists) {
  auto *op = new LogicalDropView();
  op->database_oid_ = database_oid;
  op->view_oid_ = view_oid;
  op->if_exists_ = if_exists;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalDropView::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
  return hash;
}

bool LogicalDropView::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALDROPVIEW) return false;
  const LogicalDropView &node = *dynamic_cast<const LogicalDropView *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (view_oid_ != node.view_oid_) return false;
  return if_exists_ == node.if_exists_;
}

//===--------------------------------------------------------------------===//
// LogicalAnalyze
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LogicalAnalyze::Copy() const { return new LogicalAnalyze(*this); }

Operator LogicalAnalyze::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                              std::vector<catalog::col_oid_t> &&columns) {
  auto *op = new LogicalAnalyze();
  op->database_oid_ = database_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t LogicalAnalyze::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
  return hash;
}

bool LogicalAnalyze::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LOGICALANALYZE) return false;
  const LogicalAnalyze &node = *dynamic_cast<const LogicalAnalyze *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (columns_ != node.columns_) return false;
  return true;
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNodeContents<LeafOperator>::name = "LeafOperator";
template <>
const char *OperatorNodeContents<LogicalGet>::name = "LogicalGet";
template <>
const char *OperatorNodeContents<LogicalExternalFileGet>::name = "LogicalExternalFileGet";
template <>
const char *OperatorNodeContents<LogicalQueryDerivedGet>::name = "LogicalQueryDerivedGet";
template <>
const char *OperatorNodeContents<LogicalFilter>::name = "LogicalFilter";
template <>
const char *OperatorNodeContents<LogicalProjection>::name = "LogicalProjection";
template <>
const char *OperatorNodeContents<LogicalMarkJoin>::name = "LogicalMarkJoin";
template <>
const char *OperatorNodeContents<LogicalSingleJoin>::name = "LogicalSingleJoin";
template <>
const char *OperatorNodeContents<LogicalDependentJoin>::name = "LogicalDependentJoin";
template <>
const char *OperatorNodeContents<LogicalInnerJoin>::name = "LogicalInnerJoin";
template <>
const char *OperatorNodeContents<LogicalLeftJoin>::name = "LogicalLeftJoin";
template <>
const char *OperatorNodeContents<LogicalRightJoin>::name = "LogicalRightJoin";
template <>
const char *OperatorNodeContents<LogicalOuterJoin>::name = "LogicalOuterJoin";
template <>
const char *OperatorNodeContents<LogicalSemiJoin>::name = "LogicalSemiJoin";
template <>
const char *OperatorNodeContents<LogicalAggregateAndGroupBy>::name = "LogicalAggregateAndGroupBy";
template <>
const char *OperatorNodeContents<LogicalInsert>::name = "LogicalInsert";
template <>
const char *OperatorNodeContents<LogicalInsertSelect>::name = "LogicalInsertSelect";
template <>
const char *OperatorNodeContents<LogicalUpdate>::name = "LogicalUpdate";
template <>
const char *OperatorNodeContents<LogicalDelete>::name = "LogicalDelete";
template <>
const char *OperatorNodeContents<LogicalLimit>::name = "LogicalLimit";
template <>
const char *OperatorNodeContents<LogicalExportExternalFile>::name = "LogicalExportExternalFile";
template <>
const char *OperatorNodeContents<LogicalCreateDatabase>::name = "LogicalCreateDatabase";
template <>
const char *OperatorNodeContents<LogicalCreateTable>::name = "LogicalCreateTable";
template <>
const char *OperatorNodeContents<LogicalCreateIndex>::name = "LogicalCreateIndex";
template <>
const char *OperatorNodeContents<LogicalCreateFunction>::name = "LogicalCreateFunction";
template <>
const char *OperatorNodeContents<LogicalCreateNamespace>::name = "LogicalCreateNamespace";
template <>
const char *OperatorNodeContents<LogicalCreateTrigger>::name = "LogicalCreateTrigger";
template <>
const char *OperatorNodeContents<LogicalCreateView>::name = "LogicalCreateView";
template <>
const char *OperatorNodeContents<LogicalDropDatabase>::name = "LogicalDropDatabase";
template <>
const char *OperatorNodeContents<LogicalDropTable>::name = "LogicalDropTable";
template <>
const char *OperatorNodeContents<LogicalDropIndex>::name = "LogicalDropIndex";
template <>
const char *OperatorNodeContents<LogicalDropNamespace>::name = "LogicalDropNamespace";
template <>
const char *OperatorNodeContents<LogicalDropTrigger>::name = "LogicalDropTrigger";
template <>
const char *OperatorNodeContents<LogicalDropView>::name = "LogicalDropView";
template <>
const char *OperatorNodeContents<LogicalAnalyze>::name = "LogicalAnalyze";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNodeContents<LeafOperator>::type = OpType::LEAF;
template <>
OpType OperatorNodeContents<LogicalGet>::type = OpType::LOGICALGET;
template <>
OpType OperatorNodeContents<LogicalExternalFileGet>::type = OpType::LOGICALEXTERNALFILEGET;
template <>
OpType OperatorNodeContents<LogicalQueryDerivedGet>::type = OpType::LOGICALQUERYDERIVEDGET;
template <>
OpType OperatorNodeContents<LogicalFilter>::type = OpType::LOGICALFILTER;
template <>
OpType OperatorNodeContents<LogicalProjection>::type = OpType::LOGICALPROJECTION;
template <>
OpType OperatorNodeContents<LogicalMarkJoin>::type = OpType::LOGICALMARKJOIN;
template <>
OpType OperatorNodeContents<LogicalSingleJoin>::type = OpType::LOGICALSINGLEJOIN;
template <>
OpType OperatorNodeContents<LogicalDependentJoin>::type = OpType::LOGICALDEPENDENTJOIN;
template <>
OpType OperatorNodeContents<LogicalInnerJoin>::type = OpType::LOGICALINNERJOIN;
template <>
OpType OperatorNodeContents<LogicalLeftJoin>::type = OpType::LOGICALLEFTJOIN;
template <>
OpType OperatorNodeContents<LogicalRightJoin>::type = OpType::LOGICALRIGHTJOIN;
template <>
OpType OperatorNodeContents<LogicalOuterJoin>::type = OpType::LOGICALOUTERJOIN;
template <>
OpType OperatorNodeContents<LogicalSemiJoin>::type = OpType::LOGICALSEMIJOIN;
template <>
OpType OperatorNodeContents<LogicalAggregateAndGroupBy>::type = OpType::LOGICALAGGREGATEANDGROUPBY;
template <>
OpType OperatorNodeContents<LogicalInsert>::type = OpType::LOGICALINSERT;
template <>
OpType OperatorNodeContents<LogicalInsertSelect>::type = OpType::LOGICALINSERTSELECT;
template <>
OpType OperatorNodeContents<LogicalUpdate>::type = OpType::LOGICALUPDATE;
template <>
OpType OperatorNodeContents<LogicalDelete>::type = OpType::LOGICALDELETE;
template <>
OpType OperatorNodeContents<LogicalLimit>::type = OpType::LOGICALLIMIT;
template <>
OpType OperatorNodeContents<LogicalExportExternalFile>::type = OpType::LOGICALEXPORTEXTERNALFILE;
template <>
OpType OperatorNodeContents<LogicalCreateDatabase>::type = OpType::LOGICALCREATEDATABASE;
template <>
OpType OperatorNodeContents<LogicalCreateTable>::type = OpType::LOGICALCREATETABLE;
template <>
OpType OperatorNodeContents<LogicalCreateIndex>::type = OpType::LOGICALCREATEINDEX;
template <>
OpType OperatorNodeContents<LogicalCreateFunction>::type = OpType::LOGICALCREATEFUNCTION;
template <>
OpType OperatorNodeContents<LogicalCreateNamespace>::type = OpType::LOGICALCREATENAMESPACE;
template <>
OpType OperatorNodeContents<LogicalCreateTrigger>::type = OpType::LOGICALCREATETRIGGER;
template <>
OpType OperatorNodeContents<LogicalCreateView>::type = OpType::LOGICALCREATEVIEW;
template <>
OpType OperatorNodeContents<LogicalDropDatabase>::type = OpType::LOGICALDROPDATABASE;
template <>
OpType OperatorNodeContents<LogicalDropTable>::type = OpType::LOGICALDROPTABLE;
template <>
OpType OperatorNodeContents<LogicalDropIndex>::type = OpType::LOGICALDROPINDEX;
template <>
OpType OperatorNodeContents<LogicalDropNamespace>::type = OpType::LOGICALDROPNAMESPACE;
template <>
OpType OperatorNodeContents<LogicalDropTrigger>::type = OpType::LOGICALDROPTRIGGER;
template <>
OpType OperatorNodeContents<LogicalDropView>::type = OpType::LOGICALDROPVIEW;
template <>
OpType OperatorNodeContents<LogicalAnalyze>::type = OpType::LOGICALANALYZE;

}  // namespace noisepage::optimizer
