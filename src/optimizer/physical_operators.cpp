#include "optimizer/physical_operators.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "optimizer/operator_visitor.h"
#include "parser/expression/abstract_expression.h"

namespace noisepage::optimizer {

//===--------------------------------------------------------------------===//
// TableFreeScan
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *TableFreeScan::Copy() const { return new TableFreeScan(*this); }

Operator TableFreeScan::Make() {
  auto *scan = new TableFreeScan();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(scan));
}

bool TableFreeScan::operator==(const BaseOperatorNodeContents &r) {
  return (r.GetOpType() == OpType::TABLEFREESCAN);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t TableFreeScan::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  // I guess every TableFreeScan object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *SeqScan::Copy() const { return new SeqScan(*this); }

Operator SeqScan::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                       std::vector<AnnotatedExpression> &&predicates, std::string table_alias, bool is_for_update) {
  auto *scan = new SeqScan();
  scan->database_oid_ = database_oid;
  scan->table_oid_ = table_oid;
  scan->predicates_ = std::move(predicates);
  scan->is_for_update_ = is_for_update;
  scan->table_alias_ = std::move(table_alias);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(scan));
}

bool SeqScan::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::SEQSCAN) return false;
  const SeqScan &node = *dynamic_cast<const SeqScan *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (predicates_.size() != node.predicates_.size()) return false;
  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) return false;
  }
  if (table_alias_ != node.table_alias_) return false;
  return is_for_update_ == node.is_for_update_;
}

common::hash_t SeqScan::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, predicates_.begin(), predicates_.end());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *IndexScan::Copy() const { return new IndexScan(*this); }

Operator IndexScan::Make(catalog::db_oid_t database_oid, catalog::table_oid_t tbl_oid, catalog::index_oid_t index_oid,
                         std::vector<AnnotatedExpression> &&predicates, bool is_for_update,
                         planner::IndexScanType scan_type,
                         std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> bounds) {
  auto *scan = new IndexScan();
  scan->database_oid_ = database_oid;
  scan->tbl_oid_ = tbl_oid;
  scan->index_oid_ = index_oid;
  scan->is_for_update_ = is_for_update;
  scan->predicates_ = std::move(predicates);
  scan->scan_type_ = scan_type;
  scan->bounds_ = std::move(bounds);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(scan));
}

bool IndexScan::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::INDEXSCAN) return false;
  const IndexScan &node = *dynamic_cast<const IndexScan *>(&r);
  if (database_oid_ != node.database_oid_ || index_oid_ != node.index_oid_ || tbl_oid_ != node.tbl_oid_ ||
      predicates_.size() != node.predicates_.size() || is_for_update_ != node.is_for_update_ ||
      scan_type_ != node.scan_type_)
    return false;

  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) return false;
  }

  if (bounds_.size() != node.bounds_.size()) return false;
  for (const auto &bound : bounds_) {
    if (node.bounds_.find(bound.first) == node.bounds_.end()) return false;

    std::vector<planner::IndexExpression> exprs = bound.second;
    std::vector<planner::IndexExpression> o_exprs = node.bounds_.find(bound.first)->second;
    if (exprs.size() != o_exprs.size()) return false;
    for (size_t idx = 0; idx < exprs.size(); idx++) {
      if (exprs[idx] == nullptr && o_exprs[idx] == nullptr) continue;
      if (exprs[idx] == nullptr && o_exprs[idx] != nullptr) return false;
      if (exprs[idx] != nullptr && o_exprs[idx] == nullptr) return false;
      if (*exprs[idx] != *o_exprs[idx]) return false;
    }
  }
  return true;
}

common::hash_t IndexScan::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(tbl_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(scan_type_));
  for (auto &pred : predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }

  for (const auto &bound : bounds_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(bound.first));
    for (auto expr : bound.second) {
      if (expr != nullptr) {
        hash = common::HashUtil::CombineHashes(hash, expr->Hash());
      }
    }
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// External file scan
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *ExternalFileScan::Copy() const { return new ExternalFileScan(*this); }

Operator ExternalFileScan::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                                char escape) {
  auto *get = new ExternalFileScan();
  get->format_ = format;
  get->file_name_ = std::move(file_name);
  get->delimiter_ = delimiter;
  get->quote_ = quote;
  get->escape_ = escape;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

bool ExternalFileScan::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::EXTERNALFILESCAN) return false;
  const auto &get = *dynamic_cast<const ExternalFileScan *>(&r);
  return (format_ == get.format_ && file_name_ == get.file_name_ && delimiter_ == get.delimiter_ &&
          quote_ == get.quote_ && escape_ == get.escape_);
}

common::hash_t ExternalFileScan::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
  return hash;
}

//===--------------------------------------------------------------------===//
// Query derived scan
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *QueryDerivedScan::Copy() const { return new QueryDerivedScan(*this); }

Operator QueryDerivedScan::Make(
    std::string table_alias,
    std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map) {
  auto *get = new QueryDerivedScan();
  get->table_alias_ = std::move(table_alias);
  get->alias_to_expr_map_ = alias_to_expr_map;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(get));
}

bool QueryDerivedScan::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::QUERYDERIVEDSCAN) return false;
  const QueryDerivedScan &node = *static_cast<const QueryDerivedScan *>(&r);
  if (table_alias_ != node.table_alias_) return false;
  return alias_to_expr_map_ == node.alias_to_expr_map_;
}

common::hash_t QueryDerivedScan::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
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
BaseOperatorNodeContents *OrderBy::Copy() const { return new OrderBy(*this); }

Operator OrderBy::Make() {
  auto *order_by = new OrderBy();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(order_by));
}

bool OrderBy::operator==(const BaseOperatorNodeContents &r) {
  return (r.GetOpType() == OpType::ORDERBY);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t OrderBy::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  // I guess every OrderBy object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalLimit
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *Limit::Copy() const { return new Limit(*this); }

Operator Limit::Make(size_t offset, size_t limit,
                     std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_columns,
                     std::vector<optimizer::OrderByOrderingType> &&sort_directions) {
  auto *limit_op = new Limit();
  limit_op->offset_ = offset;
  limit_op->limit_ = limit;
  limit_op->sort_exprs_ = std::move(sort_columns);
  limit_op->sort_directions_ = std::move(sort_directions);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(limit_op));
}

bool Limit::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LIMIT) return false;
  const Limit &node = *static_cast<const Limit *>(&r);
  if (offset_ != node.offset_) return false;
  if (limit_ != node.limit_) return false;
  if (sort_exprs_ != node.sort_exprs_) return false;
  if (sort_directions_ != node.sort_directions_) return false;
  return (true);
}

common::hash_t Limit::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));
  hash = common::HashUtil::CombineHashInRange(hash, sort_exprs_.begin(), sort_exprs_.end());
  hash = common::HashUtil::CombineHashInRange(hash, sort_directions_.begin(), sort_directions_.end());
  return hash;
}

//===--------------------------------------------------------------------===//
// InnerIndexJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *InnerIndexJoin::Copy() const { return new InnerIndexJoin(*this); }

Operator InnerIndexJoin::Make(
    catalog::table_oid_t tbl_oid, catalog::index_oid_t idx_oid, planner::IndexScanType scan_type,
    std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> join_keys,
    std::vector<AnnotatedExpression> join_predicates) {
  auto *join = new InnerIndexJoin();
  join->tbl_oid_ = tbl_oid;
  join->idx_oid_ = idx_oid;
  join->join_keys_ = std::move(join_keys);
  join->join_predicates_ = std::move(join_predicates);
  join->scan_type_ = scan_type;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t InnerIndexJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::SumHashes(hash, common::HashUtil::Hash(tbl_oid_));
  hash = common::HashUtil::SumHashes(hash, common::HashUtil::Hash(idx_oid_));
  hash = common::HashUtil::SumHashes(hash, common::HashUtil::Hash(scan_type_));

  std::vector<catalog::indexkeycol_oid_t> cols;
  for (auto &join_key : join_keys_) {
    cols.push_back(join_key.first);
  }

  std::sort(cols.begin(), cols.end());
  for (auto &col : cols) {
    hash = common::HashUtil::SumHashes(hash, common::HashUtil::Hash(col));
    for (auto &expr : join_keys_.find(col)->second) {
      if (expr == nullptr)
        hash = common::HashUtil::SumHashes(hash, 0);
      else
        hash = common::HashUtil::SumHashes(hash, expr->Hash());
    }
  }

  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  return hash;
}

bool InnerIndexJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::INNERINDEXJOIN) return false;
  const InnerIndexJoin &node = *dynamic_cast<const InnerIndexJoin *>(&r);
  if (tbl_oid_ != node.tbl_oid_) return false;
  if (idx_oid_ != node.idx_oid_) return false;
  if (scan_type_ != node.scan_type_) return false;

  if (join_predicates_.size() != node.join_predicates_.size()) return false;
  if (join_predicates_ != node.join_predicates_) return false;

  if (join_keys_.size() != node.join_keys_.size()) return false;
  for (auto &join_key : join_keys_) {
    if (node.join_keys_.find(join_key.first) == node.join_keys_.end()) return false;

    auto &expr = join_key.second;
    auto &other = node.join_keys_.find(join_key.first)->second;
    if (expr.size() != other.size()) return false;
    for (size_t i = 0; i < expr.size(); i++) {
      if (expr[i] == nullptr && other[i] == nullptr) continue;
      if ((expr[i] != nullptr && other[i] == nullptr) || (expr[i] == nullptr && other[i] != nullptr)) return false;
      if (*(expr[i]) != *(other[i])) return false;
    }
  }

  return true;
}

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *InnerNLJoin::Copy() const { return new InnerNLJoin(*this); }

Operator InnerNLJoin::Make(std::vector<AnnotatedExpression> &&join_predicates) {
  auto *join = new InnerNLJoin();
  join->join_predicates_ = std::move(join_predicates);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t InnerNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  return hash;
}

bool InnerNLJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::INNERNLJOIN) return false;
  const InnerNLJoin &node = *dynamic_cast<const InnerNLJoin *>(&r);
  if (join_predicates_.size() != node.join_predicates_.size()) return false;
  if (join_predicates_ != node.join_predicates_) return false;
  return true;
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LeftNLJoin::Copy() const { return new LeftNLJoin(*this); }

Operator LeftNLJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LeftNLJoin();
  join->join_predicate_ = join_predicate;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LeftNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LeftNLJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LEFTNLJOIN) return false;
  const LeftNLJoin &node = *static_cast<const LeftNLJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}
//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *RightNLJoin::Copy() const { return new RightNLJoin(*this); }

Operator RightNLJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new RightNLJoin();
  join->join_predicate_ = join_predicate;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t RightNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool RightNLJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::RIGHTNLJOIN) return false;
  const RightNLJoin &node = *static_cast<const RightNLJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *OuterNLJoin::Copy() const { return new OuterNLJoin(*this); }

Operator OuterNLJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new OuterNLJoin();
  join->join_predicate_ = join_predicate;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t OuterNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool OuterNLJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::OUTERNLJOIN) return false;
  const OuterNLJoin &node = *static_cast<const OuterNLJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *InnerHashJoin::Copy() const { return new InnerHashJoin(*this); }

Operator InnerHashJoin::Make(std::vector<AnnotatedExpression> &&join_predicates,
                             std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                             std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys) {
  auto *join = new InnerHashJoin();
  join->join_predicates_ = std::move(join_predicates);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t InnerHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  return hash;
}

bool InnerHashJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::INNERHASHJOIN) return false;
  const InnerHashJoin &node = *dynamic_cast<const InnerHashJoin *>(&r);
  if (left_keys_.size() != node.left_keys_.size() || right_keys_.size() != node.right_keys_.size() ||
      join_predicates_.size() != node.join_predicates_.size())
    return false;
  if (join_predicates_ != node.join_predicates_) return false;
  for (size_t i = 0; i < left_keys_.size(); i++) {
    if (*(left_keys_[i]) != *(node.left_keys_[i])) return false;
  }
  for (size_t i = 0; i < right_keys_.size(); i++) {
    if (*(right_keys_[i]) != *(node.right_keys_[i])) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftSemiHashJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LeftSemiHashJoin::Copy() const { return new LeftSemiHashJoin(*this); }

Operator LeftSemiHashJoin::Make(std::vector<AnnotatedExpression> &&join_predicates,
                                std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                                std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys) {
  auto *join = new LeftSemiHashJoin();
  join->join_predicates_ = std::move(join_predicates);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LeftSemiHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  return hash;
}

bool LeftSemiHashJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LEFTSEMIHASHJOIN) return false;
  const LeftSemiHashJoin &node = *dynamic_cast<const LeftSemiHashJoin *>(&r);
  if (left_keys_.size() != node.left_keys_.size() || right_keys_.size() != node.right_keys_.size() ||
      join_predicates_.size() != node.join_predicates_.size())
    return false;
  if (join_predicates_ != node.join_predicates_) return false;
  for (size_t i = 0; i < left_keys_.size(); i++) {
    if (*(left_keys_[i]) != *(node.left_keys_[i])) return false;
  }
  for (size_t i = 0; i < right_keys_.size(); i++) {
    if (*(right_keys_[i]) != *(node.right_keys_[i])) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *LeftHashJoin::Copy() const { return new LeftHashJoin(*this); }

Operator LeftHashJoin::Make(std::vector<AnnotatedExpression> &&join_predicates,
                            std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                            std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys) {
  auto *join = new LeftHashJoin();
  join->join_predicates_ = std::move(join_predicates);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t LeftHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNodeContents::Hash());
  }
  return hash;
}

bool LeftHashJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::LEFTHASHJOIN) return false;
  const LeftHashJoin &node = *dynamic_cast<const LeftHashJoin *>(&r);
  if (left_keys_.size() != node.left_keys_.size() || right_keys_.size() != node.right_keys_.size() ||
      join_predicates_.size() != node.join_predicates_.size())
    return false;
  if (join_predicates_ != node.join_predicates_) return false;
  for (size_t i = 0; i < left_keys_.size(); i++) {
    if (*(left_keys_[i]) != *(node.left_keys_[i])) return false;
  }
  for (size_t i = 0; i < right_keys_.size(); i++) {
    if (*(right_keys_[i]) != *(node.right_keys_[i])) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *RightHashJoin::Copy() const { return new RightHashJoin(*this); }

Operator RightHashJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new RightHashJoin();
  join->join_predicate_ = join_predicate;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t RightHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool RightHashJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::RIGHTHASHJOIN) return false;
  const RightHashJoin &node = *static_cast<const RightHashJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *OuterHashJoin::Copy() const { return new OuterHashJoin(*this); }

Operator OuterHashJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new OuterHashJoin();
  join->join_predicate_ = join_predicate;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(join));
}

common::hash_t OuterHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool OuterHashJoin::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::OUTERHASHJOIN) return false;
  const OuterHashJoin &node = *static_cast<const OuterHashJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *Insert::Copy() const { return new Insert(*this); }

Operator Insert::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                      std::vector<catalog::col_oid_t> &&columns,
                      std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &&values,
                      std::vector<catalog::index_oid_t> &&index_oids) {
#ifndef NDEBUG
  // We need to check whether the number of values for each insert vector
  // matches the number of columns
  for (const auto &insert_vals : values) {
    NOISEPAGE_ASSERT(columns.size() == insert_vals.size(), "Mismatched number of columns and values");
  }
#endif

  auto *op = new Insert();
  op->database_oid_ = database_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  op->values_ = std::move(values);
  op->index_oids_ = std::move(index_oids);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t Insert::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());

  // Perform a deep hash of the values
  for (const auto &insert_vals : values_) {
    hash = common::HashUtil::CombineHashInRange(hash, insert_vals.begin(), insert_vals.end());
  }

  return hash;
}

bool Insert::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::INSERT) return false;
  const Insert &node = *dynamic_cast<const Insert *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (columns_ != node.columns_) return false;
  if (values_ != node.values_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// InsertSelect
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *InsertSelect::Copy() const { return new InsertSelect(*this); }

Operator InsertSelect::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                            std::vector<catalog::index_oid_t> &&index_oids) {
  auto *insert_op = new InsertSelect();
  insert_op->database_oid_ = database_oid;
  insert_op->table_oid_ = table_oid;
  insert_op->index_oids_ = index_oids;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(insert_op));
}

common::hash_t InsertSelect::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool InsertSelect::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::INSERTSELECT) return false;
  const InsertSelect &node = *dynamic_cast<const InsertSelect *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *Delete::Copy() const { return new Delete(*this); }

Operator Delete::Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid) {
  auto *delete_op = new Delete();
  delete_op->database_oid_ = database_oid;
  delete_op->table_alias_ = std::move(table_alias);
  delete_op->table_oid_ = table_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(delete_op));
}

common::hash_t Delete::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool Delete::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DELETE) return false;
  const Delete &node = *dynamic_cast<const Delete *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  return table_oid_ == node.table_oid_;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *Update::Copy() const { return new Update(*this); }

Operator Update::Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid,
                      std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates) {
  auto *op = new Update();
  op->database_oid_ = database_oid;
  op->table_alias_ = std::move(table_alias);
  op->table_oid_ = table_oid;
  op->updates_ = updates;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t Update::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, updates_.begin(), updates_.end());
  return hash;
}

bool Update::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::UPDATE) return false;
  const Update &node = *dynamic_cast<const Update *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (updates_ != node.updates_) return false;
  return table_alias_ == node.table_alias_;
}

//===--------------------------------------------------------------------===//
// ExportExternalFile
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *ExportExternalFile::Copy() const { return new ExportExternalFile(*this); }

Operator ExportExternalFile::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                                  char escape) {
  auto *export_op = new ExportExternalFile();
  export_op->format_ = format;
  export_op->file_name_ = std::move(file_name);
  export_op->delimiter_ = delimiter;
  export_op->quote_ = quote;
  export_op->escape_ = escape;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(export_op));
}

bool ExportExternalFile::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::EXPORTEXTERNALFILE) return false;
  const auto &export_op = *dynamic_cast<const ExportExternalFile *>(&r);
  return (format_ == export_op.format_ && file_name_ == export_op.file_name_ && delimiter_ == export_op.delimiter_ &&
          quote_ == export_op.quote_ && escape_ == export_op.escape_);
}

common::hash_t ExportExternalFile::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));
  return hash;
}

//===--------------------------------------------------------------------===//
// HashGroupBy
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *HashGroupBy::Copy() const { return new HashGroupBy(*this); }

Operator HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                           std::vector<AnnotatedExpression> &&having) {
  auto *agg = new HashGroupBy();
  agg->columns_ = std::move(columns);
  agg->having_ = std::move(having);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(agg));
}

bool HashGroupBy::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::HASHGROUPBY) return false;
  const HashGroupBy &node = *static_cast<const HashGroupBy *>(&r);
  if (having_.size() != node.having_.size() || columns_.size() != node.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i] != node.having_[i]) return false;
  }
  for (size_t i = 0; i < columns_.size(); i++) {
    if (*(columns_[i]) != *(node.columns_[i])) return false;
  }
  return true;
}

common::hash_t HashGroupBy::Hash() const {
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
// SortGroupBy
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *SortGroupBy::Copy() const { return new SortGroupBy(*this); }

Operator SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                           std::vector<AnnotatedExpression> &&having) {
  auto *agg = new SortGroupBy();
  agg->columns_ = std::move(columns);
  agg->having_ = move(having);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(agg));
}

bool SortGroupBy::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::SORTGROUPBY) return false;
  const SortGroupBy &node = *static_cast<const SortGroupBy *>(&r);
  if (having_.size() != node.having_.size() || columns_.size() != node.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i] != node.having_[i]) return false;
  }
  for (size_t i = 0; i < columns_.size(); i++) {
    if (*(columns_[i]) != *(node.columns_[i])) return false;
  }
  return true;
}

common::hash_t SortGroupBy::Hash() const {
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
// Aggregate
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *Aggregate::Copy() const { return new Aggregate(*this); }

Operator Aggregate::Make() {
  auto *agg = new Aggregate();
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(agg));
}

bool Aggregate::operator==(const BaseOperatorNodeContents &r) {
  return (r.GetOpType() == OpType::AGGREGATE);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t Aggregate::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  // I guess every Aggregate object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// CreateDatabase
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateDatabase::Copy() const { return new CreateDatabase(*this); }

Operator CreateDatabase::Make(std::string database_name) {
  auto *op = new CreateDatabase();
  op->database_name_ = std::move(database_name);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t CreateDatabase::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));
  return hash;
}

bool CreateDatabase::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATEDATABASE) return false;
  const CreateDatabase &node = *dynamic_cast<const CreateDatabase *>(&r);
  return node.database_name_ == database_name_;
}

//===--------------------------------------------------------------------===//
// CreateTable
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateTable::Copy() const { return new CreateTable(*this); }

Operator CreateTable::Make(catalog::namespace_oid_t namespace_oid, std::string table_name,
                           std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&columns,
                           std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&foreign_keys) {
  auto *op = new CreateTable();
  op->namespace_oid_ = namespace_oid;
  op->table_name_ = std::move(table_name);
  op->columns_ = std::move(columns);
  op->foreign_keys_ = std::move(foreign_keys);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t CreateTable::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
  for (const auto &col : columns_) hash = common::HashUtil::CombineHashes(hash, col->Hash());
  for (const auto &fk : foreign_keys_) hash = common::HashUtil::CombineHashes(hash, fk->Hash());
  return hash;
}

bool CreateTable::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATETABLE) return false;
  const CreateTable &node = *dynamic_cast<const CreateTable *>(&r);
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
// CreateIndex
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateIndex::Copy() const {
  std::vector<catalog::IndexSchema::Column> columns;
  for (auto &col : schema_->GetColumns()) {
    columns.emplace_back(col);
  }
  auto schema = std::make_unique<catalog::IndexSchema>(std::move(columns), schema_->Type(), schema_->Unique(),
                                                       schema_->Primary(), schema_->Exclusion(), schema_->Immediate());

  auto op = new CreateIndex();
  op->namespace_oid_ = namespace_oid_;
  op->table_oid_ = table_oid_;
  op->index_name_ = index_name_;
  op->schema_ = std::move(schema);
  return op;
}

Operator CreateIndex::Make(catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                           std::string index_name, std::unique_ptr<catalog::IndexSchema> &&schema) {
  auto *op = new CreateIndex();
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->index_name_ = std::move(index_name);
  op->schema_ = std::move(schema);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t CreateIndex::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_name_));
  if (schema_ != nullptr) hash = common::HashUtil::CombineHashes(hash, schema_->Hash());
  return hash;
}

bool CreateIndex::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATEINDEX) return false;
  const CreateIndex &node = *dynamic_cast<const CreateIndex *>(&r);
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (index_name_ != node.index_name_) return false;
  if (schema_ != nullptr && *schema_ != *node.schema_) return false;
  if (schema_ == nullptr && node.schema_ != nullptr) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// CreateNamespace
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateNamespace::Copy() const { return new CreateNamespace(*this); }

Operator CreateNamespace::Make(std::string namespace_name) {
  auto *op = new CreateNamespace();
  op->namespace_name_ = std::move(namespace_name);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t CreateNamespace::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
  return hash;
}

bool CreateNamespace::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATENAMESPACE) return false;
  const CreateNamespace &node = *dynamic_cast<const CreateNamespace *>(&r);
  return node.namespace_name_ == namespace_name_;
}

//===--------------------------------------------------------------------===//
// CreateTrigger
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateTrigger::Copy() const { return new CreateTrigger(*this); }

Operator CreateTrigger::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                             catalog::table_oid_t table_oid, std::string trigger_name,
                             std::vector<std::string> &&trigger_funcnames, std::vector<std::string> &&trigger_args,
                             std::vector<catalog::col_oid_t> &&trigger_columns,
                             common::ManagedPointer<parser::AbstractExpression> &&trigger_when, int16_t trigger_type) {
  auto *op = new CreateTrigger();
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

common::hash_t CreateTrigger::Hash() const {
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

bool CreateTrigger::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATETRIGGER) return false;
  const CreateTrigger &node = *dynamic_cast<const CreateTrigger *>(&r);
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
// CreateView
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateView::Copy() const { return new CreateView(*this); }

Operator CreateView::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, std::string view_name,
                          common::ManagedPointer<parser::SelectStatement> view_query) {
  auto *op = new CreateView();
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->view_name_ = std::move(view_name);
  op->view_query_ = view_query;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t CreateView::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));
  if (view_query_ != nullptr) hash = common::HashUtil::CombineHashes(hash, view_query_->Hash());
  return hash;
}

bool CreateView::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATEVIEW) return false;
  const CreateView &node = *dynamic_cast<const CreateView *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (view_name_ != node.view_name_) return false;
  if (view_query_ == nullptr) return node.view_query_ == nullptr;
  return node.view_query_ != nullptr && *view_query_ == *node.view_query_;
}

//===--------------------------------------------------------------------===//
// CreateFunction
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *CreateFunction::Copy() const { return new CreateFunction(*this); }

Operator CreateFunction::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                              std::string function_name, parser::PLType language,
                              std::vector<std::string> &&function_body, std::vector<std::string> &&function_param_names,
                              std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types,
                              parser::BaseFunctionParameter::DataType return_type, size_t param_count, bool replace) {
  NOISEPAGE_ASSERT(function_param_names.size() == param_count && function_param_types.size() == param_count,
                   "Mismatched number of items in vector and number of function parameters");
  auto *op = new CreateFunction();
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

common::hash_t CreateFunction::Hash() const {
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

bool CreateFunction::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::CREATEFUNCTION) return false;
  const CreateFunction &node = *dynamic_cast<const CreateFunction *>(&r);
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
// DropDatabase
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *DropDatabase::Copy() const { return new DropDatabase(*this); }

Operator DropDatabase::Make(catalog::db_oid_t db_oid) {
  auto *op = new DropDatabase();
  op->db_oid_ = db_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t DropDatabase::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(db_oid_));
  return hash;
}

bool DropDatabase::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DROPDATABASE) return false;
  const DropDatabase &node = *dynamic_cast<const DropDatabase *>(&r);
  return node.db_oid_ == db_oid_;
}

//===--------------------------------------------------------------------===//
// DropTable
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *DropTable::Copy() const { return new DropTable(*this); }

Operator DropTable::Make(catalog::table_oid_t table_oid) {
  auto *op = new DropTable();
  op->table_oid_ = table_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t DropTable::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool DropTable::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DROPTABLE) return false;
  const DropTable &node = *dynamic_cast<const DropTable *>(&r);
  return node.table_oid_ == table_oid_;
}

//===--------------------------------------------------------------------===//
// DropIndex
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *DropIndex::Copy() const { return new DropIndex(*this); }

Operator DropIndex::Make(catalog::index_oid_t index_oid) {
  auto *op = new DropIndex();
  op->index_oid_ = index_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t DropIndex::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));
  return hash;
}

bool DropIndex::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DROPINDEX) return false;
  const DropIndex &node = *dynamic_cast<const DropIndex *>(&r);
  return node.index_oid_ == index_oid_;
}

//===--------------------------------------------------------------------===//
// DropNamespace
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *DropNamespace::Copy() const { return new DropNamespace(*this); }

Operator DropNamespace::Make(catalog::namespace_oid_t namespace_oid) {
  auto *op = new DropNamespace();
  op->namespace_oid_ = namespace_oid;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t DropNamespace::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  return hash;
}

bool DropNamespace::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DROPNAMESPACE) return false;
  const DropNamespace &node = *dynamic_cast<const DropNamespace *>(&r);
  return node.namespace_oid_ == namespace_oid_;
}

//===--------------------------------------------------------------------===//
// DropTrigger
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *DropTrigger::Copy() const { return new DropTrigger(*this); }

Operator DropTrigger::Make(catalog::db_oid_t database_oid, catalog::trigger_oid_t trigger_oid, bool if_exists) {
  auto *op = new DropTrigger();
  op->database_oid_ = database_oid;
  op->trigger_oid_ = trigger_oid;
  op->if_exists_ = if_exists;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t DropTrigger::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
  return hash;
}

bool DropTrigger::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DROPTRIGGER) return false;
  const DropTrigger &node = *dynamic_cast<const DropTrigger *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (trigger_oid_ != node.trigger_oid_) return false;
  if (if_exists_ != node.if_exists_) return false;
  return node.namespace_oid_ == namespace_oid_;
}

//===--------------------------------------------------------------------===//
// DropView
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *DropView::Copy() const { return new DropView(*this); }

Operator DropView::Make(catalog::db_oid_t database_oid, catalog::view_oid_t view_oid, bool if_exists) {
  auto *op = new DropView();
  op->database_oid_ = database_oid;
  op->view_oid_ = view_oid;
  op->if_exists_ = if_exists;
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t DropView::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
  return hash;
}

bool DropView::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::DROPVIEW) return false;
  const DropView &node = *dynamic_cast<const DropView *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (view_oid_ != node.view_oid_) return false;
  return if_exists_ == node.if_exists_;
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
BaseOperatorNodeContents *Analyze::Copy() const { return new Analyze(*this); }

Operator Analyze::Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                       std::vector<catalog::col_oid_t> &&columns) {
  auto *op = new Analyze();
  op->database_oid_ = database_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  return Operator(common::ManagedPointer<BaseOperatorNodeContents>(op));
}

common::hash_t Analyze::Hash() const {
  common::hash_t hash = BaseOperatorNodeContents::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, columns_.begin(), columns_.end());
  return hash;
}

bool Analyze::operator==(const BaseOperatorNodeContents &r) {
  if (r.GetOpType() != OpType::ANALYZE) return false;
  const Analyze &node = *dynamic_cast<const Analyze *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (columns_ != node.columns_) return false;
  return true;
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNodeContents<TableFreeScan>::name = "TableFreeScan";
template <>
const char *OperatorNodeContents<SeqScan>::name = "SeqScan";
template <>
const char *OperatorNodeContents<IndexScan>::name = "IndexScan";
template <>
const char *OperatorNodeContents<ExternalFileScan>::name = "ExternalFileScan";
template <>
const char *OperatorNodeContents<QueryDerivedScan>::name = "QueryDerivedScan";
template <>
const char *OperatorNodeContents<OrderBy>::name = "OrderBy";
template <>
const char *OperatorNodeContents<Limit>::name = "Limit";
template <>
const char *OperatorNodeContents<InnerIndexJoin>::name = "InnerIndexJoin";
template <>
const char *OperatorNodeContents<InnerNLJoin>::name = "InnerNLJoin";
template <>
const char *OperatorNodeContents<LeftNLJoin>::name = "LeftNLJoin";
template <>
const char *OperatorNodeContents<RightNLJoin>::name = "RightNLJoin";
template <>
const char *OperatorNodeContents<OuterNLJoin>::name = "OuterNLJoin";
template <>
const char *OperatorNodeContents<InnerHashJoin>::name = "InnerHashJoin";
template <>
const char *OperatorNodeContents<LeftSemiHashJoin>::name = "LeftSemiHashJoin";
template <>
const char *OperatorNodeContents<LeftHashJoin>::name = "LeftHashJoin";
template <>
const char *OperatorNodeContents<RightHashJoin>::name = "RightHashJoin";
template <>
const char *OperatorNodeContents<OuterHashJoin>::name = "OuterHashJoin";
template <>
const char *OperatorNodeContents<Insert>::name = "Insert";
template <>
const char *OperatorNodeContents<InsertSelect>::name = "InsertSelect";
template <>
const char *OperatorNodeContents<Delete>::name = "Delete";
template <>
const char *OperatorNodeContents<Update>::name = "Update";
template <>
const char *OperatorNodeContents<HashGroupBy>::name = "HashGroupBy";
template <>
const char *OperatorNodeContents<SortGroupBy>::name = "SortGroupBy";
template <>
const char *OperatorNodeContents<Aggregate>::name = "Aggregate";
template <>
const char *OperatorNodeContents<ExportExternalFile>::name = "ExportExternalFile";
template <>
const char *OperatorNodeContents<CreateDatabase>::name = "CreateDatabase";
template <>
const char *OperatorNodeContents<CreateTable>::name = "CreateTable";
template <>
const char *OperatorNodeContents<CreateIndex>::name = "CreateIndex";
template <>
const char *OperatorNodeContents<CreateFunction>::name = "CreateFunction";
template <>
const char *OperatorNodeContents<CreateNamespace>::name = "CreateNamespace";
template <>
const char *OperatorNodeContents<CreateTrigger>::name = "CreateTrigger";
template <>
const char *OperatorNodeContents<CreateView>::name = "CreateView";
template <>
const char *OperatorNodeContents<DropDatabase>::name = "DropDatabase";
template <>
const char *OperatorNodeContents<DropTable>::name = "DropTable";
template <>
const char *OperatorNodeContents<DropIndex>::name = "DropIndex";
template <>
const char *OperatorNodeContents<DropNamespace>::name = "DropNamespace";
template <>
const char *OperatorNodeContents<DropTrigger>::name = "DropTrigger";
template <>
const char *OperatorNodeContents<DropView>::name = "DropView";
template <>
const char *OperatorNodeContents<Analyze>::name = "Analyze";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNodeContents<TableFreeScan>::type = OpType::TABLEFREESCAN;
template <>
OpType OperatorNodeContents<SeqScan>::type = OpType::SEQSCAN;
template <>
OpType OperatorNodeContents<IndexScan>::type = OpType::INDEXSCAN;
template <>
OpType OperatorNodeContents<ExternalFileScan>::type = OpType::EXTERNALFILESCAN;
template <>
OpType OperatorNodeContents<QueryDerivedScan>::type = OpType::QUERYDERIVEDSCAN;
template <>
OpType OperatorNodeContents<OrderBy>::type = OpType::ORDERBY;
template <>
OpType OperatorNodeContents<Limit>::type = OpType::LIMIT;
template <>
OpType OperatorNodeContents<InnerIndexJoin>::type = OpType::INNERINDEXJOIN;
template <>
OpType OperatorNodeContents<InnerNLJoin>::type = OpType::INNERNLJOIN;
template <>
OpType OperatorNodeContents<LeftNLJoin>::type = OpType::LEFTNLJOIN;
template <>
OpType OperatorNodeContents<RightNLJoin>::type = OpType::RIGHTNLJOIN;
template <>
OpType OperatorNodeContents<OuterNLJoin>::type = OpType::OUTERNLJOIN;
template <>
OpType OperatorNodeContents<InnerHashJoin>::type = OpType::INNERHASHJOIN;
template <>
OpType OperatorNodeContents<LeftSemiHashJoin>::type = OpType::LEFTSEMIHASHJOIN;
template <>
OpType OperatorNodeContents<LeftHashJoin>::type = OpType::LEFTHASHJOIN;
template <>
OpType OperatorNodeContents<RightHashJoin>::type = OpType::RIGHTHASHJOIN;
template <>
OpType OperatorNodeContents<OuterHashJoin>::type = OpType::OUTERHASHJOIN;
template <>
OpType OperatorNodeContents<Insert>::type = OpType::INSERT;
template <>
OpType OperatorNodeContents<InsertSelect>::type = OpType::INSERTSELECT;
template <>
OpType OperatorNodeContents<Delete>::type = OpType::DELETE;
template <>
OpType OperatorNodeContents<Update>::type = OpType::UPDATE;
template <>
OpType OperatorNodeContents<HashGroupBy>::type = OpType::HASHGROUPBY;
template <>
OpType OperatorNodeContents<SortGroupBy>::type = OpType::SORTGROUPBY;
template <>
OpType OperatorNodeContents<Aggregate>::type = OpType::AGGREGATE;
template <>
OpType OperatorNodeContents<ExportExternalFile>::type = OpType::EXPORTEXTERNALFILE;
template <>
OpType OperatorNodeContents<CreateDatabase>::type = OpType::CREATEDATABASE;
template <>
OpType OperatorNodeContents<CreateTable>::type = OpType::CREATETABLE;
template <>
OpType OperatorNodeContents<CreateIndex>::type = OpType::CREATEINDEX;
template <>
OpType OperatorNodeContents<CreateFunction>::type = OpType::CREATEFUNCTION;
template <>
OpType OperatorNodeContents<CreateNamespace>::type = OpType::CREATENAMESPACE;
template <>
OpType OperatorNodeContents<CreateTrigger>::type = OpType::CREATETRIGGER;
template <>
OpType OperatorNodeContents<CreateView>::type = OpType::CREATEVIEW;
template <>
OpType OperatorNodeContents<DropDatabase>::type = OpType::DROPDATABASE;
template <>
OpType OperatorNodeContents<DropTable>::type = OpType::DROPTABLE;
template <>
OpType OperatorNodeContents<DropIndex>::type = OpType::DROPINDEX;
template <>
OpType OperatorNodeContents<DropNamespace>::type = OpType::DROPNAMESPACE;
template <>
OpType OperatorNodeContents<DropTrigger>::type = OpType::DROPTRIGGER;
template <>
OpType OperatorNodeContents<DropView>::type = OpType::DROPVIEW;
template <>
OpType OperatorNodeContents<Analyze>::type = OpType::ANALYZE;

}  // namespace noisepage::optimizer
