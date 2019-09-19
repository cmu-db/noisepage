#include "optimizer/physical_operators.h"
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
// TableFreeScan
//===--------------------------------------------------------------------===//
Operator TableFreeScan::Make() {
  auto *table_free_scan = new TableFreeScan;
  return Operator(table_free_scan);
}

bool TableFreeScan::operator==(const BaseOperatorNode &r) {
  return (r.GetType() == OpType::TABLEFREESCAN);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t TableFreeScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  // I guess every TableFreeScan object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
Operator SeqScan::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> &&predicates,
                       std::string table_alias, bool is_for_update) {
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

common::hash_t SeqScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, predicates_.begin(), predicates_.end());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
Operator IndexScan::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                         catalog::index_oid_t index_oid, std::vector<AnnotatedExpression> &&predicates,
                         std::string table_alias, bool is_for_update,
                         std::vector<catalog::col_oid_t> &&key_column_oid_list,
                         std::vector<parser::ExpressionType> &&expr_type_list,
                         std::vector<type::TransientValue> &&value_list) {
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
      table_alias_ != node.table_alias_ || key_column_oid_list_ != node.key_column_oid_list_ ||
      expr_type_list_ != node.expr_type_list_ || predicates_.size() != node.predicates_.size() ||
      key_column_oid_list_.size() != node.key_column_oid_list_.size() || is_for_update_ != node.is_for_update_ ||
      expr_type_list_.size() != node.expr_type_list_.size() || value_list_.size() != node.value_list_.size())
    return false;

  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].GetExpr() != node.predicates_[i].GetExpr()) return false;
  }
  if (key_column_oid_list_ != node.key_column_oid_list_) return false;
  if (expr_type_list_ != node.expr_type_list_) return false;
  if (value_list_ != node.value_list_) return false;
  return true;
}

common::hash_t IndexScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));
  for (auto &pred : predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
  }
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
Operator ExternalFileScan::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
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
Operator QueryDerivedScan::Make(
    std::string table_alias,
    std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map) {
  auto *get = new QueryDerivedScan;
  get->table_alias_ = std::move(table_alias);
  get->alias_to_expr_map_ = alias_to_expr_map;

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
Operator OrderBy::Make() {
  auto *order_by = new OrderBy;

  return Operator(order_by);
}

bool OrderBy::operator==(const BaseOperatorNode &r) {
  return (r.GetType() == OpType::ORDERBY);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t OrderBy::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  // I guess every OrderBy object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalLimit
//===--------------------------------------------------------------------===//
Operator Limit::Make(size_t offset, size_t limit,
                     std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_columns,
                     std::vector<planner::OrderByOrderingType> &&sort_directions) {
  auto *limit_op = new Limit;
  limit_op->offset_ = offset;
  limit_op->limit_ = limit;
  limit_op->sort_exprs_ = std::move(sort_columns);
  limit_op->sort_directions_ = std::move(sort_directions);
  return Operator(limit_op);
}

bool Limit::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LIMIT) return false;
  const Limit &node = *static_cast<const Limit *>(&r);
  if (offset_ != node.offset_) return false;
  if (limit_ != node.limit_) return false;
  if (sort_exprs_ != node.sort_exprs_) return false;
  if (sort_directions_ != node.sort_directions_) return false;
  return (true);
}

common::hash_t Limit::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));
  hash = common::HashUtil::CombineHashInRange(hash, sort_exprs_.begin(), sort_exprs_.end());
  hash = common::HashUtil::CombineHashInRange(hash, sort_directions_.begin(), sort_directions_.end());
  return hash;
}

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
Operator InnerNLJoin::Make(std::vector<AnnotatedExpression> &&join_predicates,
                           std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                           std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys) {
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
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
  }
  return hash;
}

bool InnerNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INNERNLJOIN) return false;
  const InnerNLJoin &node = *dynamic_cast<const InnerNLJoin *>(&r);
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
// LeftNLJoin
//===--------------------------------------------------------------------===//
Operator LeftNLJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LeftNLJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t LeftNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LeftNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LEFTNLJOIN) return false;
  const LeftNLJoin &node = *static_cast<const LeftNLJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}
//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
Operator RightNLJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new RightNLJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t RightNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool RightNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::RIGHTNLJOIN) return false;
  const RightNLJoin &node = *static_cast<const RightNLJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
Operator OuterNLJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new OuterNLJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t OuterNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool OuterNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::OUTERNLJOIN) return false;
  const OuterNLJoin &node = *static_cast<const OuterNLJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
Operator InnerHashJoin::Make(std::vector<AnnotatedExpression> &&join_predicates,
                             std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                             std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys) {
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
  for (auto &pred : join_predicates_) {
    auto expr = pred.GetExpr();
    if (expr)
      hash = common::HashUtil::SumHashes(hash, expr->Hash());
    else
      hash = common::HashUtil::SumHashes(hash, BaseOperatorNode::Hash());
  }
  return hash;
}

bool InnerHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INNERHASHJOIN) return false;
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
// LeftHashJoin
//===--------------------------------------------------------------------===//
Operator LeftHashJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new LeftHashJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t LeftHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool LeftHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::LEFTHASHJOIN) return false;
  const LeftHashJoin &node = *static_cast<const LeftHashJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
Operator RightHashJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new RightHashJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t RightHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool RightHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::RIGHTHASHJOIN) return false;
  const RightHashJoin &node = *static_cast<const RightHashJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
Operator OuterHashJoin::Make(common::ManagedPointer<parser::AbstractExpression> join_predicate) {
  auto *join = new OuterHashJoin();
  join->join_predicate_ = join_predicate;
  return Operator(join);
}

common::hash_t OuterHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  return hash;
}

bool OuterHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::OUTERHASHJOIN) return false;
  const OuterHashJoin &node = *static_cast<const OuterHashJoin *>(&r);
  return (*join_predicate_ == *(node.join_predicate_));
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
Operator Insert::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&columns,
                      std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &&values,
                      std::vector<catalog::index_oid_t> &&index_oids) {
#ifndef NDEBUG
  // We need to check whether the number of values for each insert vector
  // matches the number of columns
  for (const auto &insert_vals : values) {
    TERRIER_ASSERT(columns.size() == insert_vals.size(), "Mismatched number of columns and values");
  }
#endif

  auto *op = new Insert;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->columns_ = std::move(columns);
  op->values_ = std::move(values);
  op->index_oids_ = std::move(index_oids);
  return Operator(op);
}

common::hash_t Insert::Hash() const {
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

bool Insert::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INSERT) return false;
  const Insert &node = *dynamic_cast<const Insert *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (columns_ != node.columns_) return false;
  if (values_ != node.values_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// InsertSelect
//===--------------------------------------------------------------------===//
Operator InsertSelect::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                            catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids) {
  auto *insert_op = new InsertSelect;
  insert_op->database_oid_ = database_oid;
  insert_op->namespace_oid_ = namespace_oid;
  insert_op->table_oid_ = table_oid;
  insert_op->index_oids_ = index_oids;
  return Operator(insert_op);
}

common::hash_t InsertSelect::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  return hash;
}

bool InsertSelect::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::INSERTSELECT) return false;
  const InsertSelect &node = *dynamic_cast<const InsertSelect *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
Operator Delete::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid,
                      common::ManagedPointer<parser::AbstractExpression> delete_condition) {
  auto *delete_op = new Delete;
  delete_op->database_oid_ = database_oid;
  delete_op->namespace_oid_ = namespace_oid;
  delete_op->table_oid_ = table_oid;
  delete_op->delete_condition_ = delete_condition;
  return Operator(delete_op);
}

common::hash_t Delete::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashes(hash, delete_condition_->Hash());
  return hash;
}

bool Delete::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::DELETE) return false;
  const Delete &node = *dynamic_cast<const Delete *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  return (*delete_condition_ == *(node.delete_condition_));
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
Operator Update::Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid,
                      std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates) {
  auto *op = new Update;
  op->database_oid_ = database_oid;
  op->namespace_oid_ = namespace_oid;
  op->table_oid_ = table_oid;
  op->updates_ = updates;
  return Operator(op);
}

common::hash_t Update::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, updates_.begin(), updates_.end());
  return hash;
}

bool Update::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::UPDATE) return false;
  const Update &node = *dynamic_cast<const Update *>(&r);
  if (database_oid_ != node.database_oid_) return false;
  if (namespace_oid_ != node.namespace_oid_) return false;
  if (table_oid_ != node.table_oid_) return false;
  if (updates_ != node.updates_) return false;
  return (true);
}

//===--------------------------------------------------------------------===//
// ExportExternalFile
//===--------------------------------------------------------------------===//
Operator ExportExternalFile::Make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
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
Operator HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                           std::vector<AnnotatedExpression> &&having) {
  auto *agg = new HashGroupBy;
  agg->columns_ = std::move(columns);
  agg->having_ = std::move(having);
  return Operator(agg);
}

bool HashGroupBy::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::HASHGROUPBY) return false;
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
// SortGroupBy
//===--------------------------------------------------------------------===//
Operator SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                           std::vector<AnnotatedExpression> &&having) {
  auto *agg = new SortGroupBy;
  agg->columns_ = std::move(columns);
  agg->having_ = move(having);
  return Operator(agg);
}

bool SortGroupBy::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::SORTGROUPBY) return false;
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
// Aggregate
//===--------------------------------------------------------------------===//
Operator Aggregate::Make() {
  auto *agg = new Aggregate;
  return Operator(agg);
}

bool Aggregate::operator==(const BaseOperatorNode &r) {
  return (r.GetType() == OpType::AGGREGATE);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t Aggregate::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  // I guess every Aggregate object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
// Hash
//===--------------------------------------------------------------------===//
Operator Distinct::Make() {
  auto *hash = new Distinct;
  return Operator(hash);
}

bool Distinct::operator==(const BaseOperatorNode &r) {
  return (r.GetType() == OpType::DISTINCT);
  // Again, there isn't any internal data so I guess we're always equal!
}

common::hash_t Distinct::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  // I guess every Aggregate object hashes to the same thing?
  return hash;
}

//===--------------------------------------------------------------------===//
template <typename T>
void OperatorNode<T>::Accept(OperatorVisitor *v) const {
  v->Visit(reinterpret_cast<const T *>(this));
}

//===--------------------------------------------------------------------===//
template <>
const char *OperatorNode<TableFreeScan>::name = "TableFreeScan";
template <>
const char *OperatorNode<SeqScan>::name = "SeqScan";
template <>
const char *OperatorNode<IndexScan>::name = "IndexScan";
template <>
const char *OperatorNode<ExternalFileScan>::name = "ExternalFileScan";
template <>
const char *OperatorNode<QueryDerivedScan>::name = "QueryDerivedScan";
template <>
const char *OperatorNode<OrderBy>::name = "OrderBy";
template <>
const char *OperatorNode<Limit>::name = "Limit";
template <>
const char *OperatorNode<InnerNLJoin>::name = "InnerNLJoin";
template <>
const char *OperatorNode<LeftNLJoin>::name = "LeftNLJoin";
template <>
const char *OperatorNode<RightNLJoin>::name = "RightNLJoin";
template <>
const char *OperatorNode<OuterNLJoin>::name = "OuterNLJoin";
template <>
const char *OperatorNode<InnerHashJoin>::name = "InnerHashJoin";
template <>
const char *OperatorNode<LeftHashJoin>::name = "LeftHashJoin";
template <>
const char *OperatorNode<RightHashJoin>::name = "RightHashJoin";
template <>
const char *OperatorNode<OuterHashJoin>::name = "OuterHashJoin";
template <>
const char *OperatorNode<Insert>::name = "Insert";
template <>
const char *OperatorNode<InsertSelect>::name = "InsertSelect";
template <>
const char *OperatorNode<Delete>::name = "Delete";
template <>
const char *OperatorNode<Update>::name = "Update";
template <>
const char *OperatorNode<HashGroupBy>::name = "HashGroupBy";
template <>
const char *OperatorNode<SortGroupBy>::name = "SortGroupBy";
template <>
const char *OperatorNode<Distinct>::name = "Distinct";
template <>
const char *OperatorNode<Aggregate>::name = "Aggregate";
template <>
const char *OperatorNode<ExportExternalFile>::name = "ExportExternalFile";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<TableFreeScan>::type = OpType::TABLEFREESCAN;
template <>
OpType OperatorNode<SeqScan>::type = OpType::SEQSCAN;
template <>
OpType OperatorNode<IndexScan>::type = OpType::INDEXSCAN;
template <>
OpType OperatorNode<ExternalFileScan>::type = OpType::EXTERNALFILESCAN;
template <>
OpType OperatorNode<QueryDerivedScan>::type = OpType::QUERYDERIVEDSCAN;
template <>
OpType OperatorNode<OrderBy>::type = OpType::ORDERBY;
template <>
OpType OperatorNode<Distinct>::type = OpType::DISTINCT;
template <>
OpType OperatorNode<Limit>::type = OpType::LIMIT;
template <>
OpType OperatorNode<InnerNLJoin>::type = OpType::INNERNLJOIN;
template <>
OpType OperatorNode<LeftNLJoin>::type = OpType::LEFTNLJOIN;
template <>
OpType OperatorNode<RightNLJoin>::type = OpType::RIGHTNLJOIN;
template <>
OpType OperatorNode<OuterNLJoin>::type = OpType::OUTERNLJOIN;
template <>
OpType OperatorNode<InnerHashJoin>::type = OpType::INNERHASHJOIN;
template <>
OpType OperatorNode<LeftHashJoin>::type = OpType::LEFTHASHJOIN;
template <>
OpType OperatorNode<RightHashJoin>::type = OpType::RIGHTHASHJOIN;
template <>
OpType OperatorNode<OuterHashJoin>::type = OpType::OUTERHASHJOIN;
template <>
OpType OperatorNode<Insert>::type = OpType::INSERT;
template <>
OpType OperatorNode<InsertSelect>::type = OpType::INSERTSELECT;
template <>
OpType OperatorNode<Delete>::type = OpType::DELETE;
template <>
OpType OperatorNode<Update>::type = OpType::UPDATE;
template <>
OpType OperatorNode<HashGroupBy>::type = OpType::HASHGROUPBY;
template <>
OpType OperatorNode<SortGroupBy>::type = OpType::SORTGROUPBY;
template <>
OpType OperatorNode<Aggregate>::type = OpType::AGGREGATE;
template <>
OpType OperatorNode<ExportExternalFile>::type = OpType::EXPORTEXTERNALFILE;

template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return type < OpType::LOGICALPHYSICALDELIMITER;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return type > OpType::LOGICALPHYSICALDELIMITER;
}

}  // namespace terrier::optimizer
