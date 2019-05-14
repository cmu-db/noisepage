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

/**
 * @return A TableFreeScan operator
 */
Operator TableFreeScan::make() {
  auto *table_free_scan = new TableFreeScan;
  return Operator(table_free_scan);
}

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
Operator SeqScan::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> predicates, bool update) {
  auto *scan = new SeqScan;
  scan->database_oid_ = database_oid;
  scan->namespace_oid_ = namespace_oid;
  scan->table_oid_ = table_oid;
  scan->predicates_ = std::move(predicates);
  scan->is_for_update_ = update;
  return Operator(scan);
}

bool SeqScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::SeqScan) return false;
  const SeqScan &node = *dynamic_cast<const SeqScan *>(&r);
  if (predicates_.size() != node.predicates_.size()) return false;
  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].expr_ != node.predicates_[i].expr_) return false;
  }
  return true;
}

common::hash_t SeqScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : predicates_) hash = common::HashUtil::CombineHashes(hash, pred.expr_->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
Operator IndexScan::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                         catalog::index_oid_t index_oid, std::vector<AnnotatedExpression> predicates, bool update,
                         std::vector<catalog::col_oid_t> key_column_id_list,
                         std::vector<parser::ExpressionType> expr_type_list,
                         std::vector<type::TransientValue> value_list) {
  auto *scan = new IndexScan;
  scan->database_oid_ = database_oid;
  scan->namespace_oid_ = namespace_oid;
  scan->index_oid_ = index_oid;
  scan->is_for_update_ = update;
  scan->predicates_ = std::move(predicates);
  scan->key_column_id_list_ = std::move(key_column_id_list);
  scan->expr_type_list_ = std::move(expr_type_list);
  scan->value_list_ = std::move(value_list);

  return Operator(scan);
}

bool IndexScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::IndexScan) return false;
  const IndexScan &node = *dynamic_cast<const IndexScan *>(&r);
  if (index_oid_ != node.index_oid_ || key_column_id_list_ != node.key_column_id_list_ ||
      expr_type_list_ != node.expr_type_list_ || predicates_.size() != node.predicates_.size())
    return false;

  for (size_t i = 0; i < predicates_.size(); i++) {
    if (predicates_[i].expr_ != node.predicates_[i].expr_) return false;
  }
  return true;
}

common::hash_t IndexScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&index_oid_));
  for (auto &pred : predicates_) hash = common::HashUtil::CombineHashes(hash, pred.expr_->Hash());
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
  if (r.GetType() != OpType::ExternalFileScan) return false;
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
    catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
    std::unordered_map<catalog::table_oid_t, std::shared_ptr<parser::AbstractExpression>> &&alias_to_expr_map) {
  auto *get = new QueryDerivedScan;
  get->database_oid_ = database_oid;
  get->namespace_oid_ = namespace_oid;
  get->table_oid_ = table_oid;
  get->alias_to_expr_map_ = std::move(alias_to_expr_map);

  return Operator(get);
}

bool QueryDerivedScan::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::QueryDerivedScan) return false;
  const auto &get = *dynamic_cast<const QueryDerivedScan *>(&r);
  if (database_oid_ != get.database_oid_ || alias_to_expr_map_ != get.alias_to_expr_map_ ||
      namespace_oid_ != get.namespace_oid_ || table_oid_ != get.table_oid_)
    return false;
  return true;
}

common::hash_t QueryDerivedScan::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid_));

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
Operator InnerNLJoin::make(std::vector<AnnotatedExpression> conditions,
                           std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                           std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys) {
  auto *join = new InnerNLJoin();
  join->join_predicates_ = std::move(conditions);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);

  return Operator(join);
}

common::hash_t InnerNLJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) hash = common::HashUtil::CombineHashes(hash, pred.expr_->Hash());
  return hash;
}

bool InnerNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::InnerNLJoin) return false;
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
    if (join_predicates_[i].expr_ != node.join_predicates_[i].expr_) return false;
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
Operator InnerHashJoin::make(std::vector<AnnotatedExpression> conditions,
                             std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                             std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys) {
  auto *join = new InnerHashJoin();
  join->join_predicates_ = std::move(conditions);
  join->left_keys_ = std::move(left_keys);
  join->right_keys_ = std::move(right_keys);
  return Operator(join);
}

common::hash_t InnerHashJoin::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : left_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys_) hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates_) hash = common::HashUtil::CombineHashes(hash, pred.expr_->Hash());
  return hash;
}

bool InnerHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.GetType() != OpType::InnerHashJoin) return false;
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
    if (join_predicates_[i].expr_ != node.join_predicates_[i].expr_) return false;
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
                      catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids,
                      const std::vector<std::string> *columns,
                      const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values) {
  auto *insert_op = new Insert;
  insert_op->database_oid_ = database_oid;
  insert_op->namespace_oid = namespace_oid;
  insert_op->table_oid_ = table_oid;
  insert_op->index_oids_ = std::move(index_oids);
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// InsertSelect
//===--------------------------------------------------------------------===//
Operator InsertSelect::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                            catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids) {
  auto *insert_op = new InsertSelect;
  insert_op->database_oid_ = database_oid;
  insert_op->namespace_oid = namespace_oid;
  insert_op->table_oid_ = table_oid;
  insert_op->index_oids_ = std::move(index_oids);
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
Operator Delete::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids) {
  auto *delete_op = new Delete;
  delete_op->database_oid_ = database_oid;
  delete_op->namespace_oid = namespace_oid;
  delete_op->table_oid_ = table_oid;
  delete_op->index_oids_ = std::move(index_oids);
  return Operator(delete_op);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
Operator Update::make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                      catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids,
                      const std::vector<std::unique_ptr<parser::UpdateClause>> *updates) {
  auto *update_op = new Update;
  update_op->database_oid_ = database_oid;
  update_op->namespace_oid = namespace_oid;
  update_op->table_oid_ = table_oid;
  update_op->index_oids_ = std::move(index_oids);
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
  if (r.GetType() != OpType::ExportExternalFile) return false;
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
  if (r.GetType() != OpType::HashGroupBy) return false;
  const HashGroupBy &hash_op = *dynamic_cast<const HashGroupBy *>(&r);
  if (having_.size() != hash_op.having_.size() || columns_.size() != hash_op.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i].expr_ != hash_op.having_[i].expr_) return false;
  }

  std::unordered_set<std::shared_ptr<parser::AbstractExpression>> l_set, r_set;
  for (auto &expr : columns_) l_set.emplace(expr);
  for (auto &expr : hash_op.columns_) r_set.emplace(expr);
  return l_set == r_set;
}

common::hash_t HashGroupBy::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having_) hash = common::HashUtil::CombineHashes(hash, pred.expr_->Hash());
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
  if (r.GetType() != OpType::SortGroupBy) return false;
  const SortGroupBy &sort_op = *dynamic_cast<const SortGroupBy *>(&r);
  if (having_.size() != sort_op.having_.size() || columns_.size() != sort_op.columns_.size()) return false;
  for (size_t i = 0; i < having_.size(); i++) {
    if (having_[i].expr_ != sort_op.having_[i].expr_) return false;
  }
  std::unordered_set<std::shared_ptr<parser::AbstractExpression>> l_set, r_set;
  for (auto &expr : columns_) l_set.emplace(expr);
  for (auto &expr : sort_op.columns_) r_set.emplace(expr);
  return l_set == r_set;
}

common::hash_t SortGroupBy::Hash() const {
  common::hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having_) hash = common::HashUtil::CombineHashes(hash, pred.expr_->Hash());
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
OpType OperatorNode<TableFreeScan>::type_ = OpType::TableFreeScan;
template <>
OpType OperatorNode<SeqScan>::type_ = OpType::SeqScan;
template <>
OpType OperatorNode<IndexScan>::type_ = OpType::IndexScan;
template <>
OpType OperatorNode<ExternalFileScan>::type_ = OpType::ExternalFileScan;
template <>
OpType OperatorNode<QueryDerivedScan>::type_ = OpType::QueryDerivedScan;
template <>
OpType OperatorNode<OrderBy>::type_ = OpType::OrderBy;
template <>
OpType OperatorNode<Distinct>::type_ = OpType::Distinct;
template <>
OpType OperatorNode<Limit>::type_ = OpType::Limit;
template <>
OpType OperatorNode<InnerNLJoin>::type_ = OpType::InnerNLJoin;
template <>
OpType OperatorNode<LeftNLJoin>::type_ = OpType::LeftNLJoin;
template <>
OpType OperatorNode<RightNLJoin>::type_ = OpType::RightNLJoin;
template <>
OpType OperatorNode<OuterNLJoin>::type_ = OpType::OuterNLJoin;
template <>
OpType OperatorNode<InnerHashJoin>::type_ = OpType::InnerHashJoin;
template <>
OpType OperatorNode<LeftHashJoin>::type_ = OpType::LeftHashJoin;
template <>
OpType OperatorNode<RightHashJoin>::type_ = OpType::RightHashJoin;
template <>
OpType OperatorNode<OuterHashJoin>::type_ = OpType::OuterHashJoin;
template <>
OpType OperatorNode<Insert>::type_ = OpType::Insert;
template <>
OpType OperatorNode<InsertSelect>::type_ = OpType::InsertSelect;
template <>
OpType OperatorNode<Delete>::type_ = OpType::Delete;
template <>
OpType OperatorNode<Update>::type_ = OpType::Update;
template <>
OpType OperatorNode<HashGroupBy>::type_ = OpType::HashGroupBy;
template <>
OpType OperatorNode<SortGroupBy>::type_ = OpType::SortGroupBy;
template <>
OpType OperatorNode<Aggregate>::type_ = OpType::Aggregate;
template <>
OpType OperatorNode<ExportExternalFile>::type_ = OpType::ExportExternalFile;

}  // namespace terrier::optimizer
