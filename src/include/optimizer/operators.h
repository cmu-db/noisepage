#pragma once

#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "optimizer/operator_node.h"
#include "parser/expression_defs.h"
#include "parser/parser_defs.h"
#include "type/value.h"

#include <unordered_map>
#include <vector>

namespace terrier {

namespace parser {
class AbstractExpression;
class UpdateClause;
}  // namespace parser

namespace catalog {
class TableCatalogEntry;
}

namespace optimizer {
class PropertySort;
//===--------------------------------------------------------------------===//
// DummyScan
//===--------------------------------------------------------------------===//
class DummyScan : public OperatorNode<DummyScan> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
class SeqScan : public OperatorNode<SeqScan> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> table, std::string alias,
                       std::vector<AnnotatedExpression> predicates, bool update);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  std::vector<AnnotatedExpression> predicates;
  std::string table_alias;
  bool is_for_update;
  std::shared_ptr<catalog::TableCatalogEntry> table_;
};

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
class IndexScan : public OperatorNode<IndexScan> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> table, std::string alias,
                       std::vector<AnnotatedExpression> predicates, bool update, catalog::index_oid_t index_id,
                       std::vector<catalog::col_oid_t> key_column_id_list,
                       std::vector<parser::ExpressionType> expr_type_list, std::vector<type::Value> value_list);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  catalog::index_oid_t index_id;
  std::vector<AnnotatedExpression> predicates;
  std::string table_alias;
  bool is_for_update;
  std::shared_ptr<catalog::TableCatalogEntry> table_;

  std::vector<catalog::col_oid_t> key_column_id_list;
  std::vector<parser::ExpressionType> expr_type_list;
  std::vector<type::Value> value_list;
};

//===--------------------------------------------------------------------===//
// Physical external file scan
//===--------------------------------------------------------------------===//
class ExternalFileScan : public OperatorNode<ExternalFileScan> {
 public:
  static Operator make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  // identifier for all get operators
  parser::ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
};

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
class QueryDerivedScan : public OperatorNode<QueryDerivedScan> {
 public:
  static Operator make(std::string alias,
                       std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  std::string table_alias;
  std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> alias_to_expr_map;
};

//===--------------------------------------------------------------------===//
// OrderBy
//===--------------------------------------------------------------------===//
class OrderBy : public OperatorNode<OrderBy> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//
class Limit : public OperatorNode<Limit> {
 public:
  static Operator make(int64_t offset, int64_t limit, std::vector<parser::AbstractExpression *> sort_columns,
                       std::vector<bool> sort_ascending);
  int64_t offset;
  int64_t limit;
  // When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
  // We'll let the limit operator keep the order by clause's content as an
  // internal order, then the limit operator will generate sort plan with
  // limit as a optimization.
  std::vector<parser::AbstractExpression *> sort_exprs;
  std::vector<bool> sort_acsending;
};

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
class InnerNLJoin : public OperatorNode<InnerNLJoin> {
 public:
  static Operator make(std::vector<AnnotatedExpression> conditions,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  std::vector<std::unique_ptr<parser::AbstractExpression>> left_keys;
  std::vector<std::unique_ptr<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
class LeftNLJoin : public OperatorNode<LeftNLJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
class RightNLJoin : public OperatorNode<RightNLJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
class OuterNLJoin : public OperatorNode<OuterNLJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
class InnerHashJoin : public OperatorNode<InnerHashJoin> {
 public:
  static Operator make(std::vector<AnnotatedExpression> conditions,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  std::vector<std::unique_ptr<parser::AbstractExpression>> left_keys;
  std::vector<std::unique_ptr<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
class LeftHashJoin : public OperatorNode<LeftHashJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
class RightHashJoin : public OperatorNode<RightHashJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
class OuterHashJoin : public OperatorNode<OuterHashJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// PhysicalInsert
//===--------------------------------------------------------------------===//
class Insert : public OperatorNode<Insert> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       const std::vector<std::string> *columns,
                       const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::string> *columns;
  const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values;
};

class InsertSelect : public OperatorNode<InsertSelect> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
class Delete : public OperatorNode<Delete> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table);
  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// ExportExternalFile
//===--------------------------------------------------------------------===//
class ExportExternalFile : public OperatorNode<ExportExternalFile> {
 public:
  static Operator make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  parser::ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
};

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
class Update : public OperatorNode<Update> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::unique_ptr<parser::UpdateClause>> *updates;
};

//===--------------------------------------------------------------------===//
// HashGroupBy
//===--------------------------------------------------------------------===//
class HashGroupBy : public OperatorNode<HashGroupBy> {
 public:
  static Operator make(std::vector<std::shared_ptr<parser::AbstractExpression>> columns,
                       std::vector<AnnotatedExpression> having);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  std::vector<std::shared_ptr<parser::AbstractExpression>> columns;
  std::vector<AnnotatedExpression> having;
};

//===--------------------------------------------------------------------===//
// SortGroupBy
//===--------------------------------------------------------------------===//
class SortGroupBy : public OperatorNode<SortGroupBy> {
 public:
  static Operator make(std::vector<std::shared_ptr<parser::AbstractExpression>> columns,
                       std::vector<AnnotatedExpression> having);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  std::vector<std::shared_ptr<parser::AbstractExpression>> columns;
  std::vector<AnnotatedExpression> having;
};

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
class Aggregate : public OperatorNode<Aggregate> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Distinct
//===--------------------------------------------------------------------===//
class Distinct : public OperatorNode<Distinct> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
template <>
std::string OperatorNode<DummyScan>::name_ = "DummyScan";
template <>
std::string OperatorNode<SeqScan>::name_("SeqScan");
template <>
std::string OperatorNode<IndexScan>::name_("IndexScan");
template <>
std::string OperatorNode<ExternalFileScan>::name_ = "ExternalFileScan";
template <>
std::string OperatorNode<QueryDerivedScan>::name_ = "QueryDerivedScan";
template <>
std::string OperatorNode<OrderBy>::name_ = "OrderBy";
template <>
std::string OperatorNode<Limit>::name_ = "Limit";
template <>
std::string OperatorNode<InnerNLJoin>::name_ = "InnerNLJoin";
template <>
std::string OperatorNode<LeftNLJoin>::name_ = "LeftNLJoin";
template <>
std::string OperatorNode<RightNLJoin>::name_ = "RightNLJoin";
template <>
std::string OperatorNode<OuterNLJoin>::name_ = "OuterNLJoin";
template <>
std::string OperatorNode<InnerHashJoin>::name_ = "InnerHashJoin";
template <>
std::string OperatorNode<LeftHashJoin>::name_ = "LeftHashJoin";
template <>
std::string OperatorNode<RightHashJoin>::name_ = "RightHashJoin";
template <>
std::string OperatorNode<OuterHashJoin>::name_ = "OuterHashJoin";
template <>
std::string OperatorNode<Insert>::name_ = "Insert";
template <>
std::string OperatorNode<InsertSelect>::name_ = "InsertSelect";
template <>
std::string OperatorNode<Delete>::name_ = "Delete";
template <>
std::string OperatorNode<Update>::name_ = "Update";
template <>
std::string OperatorNode<HashGroupBy>::name_ = "HashGroupBy";
template <>
std::string OperatorNode<SortGroupBy>::name_ = "SortGroupBy";
template <>
std::string OperatorNode<Distinct>::name_ = "Distinct";
template <>
std::string OperatorNode<Aggregate>::name_ = "Aggregate";
template <>
std::string OperatorNode<ExportExternalFile>::name_ = "ExportExternalFile";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<DummyScan>::type_ = OpType::DummyScan;
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
OpType OperatorNode<Limit>::type_ = OpType::PhysicalLimit;
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

}  // namespace optimizer
}  // namespace terrier
