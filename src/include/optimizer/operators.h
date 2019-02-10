#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "optimizer/operator_node.h"
#include "parser/expression_defs.h"
#include "parser/parser_defs.h"
#include "type/value.h"

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
// Insert
//===--------------------------------------------------------------------===//
class Insert : public OperatorNode<Insert> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       std::vector<catalog::index_oid_t> &&target_index, const std::vector<std::string> *columns,
                       const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values);
  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  std::vector<catalog::index_oid_t> target_index;
  const std::vector<std::string> *columns;
  const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values;
};

//===--------------------------------------------------------------------===//
// InsertSelect
//===--------------------------------------------------------------------===//
class InsertSelect : public OperatorNode<InsertSelect> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       std::vector<catalog::index_oid_t> &&target_index);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  std::vector<catalog::index_oid_t> target_index;
};

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
class Delete : public OperatorNode<Delete> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       std::vector<catalog::index_oid_t> &&target_index);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  std::vector<catalog::index_oid_t> target_index;
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
                       std::vector<catalog::index_oid_t> &&target_index,
                       const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  std::vector<catalog::index_oid_t> target_index;
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

}  // namespace optimizer
}  // namespace terrier
