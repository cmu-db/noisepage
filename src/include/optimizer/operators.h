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
#include "type/transient_value.h"

namespace terrier {

namespace parser {
class AbstractExpression;
class UpdateClause;
}  // namespace parser

namespace optimizer {

/**
 * Operator for SELECT without FROM (e.g. SELECT 1;)
 */
class TableFreeScan : public OperatorNode<TableFreeScan> {
 public:
  /**
   * @return a TableFreeScan operator
   */
  static Operator make();
};

/**
 * Operator for sequential scan
 */
class SeqScan : public OperatorNode<SeqScan> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param table_alias alias of the table
   * @param predicates predicates for get
   * @param update whether the scan is used for update
   * @return a SeqScan operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::string table_alias,
                       std::vector<AnnotatedExpression> predicates, bool update);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * OID of the table
   */
  catalog::table_oid_t table_oid_;

  /**
   * SELECT predicates
   */
  std::vector<AnnotatedExpression> predicates_;

  /**
   * Table alias
   */
  std::string table_alias_;

  /**
   * Whether the scan is used for update
   */
  bool is_for_update_;
};

/**
 * Operator for index scan
 */
class IndexScan : public OperatorNode<IndexScan> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param index_oid OID of the index
   * @param table_alias
   * @param predicates
   * @param update
   * @param key_column_id_list
   * @param expr_type_list
   * @param value_list
   * @return an IndexScan operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::index_oid_t index_oid, std::string table_alias,
                       std::vector<AnnotatedExpression> predicates, bool update,
                       std::vector<catalog::col_oid_t> key_column_id_list,
                       std::vector<parser::ExpressionType> expr_type_list,
                       std::vector<type::TransientValue> value_list);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * OID of the index
   */
  catalog::index_oid_t index_oid_;

  /**
   * SELECT predicates
   */
  std::vector<AnnotatedExpression> predicates_;

  /**
   * Table alias
   */
  std::string table_alias_;

  /**
   * Whether the scan is used for update
   */
  bool is_for_update_;

  /**
   * OIDs of key columns
   */
  std::vector<catalog::col_oid_t> key_column_id_list_;

  /**
   * Expression types
   */
  std::vector<parser::ExpressionType> expr_type_list_;

  /**
   * Parameter values
   */
  std::vector<type::TransientValue> value_list_;
};

/**
 * Operator for external file scan
 */
class ExternalFileScan : public OperatorNode<ExternalFileScan> {
 public:
  /**
   * @param format file format
   * @param file_name file name
   * @param delimiter character used as delimiter
   * @param quote character used for quotation
   * @param escape character used for escape sequences
   * @return an ExternalFileScan operator
   */
  static Operator make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * File format
   */
  parser::ExternalFileFormat format_;

  /**
   * File name
   */
  std::string file_name_;

  /**
   * Character used as delimiter
   */
  char delimiter_;

  /**
   * Character used for quotation
   */
  char quote_;

  /**
   * Character used for escape sequences
   */
  char escape_;
};

/**
 * Operator for query derived scan (scan on result sets of subqueries)
 */
class QueryDerivedScan : public OperatorNode<QueryDerivedScan> {
 public:
  /**
   * @param database_oid
   * @param namespace_oid
   * @param table_oid
   * @param table_alias
   * @param alias_to_expr_map
   * @return a QueryDerivedScan operator
   */
  static Operator make(
      catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
      std::string table_alias,
      std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> &&alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  catalog::db_oid_t database_oid_;
  catalog::namespace_oid_t namespace_oid_;
  catalog::table_oid_t table_oid_;
  std::string table_alias_;
  std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> alias_to_expr_map_;
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
  int64_t offset_;
  int64_t limit_;
  // When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
  // We'll let the limit operator keep the order by clause's content as an
  // internal order, then the limit operator will generate sort plan with
  // limit as a optimization.
  std::vector<parser::AbstractExpression *> sort_exprs_;
  std::vector<bool> sort_acsending_;
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

  std::vector<std::unique_ptr<parser::AbstractExpression>> left_keys_;
  std::vector<std::unique_ptr<parser::AbstractExpression>> right_keys_;

  std::vector<AnnotatedExpression> join_predicates_;
};

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
class LeftNLJoin : public OperatorNode<LeftNLJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
class RightNLJoin : public OperatorNode<RightNLJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
class OuterNLJoin : public OperatorNode<OuterNLJoin> {
 public:
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
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

  std::vector<std::unique_ptr<parser::AbstractExpression>> left_keys_;
  std::vector<std::unique_ptr<parser::AbstractExpression>> right_keys_;

  std::vector<AnnotatedExpression> join_predicates_;
};

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
class LeftHashJoin : public OperatorNode<LeftHashJoin> {
 public:
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
class RightHashJoin : public OperatorNode<RightHashJoin> {
 public:
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
class OuterHashJoin : public OperatorNode<OuterHashJoin> {
 public:
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
class Insert : public OperatorNode<Insert> {
 public:
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids,
                       const std::vector<std::string> *columns,
                       const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values);
  catalog::db_oid_t database_oid_;
  catalog::namespace_oid_t namespace_oid;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::index_oid_t> index_oids_;

  const std::vector<std::string> *columns_;
  const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values_;
};

//===--------------------------------------------------------------------===//
// InsertSelect
//===--------------------------------------------------------------------===//
class InsertSelect : public OperatorNode<InsertSelect> {
 public:
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids);

  catalog::db_oid_t database_oid_;
  catalog::namespace_oid_t namespace_oid;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::index_oid_t> index_oids_;
  std::vector<catalog::index_oid_t> target_index;
};

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
class Delete : public OperatorNode<Delete> {
 public:
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids);

  catalog::db_oid_t database_oid_;
  catalog::namespace_oid_t namespace_oid;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::index_oid_t> index_oids_;
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

  parser::ExternalFileFormat format_;
  std::string file_name_;
  char delimiter_;
  char quote_;
  char escape_;
};

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
class Update : public OperatorNode<Update> {
 public:
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids,
                       const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  catalog::db_oid_t database_oid_;
  catalog::namespace_oid_t namespace_oid;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::index_oid_t> index_oids_;
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

  std::vector<std::shared_ptr<parser::AbstractExpression>> columns_;
  std::vector<AnnotatedExpression> having_;
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

  std::vector<std::shared_ptr<parser::AbstractExpression>> columns_;
  std::vector<AnnotatedExpression> having_;
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
