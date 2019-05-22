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
 * Logical operator for get
 */
class LogicalGet : public OperatorNode<LogicalGet> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param predicates predicates for get
   * @param table_alias alias of table to get from
   * @param is_for_update whether the scan is used for update
   * @return
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> predicates,
                       std::string table_alias, bool is_for_update);

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
   * Predicates for get
   */
  std::vector<AnnotatedExpression> predicates_;

  /**
   * Alias of the table to get from
   */
  std::string table_alias_;

  /**
   * Whether the scan is used for update
   */
  bool is_for_update_;
};

/**
 * Logical operator for external file get
 */
class LogicalExternalFileGet : public OperatorNode<LogicalExternalFileGet> {
 public:
  /**
   * @param format file format
   * @param file_name file name
   * @param delimiter character used as delimiter
   * @param quote character used for quotation
   * @param escape character used for escape sequences
   * @return an LogicalExternalFileGet operator
   */
  static Operator make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
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
 * Logical operator for query derived get (get on result sets of subqueries)
 */
class LogicalQueryDerivedGet : public OperatorNode<LogicalQueryDerivedGet> {
 public:
  /**
   * @param table_alias alias of the table
   * @param alias_to_expr_map map from table aliases to expressions of those tables
   * @return a QueryDerivedGet operator
   */
  static Operator make(
      std::string table_alias,
      std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> &&alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
  /**
   * Table aliases
   */
  std::string table_alias_;

  /**
   * Map from table aliases to expressions
   */
  std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> alias_to_expr_map_;
};

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
class LogicalFilter : public OperatorNode<LogicalFilter> {
 public:
  static Operator make(std::vector<AnnotatedExpression> &filter);
  std::vector<AnnotatedExpression> predicates;

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;
};

//===--------------------------------------------------------------------===//
// Project
//===--------------------------------------------------------------------===//
class LogicalProjection : public OperatorNode<LogicalProjection> {
 public:
  static Operator make(std::vector<std::shared_ptr<expression::AbstractExpression>> &elements);
  std::vector<std::shared_ptr<expression::AbstractExpression>> expressions;
};

//===--------------------------------------------------------------------===//
// DependentJoin
//===--------------------------------------------------------------------===//
class LogicalDependentJoin : public OperatorNode<LogicalDependentJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
class LogicalMarkJoin : public OperatorNode<LogicalMarkJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
class LogicalSingleJoin : public OperatorNode<LogicalSingleJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
class LogicalInnerJoin : public OperatorNode<LogicalInnerJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
class LogicalLeftJoin : public OperatorNode<LogicalLeftJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
class LogicalRightJoin : public OperatorNode<LogicalRightJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
class LogicalOuterJoin : public OperatorNode<LogicalOuterJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
class LogicalSemiJoin : public OperatorNode<LogicalSemiJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// GroupBy
//===--------------------------------------------------------------------===//
class LogicalAggregateAndGroupBy : public OperatorNode<LogicalAggregateAndGroupBy> {
 public:
  static Operator make();

  static Operator make(std::vector<std::shared_ptr<expression::AbstractExpression>> &columns);

  static Operator make(std::vector<std::shared_ptr<expression::AbstractExpression>> &columns,
                       std::vector<AnnotatedExpression> &having);

  bool operator==(const BaseOperatorNode &r) override;
  hash_t Hash() const override;

  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  std::vector<AnnotatedExpression> having;
};

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
class LogicalInsert : public OperatorNode<LogicalInsert> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       const std::vector<std::string> *columns,
                       const std::vector<std::vector<std::unique_ptr<expression::AbstractExpression>>> *values);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::string> *columns;
  const std::vector<std::vector<std::unique_ptr<expression::AbstractExpression>>> *values;
};

class LogicalInsertSelect : public OperatorNode<LogicalInsertSelect> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// LogicalDistinct
//===--------------------------------------------------------------------===//
class LogicalDistinct : public OperatorNode<LogicalDistinct> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// LogicalLimit
//===--------------------------------------------------------------------===//
class LogicalLimit : public OperatorNode<LogicalLimit> {
 public:
  static Operator make(int64_t offset, int64_t limit, std::vector<expression::AbstractExpression *> &&sort_exprs,
                       std::vector<bool> &&sort_ascending);
  int64_t offset;
  int64_t limit;
  // When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
  // We'll let the limit operator keep the order by clause's content as an
  // internal order, then the limit operator will generate sort plan with
  // limit as a optimization.
  std::vector<expression::AbstractExpression *> sort_exprs;
  std::vector<bool> sort_ascending;
};

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
class LogicalDelete : public OperatorNode<LogicalDelete> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
class LogicalUpdate : public OperatorNode<LogicalUpdate> {
 public:
  static Operator make(std::shared_ptr<catalog::TableCatalogEntry> target_table,
                       const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::unique_ptr<parser::UpdateClause>> *updates;
};

//===--------------------------------------------------------------------===//
// Export to external file
//===--------------------------------------------------------------------===//
class LogicalExportExternalFile : public OperatorNode<LogicalExportExternalFile> {
 public:
  static Operator make(ExternalFileFormat format, std::string file_name, char delimiter, char quote, char escape);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
};

/**
 * Physical operator for SELECT without FROM (e.g. SELECT 1;)
 */
class TableFreeScan : public OperatorNode<TableFreeScan> {
 public:
  /**
   * @return a TableFreeScan operator
   */
  static Operator make();
};

/**
 * Physical operator for sequential scan
 */
class SeqScan : public OperatorNode<SeqScan> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param table_alias alias of the table
   * @param predicates predicates for get
   * @param is_for_update whether the scan is used for update
   * @return a SeqScan operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::string table_alias,
                       std::vector<AnnotatedExpression> predicates, bool is_for_update);

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
   * Query predicates
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
 * Physical operator for index scan
 */
class IndexScan : public OperatorNode<IndexScan> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param index_oid OID of the index
   * @param table_alias alias of the table
   * @param predicates query predicates
   * @param is_for_update whether the scan is used for update
   * @param key_column_id_list OID of key columns
   * @param expr_type_list expression types
   * @param value_list values to be scanned
   * @return an IndexScan operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::index_oid_t index_oid, std::string table_alias,
                       std::vector<AnnotatedExpression> predicates, bool is_for_update,
                       std::vector<catalog::col_oid_t> key_column_oid_list,
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
   * Query predicates
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
  std::vector<catalog::col_oid_t> key_column_oid_list_;

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
 * Physucal operator for external file scan
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

 private:
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
 * Physical operator for query derived scan (scan on result sets of subqueries)
 */
class QueryDerivedScan : public OperatorNode<QueryDerivedScan> {
 public:
  /**
   * @param table_alias alias of the table
   * @param alias_to_expr_map map from table aliases to expressions of those tables
   * @return a QueryDerivedScan operator
   */
  static Operator make(
      std::string table_alias,
      std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> &&alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
  /**
   * Table aliases
   */
  std::string table_alias_;

  /**
   * Map from table aliases to expressions
   */
  std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> alias_to_expr_map_;
};

/**
 * Physical operator for ORDER BY
 */
class OrderBy : public OperatorNode<OrderBy> {
 public:
  /**
   * @return an OrderBy operator
   */
  static Operator make();
};

/**
 * Physical operator for LIMIT
 */
class Limit : public OperatorNode<Limit> {
 public:
  /**
   * @param offset number of offset rows to skip
   * @param limit number of rows to return
   * @param sort_columns columns on which to sort
   * @param sort_ascending sorting order
   * @return a Limit operator
   */
  static Operator make(int64_t offset, int64_t limit, std::vector<parser::AbstractExpression *> sort_columns,
                       std::vector<bool> sort_ascending);

 private:
  /**
   * Number of offset rows to skip
   */
  int64_t offset_;

  /**
   * Number of rows to return
   */
  int64_t limit_;

  /**
   * When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
   * We'll let the limit operator keep the order by clause's content as an
   * internal order, then the limit operator will generate sort plan with
   * limit as a optimization.
   */

  /**
   * Columns on which to sort
   */
  std::vector<parser::AbstractExpression *> sort_exprs_;

  /**
   * Sorting order
   */
  std::vector<bool> sort_acsending_;
};

/**
 * Physical operator for inner nested loop join
 */
class InnerNLJoin : public OperatorNode<InnerNLJoin> {
 public:
  /**
   * @param join_predicates predicates for join
   * @param left_keys left keys to join
   * @param right_keys right keys to join
   * @return an IneerNLJoin operator
   */
  static Operator make(std::vector<AnnotatedExpression> join_predicates,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
  /**
   * Left join keys
   */
  std::vector<std::unique_ptr<parser::AbstractExpression>> left_keys_;

  /**
   * Right join keys
   */
  std::vector<std::unique_ptr<parser::AbstractExpression>> right_keys_;

  /**
   * Predicates for join
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Physical operator for left outer nested loop join
 */
class LeftNLJoin : public OperatorNode<LeftNLJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a LeftNLJoin operator
   */
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);

 private:
  /**
   * Predicate for join
   */
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

/**
 * Physical operator for right outer nested loop join
 */
class RightNLJoin : public OperatorNode<RightNLJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a RightNLJoin operator
   */
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);

 private:
  /**
   * Predicate for join
   */
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

/**
 * Physical operator for full outer nested loop join
 */
class OuterNLJoin : public OperatorNode<OuterNLJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a OuterNLJoin operator
   */
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);

 private:
  /**
   * Predicate for join
   */
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

/**
 * Physical operator for inner hash join
 */
class InnerHashJoin : public OperatorNode<InnerHashJoin> {
 public:
  /**
   * @param join_predicates predicates for join
   * @param left_keys left keys to join
   * @param right_keys right keys to join
   * @return an IneerNLJoin operator
   */
  static Operator make(std::vector<AnnotatedExpression> join_predicates,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&left_keys,
                       std::vector<std::unique_ptr<parser::AbstractExpression>> &&right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * Left join keys
   */
  std::vector<std::unique_ptr<parser::AbstractExpression>> left_keys_;

  /**
   * Right join keys
   */
  std::vector<std::unique_ptr<parser::AbstractExpression>> right_keys_;

  /**
   * Predicate for join
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Physical operator for left outer hash join
 */
class LeftHashJoin : public OperatorNode<LeftHashJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a LeftHashJoin operator
   */
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);

 private:
  /**
   * Predicate for join
   */
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

/**
 * Physical operator for right outer hash join
 */
class RightHashJoin : public OperatorNode<RightHashJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a RightHashJoin operator
   */
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);

 private:
  /**
   * Predicate for join
   */
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

/**
 * Physical operator for full outer hash join
 */
class OuterHashJoin : public OperatorNode<OuterHashJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a OuterHashJoin operator
   */
  static Operator make(std::shared_ptr<parser::AbstractExpression> join_predicate);

 private:
  /**
   * Predicate for join
   */
  std::shared_ptr<parser::AbstractExpression> join_predicate_;
};

/**
 * Physical operator for INSERT
 */
class Insert : public OperatorNode<Insert> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param columns OIDs of columns to insert into
   * @param values expressions of values to insert
   * @return an Insert operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, const std::vector<catalog::col_oid_t> *columns,
                       const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values);

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid;

  /**
   * OID of the table
   */
  catalog::table_oid_t table_oid_;

  /**
   * Columns to insert into
   */
  const std::vector<catalog::col_oid_t> *columns_;

  /**
   * Expressions of values to insert
   */
  const std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> *values_;
};

/**
 * Physical operator for INSERT INTO ... SELECT ...
 */
class InsertSelect : public OperatorNode<InsertSelect> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @return an InsertSelect operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid);

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
};

/**
 * Physical operator for DELETE
 */
class Delete : public OperatorNode<Delete> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param delete_condition expression of delete condition
   * @return an InsertSelect operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::shared_ptr<parser::AbstractExpression> delete_condition);

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
   * Expression of delete condition
   */
  std::shared_ptr<parser::AbstractExpression> delete_condition_;
};

/**
 * Physical operator for export external file
 */
class ExportExternalFile : public OperatorNode<ExportExternalFile> {
 public:
  /**
   * @param format file format
   * @param file_name file name
   * @param delimiter character used as delimiter
   * @param quote character used for quotation
   * @param escape character used for escape sequences
   * @return an ExportExternalFile operator
   */
  static Operator make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

 private:
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
 * Physical operator for UPDATE
 */
class Update : public OperatorNode<Update> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param updates update clause
   * @return an Update operator
   */
  static Operator make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid,
                       const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid;

  /**
   * OID of the table
   */
  catalog::table_oid_t table_oid_;

  /**
   * Update clauses
   */
  const std::vector<std::unique_ptr<parser::UpdateClause>> *updates;
};

/**
 * Physical operator for GROUP BY using hashing
 */
class HashGroupBy : public OperatorNode<HashGroupBy> {
 public:
  /**
   * @param columns columns to group by
   * @param having expression of HAVING clause
   * @return a HashGroupBy operator
   */
  static Operator make(std::vector<std::shared_ptr<parser::AbstractExpression>> columns,
                       std::vector<AnnotatedExpression> having);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * Columns to group by
   */
  std::vector<std::shared_ptr<parser::AbstractExpression>> columns_;

  /**
   * Expression of HAVING clause
   */
  std::vector<AnnotatedExpression> having_;
};

/**
 * Physical operator for GROUP BY using sorting
 */
class SortGroupBy : public OperatorNode<SortGroupBy> {
 public:
  /**
   * @param columns columns to group by
   * @param having HAVING clause
   * @return a SortGroupBy operator
   */
  static Operator make(std::vector<std::shared_ptr<parser::AbstractExpression>> columns,
                       std::vector<AnnotatedExpression> having);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * Columns to group by
   */
  std::vector<std::shared_ptr<parser::AbstractExpression>> columns_;

  /**
   * Expression of HAVING clause
   */
  std::vector<AnnotatedExpression> having_;
};

/**
 * Physical operator for aggregate functions
 */
class Aggregate : public OperatorNode<Aggregate> {
 public:
  /**
   * @return an Aggregate operator
   */
  static Operator make();
};

/**
 * Physical operator for DISTINCT
 */
class Distinct : public OperatorNode<Distinct> {
 public:
  /**
   * @return a distinct operator
   */
  static Operator make();
};

}  // namespace optimizer
}  // namespace terrier
