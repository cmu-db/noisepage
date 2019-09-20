#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "common/managed_pointer.h"
#include "optimizer/operator_node.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "parser/parser_defs.h"
#include "parser/update_statement.h"
#include "planner/plannodes/plan_node_defs.h"
#include "type/transient_value.h"

namespace terrier {

namespace catalog {
class TableCatalogEntry;
}  // namespace catalog

namespace parser {
class AbstractExpression;
class UpdateClause;
}  // namespace parser

namespace optimizer {

/**
 * Physical operator for SELECT without FROM (e.g. SELECT 1;)
 */
class TableFreeScan : public OperatorNode<TableFreeScan> {
 public:
  /**
   * @return a TableFreeScan operator
   */
  static Operator Make();

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;
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
   * @param predicates predicates for get
   * @param table_alias alias of the table
   * @param is_for_update whether the scan is used for update
   * @return a SeqScan operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> &&predicates,
                       std::string table_alias, bool is_for_update);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return the OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOID() const { return database_oid_; }

  /**
   * @return the OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOID() const { return namespace_oid_; }

  /**
   * @return the OID of the table
   */
  const catalog::table_oid_t &GetTableOID() const { return table_oid_; }

  /**
   * @return the vector of predicates for get
   */
  const std::vector<AnnotatedExpression> &GetPredicates() const { return predicates_; }

  /**
   * @return the alias of the table to get from
   */
  const std::string &GetTableAlias() const { return table_alias_; }

  /**
   * @return whether the get operation is used for update
   */
  bool GetIsForUpdate() const { return is_for_update_; }

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
   * @param predicates query predicates
   * @param table_alias alias of the table
   * @param is_for_update whether the scan is used for update
   * @param key_column_oid_list OID of key columns
   * @param expr_type_list expression types
   * @param value_list values to be scanned
   * @return an IndexScan operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::index_oid_t index_oid, std::vector<AnnotatedExpression> &&predicates,
                       std::string table_alias, bool is_for_update,
                       std::vector<catalog::col_oid_t> &&key_column_oid_list,
                       std::vector<parser::ExpressionType> &&expr_type_list,
                       std::vector<type::TransientValue> &&value_list);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return the OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOID() const { return database_oid_; }

  /**
   * @return the OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOID() const { return namespace_oid_; }

  /**
   * @return the OID of the table
   */
  const catalog::index_oid_t &GetIndexOID() const { return index_oid_; }

  /**
   * @return the vector of predicates for get
   */
  const std::vector<AnnotatedExpression> &GetPredicates() const { return predicates_; }

  /**
   * @return the alias of the table to get from
   */
  const std::string &GetTableAlias() const { return table_alias_; }

  /**
   * @return whether the get operation is used for update
   */
  bool GetIsForUpdate() const { return is_for_update_; }

  /**
   * @return List of OIDs of key columns
   */
  const std::vector<catalog::col_oid_t> &GetKeyColumnOIDList() const { return key_column_oid_list_; }

  /**
   * @return List of expression types
   */
  const std::vector<parser::ExpressionType> &GetExprTypeList() const { return expr_type_list_; }

  /**
   * @return List of parameter values
   */
  const std::vector<type::TransientValue> &GetValueList() const { return value_list_; }

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
 * Physical operator for external file scan
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
  static Operator Make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return how the data should be formatted
   */
  const parser::ExternalFileFormat &GetFormat() const { return format_; }

  /**
   * @return the local file path to read the data
   */
  const std::string &GetFilename() const { return file_name_; }

  /**
   * @return the character to use to split each attribute
   */
  char GetDelimiter() const { return delimiter_; }

  /**
   * @return the character to use to 'quote' each value
   */
  char GetQuote() const { return quote_; }

  /**
   * @return the character to use to escape characters in values
   */
  char GetEscape() const { return escape_; }

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
  static Operator Make(
      std::string table_alias,
      std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Alias of the table to get from
   */
  const std::string &GetTableAlias() const { return table_alias_; }

  /**
   * @return map from table aliases to expressions
   */
  const std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &GetAliasToExprMap() const {
    return alias_to_expr_map_;
  }

 private:
  /**
   * Table aliases
   */
  std::string table_alias_;

  /**
   * Map from table aliases to expressions
   */
  std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> alias_to_expr_map_;
};

/**
 * Physical operator for ORDER BY
 */
class OrderBy : public OperatorNode<OrderBy> {
 public:
  /**
   * @return an OrderBy operator
   */
  static Operator Make();

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;
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
   * @param sort_directions sorting order
   * @return a Limit operator
   */
  static Operator Make(size_t offset, size_t limit,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_columns,
                       std::vector<planner::OrderByOrderingType> &&sort_directions);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return offset of the LIMIT operator
   */
  size_t GetOffset() const { return offset_; }

  /**
   * @return the max # of tuples to produce
   */
  size_t GetLimit() const { return limit_; }

  /**
   * @return inlined ORDER BY expressions (can be empty)
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetSortExpressions() const {
    return sort_exprs_;
  }

  /**
   * @return sorting orders (if ascending)
   */
  const std::vector<planner::OrderByOrderingType> &GetSortAscending() const { return sort_directions_; }

 private:
  /**
   * Number of offset rows to skip
   */
  size_t offset_;

  /**
   * Number of rows to return
   */
  size_t limit_;

  /**
   * When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
   * We'll let the limit operator keep the order by clause's content as an
   * internal order, then the limit operator will generate sort plan with
   * limit as a optimization.
   */

  /**
   * Columns on which to sort
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs_;

  /**
   * Sorting order
   */
  std::vector<planner::OrderByOrderingType> sort_directions_;
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
   * @return an InnerNLJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Left join keys
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetLeftKeys() const { return left_keys_; }

  /**
   * @return Right join keys
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetRightKeys() const { return right_keys_; }

  /**
   * @return Predicates for the Join
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Left join keys
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys_;

  /**
   * Right join keys
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys_;

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
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicate for the join
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Predicate for join
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicate for the join
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Predicate for join
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicate for the join
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Predicate for join
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Left join keys
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetLeftKeys() const { return left_keys_; }

  /**
   * @return Right join keys
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetRightKeys() const { return right_keys_; }

  /**
   * @return Predicates for the Join
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Left join keys
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys_;

  /**
   * Right join keys
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys_;

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
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicate for the join
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Predicate for join
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicate for the join
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Predicate for join
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicate for the join
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Predicate for join
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
   * @param index_oids the OIDs of the indexes to insert into
   * @return an Insert operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&columns,
                       std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &&values,
                       std::vector<catalog::index_oid_t> &&index_oids);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

  /**
   * @return Columns to insert into
   */
  const std::vector<catalog::col_oid_t> &GetColumns() const { return columns_; }

  /**
   * @return Expressions of values to insert
   */
  const std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &GetValues() const {
    return values_;
  }

  /**
   * @return Index oids to insert into
   */
  const std::vector<catalog::index_oid_t> &GetIndexes() const { return index_oids_; }

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
   * Columns to insert into
   */
  std::vector<catalog::col_oid_t> columns_;

  /**
   * Expressions of values to insert
   */
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> values_;

  /**
   * Indexes to insert into
   */
  std::vector<catalog::index_oid_t> index_oids_;
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
   * @param index_oids the OIDs of the indexes to insert into
   * @return an InsertSelect operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::index_oid_t> &&index_oids);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

  /**
   * @return Index oids to insert into
   */
  const std::vector<catalog::index_oid_t> &GetIndexes() const { return index_oids_; }

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
   * Indexes to insert into
   */
  std::vector<catalog::index_oid_t> index_oids_;
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
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid,
                       common::ManagedPointer<parser::AbstractExpression> delete_condition);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

  /**
   * @return Expression of delete condition
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetDeleteCondition() const { return delete_condition_; }

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
  common::ManagedPointer<parser::AbstractExpression> delete_condition_;
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
  static Operator Make(parser::ExternalFileFormat format, std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return how the data should be formatted
   */
  const parser::ExternalFileFormat &GetFormat() const { return format_; }

  /**
   * @return the local file path to read the data
   */
  const std::string &GetFilename() const { return file_name_; }

  /**
   * @return the character to use to split each attribute
   */
  char GetDelimiter() const { return delimiter_; }

  /**
   * @return the character to use to 'quote' each value
   */
  char GetQuote() const { return quote_; }

  /**
   * @return the character to use to escape characters in values
   */
  char GetEscape() const { return escape_; }

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
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid,
                       std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

  /**
   * @return the update clauses from the SET portion of the query
   */
  const std::vector<common::ManagedPointer<parser::UpdateClause>> &GetUpdateClauses() const { return updates_; }

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
   * Update clauses
   */
  std::vector<common::ManagedPointer<parser::UpdateClause>> updates_;
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
  static Operator Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                       std::vector<AnnotatedExpression> &&having);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of columns
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetColumns() const { return columns_; }

  /**
   * @return vector of having expressions
   */
  const std::vector<AnnotatedExpression> &GetHaving() const { return having_; }

 private:
  /**
   * Columns to group by
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> columns_;

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
  static Operator Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                       std::vector<AnnotatedExpression> &&having);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of columns
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetColumns() const { return columns_; }

  /**
   * @return vector of having expressions
   */
  const std::vector<AnnotatedExpression> &GetHaving() const { return having_; }

 private:
  /**
   * Columns to group by
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> columns_;

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
  static Operator Make();

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;
};

/**
 * Physical operator for DISTINCT
 */
class Distinct : public OperatorNode<Distinct> {
 public:
  /**
   * @return a distinct operator
   */
  static Operator Make();

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;
};

}  // namespace optimizer
}  // namespace terrier
