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
#include "parser/statements.h"
#include "planner/plannodes/plan_node_defs.h"
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
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<AnnotatedExpression> predicates,
                       std::string table_alias, bool is_for_update);

  /**
   * For select statement without a from table
   * @return
   */
  static Operator Make();

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return the OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return the OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return the OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

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
 * Logical operator for query derived get (get on result sets of subqueries)
 */
class LogicalQueryDerivedGet : public OperatorNode<LogicalQueryDerivedGet> {
 public:
  /**
   * @param table_alias alias of the table
   * @param alias_to_expr_map map from table aliases to expressions of those tables
   * @return a LogicalQueryDerivedGet operator
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
 * Logical operator to perform a filter during a scan
 */
class LogicalFilter : public OperatorNode<LogicalFilter> {
 public:
  /**
   * @param predicates The list of predicates used to perform the scan
   * @return a LogicalFilter operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of predicates
   */
  const std::vector<AnnotatedExpression> &GetPredicates() const { return predicates_; }

 private:
  /**
   * The list of predicates use to perform the scan.
   * Since this is a logical operator, the order of the predicates
   * in this list does not matter.
   */
  std::vector<AnnotatedExpression> predicates_;
};

/**
 * Logical operator for projections
 */
class LogicalProjection : public OperatorNode<LogicalProjection> {
 public:
  /**
   * @param expressions list of AbstractExpressions in the projection list.
   * @return a LogicalProjection operator
   */
  static Operator Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&expressions);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of predicates
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetExpressions() const { return expressions_; }

 private:
  /**
   * Each entry in the projection list is an AbstractExpression
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> expressions_;
};

/**
 * Logical operator for dependent join
 */
class LogicalDependentJoin : public OperatorNode<LogicalDependentJoin> {
 public:
  /**
   * @return a DependentJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return a DependentJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for mark join
 */
class LogicalMarkJoin : public OperatorNode<LogicalMarkJoin> {
 public:
  /**
   * @return a MarkJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return a MarkJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for single join
 */
class LogicalSingleJoin : public OperatorNode<LogicalSingleJoin> {
 public:
  /**
   * @return a SingleJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return a SingleJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for inner join
 */
class LogicalInnerJoin : public OperatorNode<LogicalInnerJoin> {
 public:
  /**
   * @return an InnerJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return an InnerJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for left join
 */
class LogicalLeftJoin : public OperatorNode<LogicalLeftJoin> {
 public:
  /**
   * @return a LeftJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return a LeftJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for right join
 */
class LogicalRightJoin : public OperatorNode<LogicalRightJoin> {
 public:
  /**
   * @return a RightJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return a RightJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for outer join
 */
class LogicalOuterJoin : public OperatorNode<LogicalOuterJoin> {
 public:
  /**
   * @return an OuterJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return an OuterJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for semi join
 */
class LogicalSemiJoin : public OperatorNode<LogicalSemiJoin> {
 public:
  /**
   * @return a SemiJoin operator
   */
  static Operator Make();

  /**
   * @param join_predicates conditions of the join
   * @return a SemiJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return vector of join predicates
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Join predicates
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Logical operator for aggregation or group by operation
 */
class LogicalAggregateAndGroupBy : public OperatorNode<LogicalAggregateAndGroupBy> {
 public:
  /**
   * @return a GroupBy operator
   */
  static Operator Make();

  /**
   * @param columns columns to group by
   * @return a GroupBy operator
   */
  static Operator Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns);

  /**
   * @param columns columns to group by
   * @param having HAVING clause
   * @return a GroupBy operator
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
 * Logical operation for an Insert
 */
class LogicalInsert : public OperatorNode<LogicalInsert> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param columns list of columns to insert into
   * @param values list of expressions that provide the values to insert into columns
   * @return
   */
  static Operator Make(
      catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
      std::vector<catalog::col_oid_t> &&columns,
      common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>> values);

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
   * @return OIDs of the columns that this operator is inserting into for the target table
   */
  const std::vector<catalog::col_oid_t> &GetColumns() const { return columns_; }

  /**
   * @return The expression objects to insert
   */
  const common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>>
      &GetValues() const {
    return values_;
  }

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
   * OIDs of the columns that this operator is inserting into for the target table
   */
  std::vector<catalog::col_oid_t> columns_;

  /**
   * The expression objects to insert.
   * The offset of an entry in this list corresponds to the offset in the columns_ list.
   */
  common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>> values_;
};

/**
 * Logical operator for an Insert that uses the output from a Select
 */
class LogicalInsertSelect : public OperatorNode<LogicalInsertSelect> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid);

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
 * Logical operator for DISTINCT
 */
class LogicalDistinct : public OperatorNode<LogicalDistinct> {
 public:
  /**
   * This generates the LogicalDistinct.
   * It doesn't need to store any data. It is just a placeholder
   * @return
   */
  static Operator Make();

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;
};

/**
 * Logical operator for LIMIT
 * This supports embedded ORDER BY information
 */
class LogicalLimit : public OperatorNode<LogicalLimit> {
 public:
  /**
   * @param offset offset of the LIMIT operator
   * @param limit the max # of tuples to produce
   * @param sort_exprs inlined ORDER BY expressions (can be empty)
   * @param sort_directions inlined sort directions (can be empty)
   * @return
   */
  static Operator Make(size_t offset, size_t limit,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&sort_exprs,
                       std::vector<optimizer::OrderByOrderingType> &&sort_directions);

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
   * @return inlined sort directions (can be empty)
   */
  const std::vector<optimizer::OrderByOrderingType> &GetSortDirections() const { return sort_directions_; }

 private:
  /**
   * The offset of the LIMIT operator
   */
  size_t offset_;

  /**
   * The number of tuples to include as defined by the LIMIT
   */
  size_t limit_;

  /**
   * When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5",
   * we'll let the limit operator keep the order by clause's content as an
   * internal order, then the limit operator will generate sort plan with
   * limit as a optimization.
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs_;

  /**
   * The sort direction of sort expressions
   */
  std::vector<optimizer::OrderByOrderingType> sort_directions_;
};

/**
 * Logical operator for Delete
 */
class LogicalDelete : public OperatorNode<LogicalDelete> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid);

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
 * Logical operator for Update
 */
class LogicalUpdate : public OperatorNode<LogicalUpdate> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param updates the update clauses from the SET portion of the query
   * @return
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
   * The update clauses from the SET portion of the query
   */
  std::vector<common::ManagedPointer<parser::UpdateClause>> updates_;
};

/**
 * Logical operator for exporting data to an external file
 */
class LogicalExportExternalFile : public OperatorNode<LogicalExportExternalFile> {
 public:
  /**
   * @param format how the data should be formatted
   * @param file_name the local file path to write the data
   * @param delimiter the character to use to split each attribute
   * @param quote the character to use to 'quote' each value
   * @param escape the character to use to escape characters in values
   * @return
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
   * @return the local file path to write the data
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
   * How the data should be formatted
   */
  parser::ExternalFileFormat format_;

  /**
   * The local file path to write the data
   * TODO: Switch this to std::filesystem::path when it becomes more widely available
   */
  std::string file_name_;

  /**
   * The character to use to split each attribute
   */
  char delimiter_;

  /**
   * The character to use to 'quote' each value
   */
  char quote_;

  /**
   * The character to use to escape characters in values that are the same as
   * either the delimiter or quote characeter.
   */
  char escape_;
};

/**
 * Logical operator for CreateDatabase
 */
class LogicalCreateDatabase : public OperatorNode<LogicalCreateDatabase> {
 public:
  /**
   * @return
   */
  static Operator Make(std::string database_name);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return the name of the database we want to create
   */
  const std::string &GetDatabaseName() const { return database_name_; }
 private:
  /**
   * Name of the new database
   */
  std::string database_name_;
};

/**
 * Logical operator for CreateFunction
 */
class LogicalCreateFunction : public OperatorNode<LogicalCreateFunction> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @return
   */
  static Operator Make(catalog::namespace_oid_t namespace_oid, std::string function_name, parser::PLType language, std::vector<std::string> &&function_body,  std::vector<std::string> &&function_param_names, std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types, parser::BaseFunctionParameter::DataType return_type, int param_count, bool replace);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return name of the user defined function
   */
  std::string GetFunctionName() const { return function_name_; }

  /**
   * @return language type of the user defined function
   */
  parser::PLType GetUDFLanguage() const { return language_; }

  /**
   * @return body of the user defined function
   */
  std::vector<std::string> GetFunctionBody() const { return function_body_; }

  /**
   * @return parameter names of the user defined function
   */
  std::vector<std::string> GetFunctionParameterNames() const { return function_param_names_; }

  /**
   * @return parameter types of the user defined function
   */
  std::vector<parser::BaseFunctionParameter::DataType> GetFunctionParameterTypes() const {
    return function_param_types_;
  }

  /**
   * @return return type of the user defined function
   */
  parser::BaseFunctionParameter::DataType GetReturnType() const { return return_type_; }

  /**
   * @return whether the definition of the user defined function needs to be replaced
   */
  bool IsReplace() const { return is_replace_; }

  /**
   * @return number of parameters of the user defined function
   */
  int GetParamCount() const { return param_count_; }

 private:
  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * Indicates the UDF language type
   */
  parser::PLType language_;

  /**
   * Function parameters names passed to the UDF
   */
  std::vector<std::string> function_param_names_;

  /**
   * Function parameter types passed to the UDF
   */
  std::vector<parser::BaseFunctionParameter::DataType> function_param_types_;

  /**
   * Query string/ function body of the UDF
   */
  std::vector<std::string> function_body_;

  /**
   * Indicates if the function definition needs to be replaced
   */
  bool is_replace_;

  /**
   * Function name of the UDF
   */
  std::string function_name_;

  /**
   * Return type of the UDF
   */
  parser::BaseFunctionParameter::DataType return_type_;

  /**
   * Number of parameters
   */
  int param_count_ = 0;
};

/**
 * Logical operator for CreateIndex
 */
class LogicalCreateIndex : public OperatorNode<LogicalCreateIndex> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @return
   */
  static Operator Make(catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid, parser::IndexType index_type, bool unique, std::string index_name, std::vector<common::ManagedPointer<parser::AbstractExpression>> index_attrs);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

  /**
   * @return Type of the index
   */
  const parser::IndexType &GetIndexType() const { return index_type_; }

  /**
   * @return Type of the index
   */
  const bool &IsUnique() const { return unique_index_; }

  /**
   * @return Name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

  /**
   * @return Type of the index
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetIndexAttr() const { return index_attrs_; }

 private:
  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * OID of the table
   */
  catalog::table_oid_t table_oid_;

  /**
   * Index type
   */
  parser::IndexType index_type_;

  /**
   * True if the index is unique
   */
  bool unique_index_;

  /**
   * Name of the Index
   */
  std::string index_name_;

  /**
   * Index attributes
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> index_attrs_;
};

/**
 * Logical operator for CreateTable
 */
class LogicalCreateTable : public OperatorNode<LogicalCreateTable> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @return
   */
  static Operator Make(catalog::namespace_oid_t namespace_oid, std::string table_name, std::vector<common::ManagedPointer<parser::ColumnDefinition>> && columns, std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&foreign_keys);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }
  /**
   * @return the name of the database we want to create
   */
  const std::string &GetTableName() const { return table_name_; }
  /**
   * @return the name of the database we want to create
   */
  const std::vector<common::ManagedPointer<parser::ColumnDefinition>> &GetColumns() const { return columns_; }
  /**
   * @return the name of the database we want to create
   */
  const std::vector<common::ManagedPointer<parser::ColumnDefinition>> &GetForeignKeys() const { return foreign_keys_; }
 private:
  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * Table Name
   */
   std::string table_name_;

  /**
   * Vector of column definitions of the new table
   */
   std::vector<common::ManagedPointer<parser::ColumnDefinition>> columns_;

  /**
   * Vector of foreign key references of the new table
   */
   std::vector<common::ManagedPointer<parser::ColumnDefinition>> foreign_keys_;
};

/**
 * Logical operator for CreateSchema/Namespace
 */
class LogicalCreateNamespace : public OperatorNode<LogicalCreateNamespace> {
 public:
  /**
   * @return
   */
  static Operator Make(std::string namespace_name);

  bool operator==(const BaseOperatorNode &r) override;
  common::hash_t Hash() const override;

  /**
   * @return the name of the namespace we want to create
   */
  const std::string &GetNamespaceName() const { return namespace_name_; }
 private:
  /**
   * Name of the new namespace
   */
  std::string namespace_name_;
};

/**
 * Logical operator for CreateTrigger
 */
class LogicalCreateTrigger : public OperatorNode<LogicalCreateTrigger> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::string trigger_name, std::vector<std::string> &&trigger_funcnames, std::vector<std::string> &&trigger_args, std::vector<catalog::col_oid_t> &&trigger_columns, common::ManagedPointer<parser::AbstractExpression> &&trigger_when, int16_t trigger_type);

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
 * @return trigger name
 */
  std::string GetTriggerName() const { return trigger_name_; }

  /**
   * @return trigger function names
   */
  std::vector<std::string> GetTriggerFuncName() const { return trigger_funcnames_; }

  /**
   * @return trigger args
   */
  std::vector<std::string> GetTriggerArgs() const { return trigger_args_; }

  /**
   * @return trigger columns
   */
  std::vector<catalog::col_oid_t> GetTriggerColumns() const { return trigger_columns_; }

  /**
   * @return trigger when clause
   */
  common::ManagedPointer<parser::AbstractExpression> GetTriggerWhen() const {
    return common::ManagedPointer(trigger_when_);
  }

  /**
   * @return trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  int16_t GetTriggerType() const { return trigger_type_; }
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
   * Name of the trigger
   */
  std::string trigger_name_;

  /**
   * Names of the trigger functions
   */
  std::vector<std::string> trigger_funcnames_;

  /**
   * Trigger arguments
   */
  std::vector<std::string> trigger_args_;

  /**
   * Trigger columns
   */
  std::vector<catalog::col_oid_t> trigger_columns_;

  /**
   * Trigger when clause
   */
  common::ManagedPointer<parser::AbstractExpression> trigger_when_;

  /**
   * Type of trigger
   */
  int16_t trigger_type_ = 0;
};

/**
 * Logical operator for CreateView
 */
class LogicalCreateView : public OperatorNode<LogicalCreateView> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, std::string view_name, common::ManagedPointer<parser::SelectStatement> view_query);

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
   * @return view name
   */
  const std::string &GetViewName() const { return view_name_; }

  /**
   * @return view query
   */
  common::ManagedPointer<parser::SelectStatement> GetViewQuery() { return common::ManagedPointer(view_query_); }

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
   * Name of the view
   */
  std::string view_name_;

  /**
   * View query
   */
  common::ManagedPointer<parser::SelectStatement> view_query_;
};


}  // namespace optimizer
}  // namespace terrier
