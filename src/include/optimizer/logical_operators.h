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
   * @param conditions condition of the join
   * @return a DependentJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&conditions);

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
   * @param conditions conditions of the join
   * @return a MarkJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&conditions);

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
   * @param conditions conditions of the join
   * @return a SingleJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&conditions);

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
   * @param conditions conditions of the join
   * @return an InnerJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&conditions);

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
   * @param join_predicate condition of the join
   * @return a LeftJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return pointer to the join predicate expression
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Join predicate
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
};

/**
 * Logical operator for right join
 */
class LogicalRightJoin : public OperatorNode<LogicalRightJoin> {
 public:
  /**
   * @param join_predicate condition of the join
   * @return a RightJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return pointer to the join predicate expression
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Join predicate
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
};

/**
 * Logical operator for outer join
 */
class LogicalOuterJoin : public OperatorNode<LogicalOuterJoin> {
 public:
  /**
   * @param join_predicate condition of the join
   * @return an OuterJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return pointer to the join predicate expression
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Join predicate
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
};

/**
 * Logical operator for semi join
 */
class LogicalSemiJoin : public OperatorNode<LogicalSemiJoin> {
 public:
  /**
   * @param join_predicate condition of the join
   * @return a SemiJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  bool operator==(const BaseOperatorNode &r) override;

  common::hash_t Hash() const override;

  /**
   * @return pointer to the join predicate expression
   */
  const common::ManagedPointer<parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  /**
   * Join predicate
   */
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
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
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&columns,
                       std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &&values);

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
  const std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &GetValues() const {
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
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> values_;
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
   * @return inlined sort directions (can be empty)
   */
  const std::vector<planner::OrderByOrderingType> &GetSortDirections() const { return sort_directions_; }

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
  std::vector<planner::OrderByOrderingType> sort_directions_;
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

}  // namespace optimizer
}  // namespace terrier
