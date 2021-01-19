#pragma once

// THIS HEADER IS HUGE! DO NOT INCLUDE IT IN OTHER HEADERS!

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "common/managed_pointer.h"
#include "optimizer/operator_node_contents.h"
#include "parser/expression_defs.h"
#include "parser/parser_defs.h"
#include "parser/statements.h"
#include "parser/update_statement.h"
#include "planner/plannodes/plan_node_defs.h"

namespace noisepage {

namespace catalog {
class IndexSchema;
class Schema;
}  // namespace catalog

namespace parser {
class AbstractExpression;
class UpdateClause;
}  // namespace parser

namespace optimizer {

/**
 * Physical operator for SELECT without FROM (e.g. SELECT 1;)
 */
class TableFreeScan : public OperatorNodeContents<TableFreeScan> {
 public:
  /**
   * @return a TableFreeScan operator
   */
  static Operator Make();

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;
};

/**
 * Physical operator for sequential scan
 */
class SeqScan : public OperatorNodeContents<SeqScan> {
 public:
  /**
   * @param database_oid OID of the database
   * @param table_oid OID of the table
   * @param predicates predicates for get
   * @param table_alias alias of the table
   * @param is_for_update whether the scan is used for update
   * @return a SeqScan operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                       std::vector<AnnotatedExpression> &&predicates, std::string table_alias, bool is_for_update);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

  common::hash_t Hash() const override;

  /**
   * @return the OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOID() const { return database_oid_; }

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
class IndexScan : public OperatorNodeContents<IndexScan> {
 public:
  /**
   * @param database_oid OID of the database
   * @param tbl_oid OID of the table
   * @param index_oid OID of the index
   * @param predicates query predicates
   * @param is_for_update whether the scan is used for update
   * @param scan_type IndexScanType
   * @param bounds Bounds for IndexScan
   * @param cover_all_columns whether the index covers all indexable columns (that we support) in the predicates
   * @return an IndexScan operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::table_oid_t tbl_oid, catalog::index_oid_t index_oid,
                       std::vector<AnnotatedExpression> &&predicates, bool is_for_update,
                       planner::IndexScanType scan_type,
                       std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> bounds,
                       bool cover_all_columns);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

  common::hash_t Hash() const override;

  /**
   * @return the OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOID() const { return database_oid_; }

  /**
   * @return the OID of the table
   */
  const catalog::table_oid_t &GetTableOID() const { return tbl_oid_; }

  /**
   * @return the OID of the index
   */
  const catalog::index_oid_t &GetIndexOID() const { return index_oid_; }

  /**
   * @return the vector of predicates for get
   */
  const std::vector<AnnotatedExpression> &GetPredicates() const { return predicates_; }

  /**
   * @return whether the get operation is used for update
   */
  bool GetIsForUpdate() const { return is_for_update_; }

  /**
   * @return index scan type
   */
  planner::IndexScanType GetIndexScanType() const { return scan_type_; }

  /**
   * @return bounds
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> &GetBounds() const {
    return bounds_;
  }

  /**
   * @return whether the index covers all predicate columns
   */
  bool GetCoverAllColumns() const { return cover_all_columns_; }

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the table
   */
  catalog::table_oid_t tbl_oid_;

  /**
   * OID of the index
   */
  catalog::index_oid_t index_oid_;

  /**
   * Query predicates
   */
  std::vector<AnnotatedExpression> predicates_;

  /**
   * Whether the scan is used for update
   */
  bool is_for_update_;

  /**
   * Scan Type
   */
  planner::IndexScanType scan_type_;

  /**
   * Bounds
   */
  std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> bounds_;

  /**
   *
   * The index covers all indexable columns in the predicates
   */
  bool cover_all_columns_;
};

/**
 * Physical operator for external file scan
 */
class ExternalFileScan : public OperatorNodeContents<ExternalFileScan> {
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

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class QueryDerivedScan : public OperatorNodeContents<QueryDerivedScan> {
 public:
  /**
   * @param table_alias alias of the table
   * @param alias_to_expr_map map from table aliases to expressions of those tables
   * @return a QueryDerivedScan operator
   */
  static Operator Make(
      std::string table_alias,
      std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> &&alias_to_expr_map);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class OrderBy : public OperatorNodeContents<OrderBy> {
 public:
  /**
   * @return an OrderBy operator
   */
  static Operator Make();

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;
};

/**
 * Physical operator for LIMIT
 */
class Limit : public OperatorNodeContents<Limit> {
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
                       std::vector<optimizer::OrderByOrderingType> &&sort_directions);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
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
  const std::vector<optimizer::OrderByOrderingType> &GetSortAscending() const { return sort_directions_; }

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
  std::vector<optimizer::OrderByOrderingType> sort_directions_;
};

/**
 * Physical operator for inner index join
 */
class InnerIndexJoin : public OperatorNodeContents<InnerIndexJoin> {
 public:
  /**
   * @param tbl_oid Table OID
   * @param idx_oid Index OID
   * @param scan_type IndexScanType
   * @param join_keys Join Keys
   * @param join_predicates predicates for join
   * @return an InnerIndexJoin operator
   */
  static Operator Make(catalog::table_oid_t tbl_oid, catalog::index_oid_t idx_oid, planner::IndexScanType scan_type,
                       std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> join_keys,
                       std::vector<AnnotatedExpression> join_predicates);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Table OID
   */
  catalog::table_oid_t GetTableOID() const { return tbl_oid_; }

  /**
   * @return Index OID
   */
  catalog::index_oid_t GetIndexOID() const { return idx_oid_; }

  /**
   * @return scan type
   */
  planner::IndexScanType GetScanType() const { return scan_type_; }

  /**
   * @return Join Keys
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> &GetJoinKeys() const {
    return join_keys_;
  }

  /**
   * @return Predicates for the Join
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Table OID
   */
  catalog::table_oid_t tbl_oid_;

  /**
   * Index OID
   */
  catalog::index_oid_t idx_oid_;

  /**
   * Scan Type
   */
  planner::IndexScanType scan_type_;

  /**
   * Join Keys
   */
  std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> join_keys_;

  /**
   * Predicates for join
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Physical operator for inner nested loop join
 */
class InnerNLJoin : public OperatorNodeContents<InnerNLJoin> {
 public:
  /**
   * @param join_predicates predicates for join
   * @return an InnerNLJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

  common::hash_t Hash() const override;

  /**
   * @return Predicates for the Join
   */
  const std::vector<AnnotatedExpression> &GetJoinPredicates() const { return join_predicates_; }

 private:
  /**
   * Predicates for join
   */
  std::vector<AnnotatedExpression> join_predicates_;
};

/**
 * Physical operator for left outer nested loop join
 */
class LeftNLJoin : public OperatorNodeContents<LeftNLJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a LeftNLJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class RightNLJoin : public OperatorNodeContents<RightNLJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a RightNLJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class OuterNLJoin : public OperatorNodeContents<OuterNLJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a OuterNLJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class InnerHashJoin : public OperatorNodeContents<InnerHashJoin> {
 public:
  /**
   * @param join_predicates predicates for join
   * @param left_keys left keys to join
   * @param right_keys right keys to join
   * @return an InnerHashJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
 * Physical operator for left semi hash join
 */
class LeftSemiHashJoin : public OperatorNodeContents<LeftSemiHashJoin> {
 public:
  /**
   * @param join_predicates predicates for join
   * @param left_keys left keys to join
   * @param right_keys right keys to join
   * @return a LeftSemiHashJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class LeftHashJoin : public OperatorNodeContents<LeftHashJoin> {
 public:
  /**
   * @param join_predicates predicates for join
   * @param left_keys left keys to join
   * @param right_keys right keys to join
   * @return an LeftHashJoin operator
   */
  static Operator Make(std::vector<AnnotatedExpression> &&join_predicates,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_keys,
                       std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_keys);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
 * Physical operator for right outer hash join
 */
class RightHashJoin : public OperatorNodeContents<RightHashJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a RightHashJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class OuterHashJoin : public OperatorNodeContents<OuterHashJoin> {
 public:
  /**
   * @param join_predicate predicate for join
   * @return a OuterHashJoin operator
   */
  static Operator Make(common::ManagedPointer<parser::AbstractExpression> join_predicate);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class Insert : public OperatorNodeContents<Insert> {
 public:
  /**
   * @param database_oid OID of the database
   * @param table_oid OID of the table
   * @param columns OIDs of columns to insert into
   * @param values expressions of values to insert
   * @return an Insert operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                       std::vector<catalog::col_oid_t> &&columns,
                       std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> &&values);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

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

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

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
};

/**
 * Physical operator for INSERT INTO ... SELECT ...
 */
class InsertSelect : public OperatorNodeContents<InsertSelect> {
 public:
  /**
   * @param database_oid OID of the database
   * @param table_oid OID of the table
   * @return an InsertSelect operator
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

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
   * OID of the table
   */
  catalog::table_oid_t table_oid_;
};

/**
 * Physical operator for DELETE
 */
class Delete : public OperatorNodeContents<Delete> {
 public:
  /**
   * @param database_oid OID of the database
   * @param table_alias Alias of the table
   * @param table_oid OID of the table
   * @return an InsertSelect operator
   */
  static Operator Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return Alias
   */
  const std::string &GetTableAlias() const { return table_alias_; }

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
   * Table Alias
   */
  std::string table_alias_;

  /**
   * OID of the table
   */
  catalog::table_oid_t table_oid_;
};

/**
 * Physical operator for export external file
 */
class ExportExternalFile : public OperatorNodeContents<ExportExternalFile> {
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

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class Update : public OperatorNodeContents<Update> {
 public:
  /**
   * @param database_oid OID of the database
   * @param table_alias Table Alias
   * @param table_oid OID of the table
   * @param updates update clause
   * @return an Update operator
   */
  static Operator Make(catalog::db_oid_t database_oid, std::string table_alias, catalog::table_oid_t table_oid,
                       std::vector<common::ManagedPointer<parser::UpdateClause>> &&updates);
  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return Table Alias
   */
  const std::string &GetTableAlias() const { return table_alias_; }

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
   * Table Alias
   */
  std::string table_alias_;

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
class HashGroupBy : public OperatorNodeContents<HashGroupBy> {
 public:
  /**
   * @param columns columns to group by
   * @param having expression of HAVING clause
   * @return a HashGroupBy operator
   */
  static Operator Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                       std::vector<AnnotatedExpression> &&having);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class SortGroupBy : public OperatorNodeContents<SortGroupBy> {
 public:
  /**
   * @param columns columns to group by
   * @param having HAVING clause
   * @return a SortGroupBy operator
   */
  static Operator Make(std::vector<common::ManagedPointer<parser::AbstractExpression>> &&columns,
                       std::vector<AnnotatedExpression> &&having);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;

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
class Aggregate : public OperatorNodeContents<Aggregate> {
 public:
  /**
   * @return an Aggregate operator
   */
  static Operator Make();

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;
};

/**
 * Physical operator for CreateDatabase
 */
class CreateDatabase : public OperatorNodeContents<CreateDatabase> {
 public:
  /**
   * @param database_name Name of the database to be created
   * @return
   */
  static Operator Make(std::string database_name);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
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
 * Physical operator for CreateTable
 */
class CreateTable : public OperatorNodeContents<CreateTable> {
 public:
  /**
   * @param namespace_oid OID of the namespace
   * @param table_name Name of the table to be created
   * @param columns Vector of definitions of the columns in the new table
   * @param foreign_keys Vector of definitions of foreign key columns in the new table
   * @return
   */
  static Operator Make(catalog::namespace_oid_t namespace_oid, std::string table_name,
                       std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&columns,
                       std::vector<common::ManagedPointer<parser::ColumnDefinition>> &&foreign_keys);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the namespace
   */
  const catalog::namespace_oid_t &GetNamespaceOid() const { return namespace_oid_; }
  /**
   * @return the name of the table we want to create
   */
  const std::string &GetTableName() const { return table_name_; }
  /**
   * @return column definitions for the table
   */
  const std::vector<common::ManagedPointer<parser::ColumnDefinition>> &GetColumns() const { return columns_; }
  /**
   * @return foreign key references for the table
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
 * Physical operator for CreateIndex
 */
class CreateIndex : public OperatorNodeContents<CreateIndex> {
 public:
  /**
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param index_name Name of the index
   * @param schema Index schema of the new index
   * @return
   */
  static Operator Make(catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid, std::string index_name,
                       std::unique_ptr<catalog::IndexSchema> &&schema);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
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
   * @return Name of the index
   */
  const std::string &GetIndexName() const { return index_name_; }

  /**
   * @return pointer to the schema
   */
  common::ManagedPointer<catalog::IndexSchema> GetSchema() const { return common::ManagedPointer(schema_); }

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
   * Name of the Index
   */
  std::string index_name_;

  /**
   * Index Schema
   */
  std::unique_ptr<catalog::IndexSchema> schema_;
};

/**
 * Physical operator for CreateNamespace/Namespace
 */
class CreateNamespace : public OperatorNodeContents<CreateNamespace> {
 public:
  /**
   * @param namespace_name Name of the namespace to be created
   * @return
   */
  static Operator Make(std::string namespace_name);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
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
 * Physical operator for CreateView
 */
class CreateView : public OperatorNodeContents<CreateView> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param view_name Name of the view
   * @param view_query Query statement of the view
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, std::string view_name,
                       common::ManagedPointer<parser::SelectStatement> view_query);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
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
  common::ManagedPointer<parser::SelectStatement> GetViewQuery() const { return common::ManagedPointer(view_query_); }

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

/**
 * Physical operator for CreateTrigger
 */
class CreateTrigger : public OperatorNodeContents<CreateTrigger> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table
   * @param trigger_name Name of the trigger
   * @param trigger_funcnames Trigger function names
   * @param trigger_args Trigger arguments
   * @param trigger_columns OIDs of trigger columns
   * @param trigger_when Trigger when clause
   * @param trigger_type Type of the trigger
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       catalog::table_oid_t table_oid, std::string trigger_name,
                       std::vector<std::string> &&trigger_funcnames, std::vector<std::string> &&trigger_args,
                       std::vector<catalog::col_oid_t> &&trigger_columns,
                       common::ManagedPointer<parser::AbstractExpression> &&trigger_when, int16_t trigger_type);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
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
 * Physical operator for DropDatabase
 */
class DropDatabase : public OperatorNodeContents<DropDatabase> {
 public:
  /**
   * @param db_oid OID of the database to be dropped
   * @return
   */
  static Operator Make(catalog::db_oid_t db_oid);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return the OID of the database we want to drop
   */
  const catalog::db_oid_t &GetDatabaseOID() const { return db_oid_; }

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t db_oid_ = catalog::INVALID_DATABASE_OID;
};

/**
 * Physical operator for CreateFunction
 */
class CreateFunction : public OperatorNodeContents<CreateFunction> {
 public:
  /**
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param function_name Name of the function
   * @param language Language type of the user defined function
   * @param function_body Body of the user defined function
   * @param function_param_names Parameter names of the user defined function
   * @param function_param_types Parameter types of the user defined function
   * @param return_type Return type of the user defined function
   * @param param_count Number of parameters of the user defined function
   * @param replace If this function should replace existing definitions
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                       std::string function_name, parser::PLType language, std::vector<std::string> &&function_body,
                       std::vector<std::string> &&function_param_names,
                       std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types,
                       parser::BaseFunctionParameter::DataType return_type, size_t param_count, bool replace);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

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
  size_t GetParamCount() const { return param_count_; }

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
  size_t param_count_ = 0;
};

/**
 * Physical operator for DropTable
 */
class DropTable : public OperatorNodeContents<DropTable> {
 public:
  /**
   * @param table_oid OID of the table to be dropped
   * @return
   */
  static Operator Make(catalog::table_oid_t table_oid);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return the OID of the database we want to drop
   */
  const catalog::table_oid_t &GetTableOID() const { return table_oid_; }

 private:
  /**
   * OID of the table
   */
  catalog::table_oid_t table_oid_ = catalog::INVALID_TABLE_OID;
};

/**
 * Physical operator for DropIndex
 */
class DropIndex : public OperatorNodeContents<DropIndex> {
 public:
  /**
   * @param index_oid OID of the index to be dropped
   * @return
   */
  static Operator Make(catalog::index_oid_t index_oid);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return the OID of the index we want to drop
   */
  const catalog::index_oid_t &GetIndexOID() const { return index_oid_; }

 private:
  /**
   * OID of the table
   */
  catalog::index_oid_t index_oid_ = catalog::INVALID_INDEX_OID;
};

/**
 * Physical operator for DropNamespace
 */
class DropNamespace : public OperatorNodeContents<DropNamespace> {
 public:
  /**
   * @param namespace_oid OID of namespace to drop
   * @return
   */
  static Operator Make(catalog::namespace_oid_t namespace_oid);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return the OID of the namespace we want to drop
   */
  const catalog::namespace_oid_t &GetNamespaceOID() const { return namespace_oid_; }

 private:
  /**
   * OID of the table
   */
  catalog::namespace_oid_t namespace_oid_ = catalog::INVALID_NAMESPACE_OID;
};

/**
 * Physical operator for DropTrigger
 */
class DropTrigger : public OperatorNodeContents<DropTrigger> {
 public:
  /**
   * @param database_oid OID of database
   * @param trigger_oid OID of trigger to drop
   * @param if_exists existence flag
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::trigger_oid_t trigger_oid, bool if_exists);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the trigger to drop
   */
  catalog::trigger_oid_t GetTriggerOid() const { return trigger_oid_; }

  /**
   * @return true if "IF EXISTS" was used
   */
  bool IsIfExists() const { return if_exists_; }

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * OID of the trigger to drop
   */
  catalog::trigger_oid_t trigger_oid_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;
};

/**
 * Physical operator for DropView
 */
class DropView : public OperatorNodeContents<DropView> {
 public:
  /**
   * @param database_oid OID of database
   * @param view_oid OID of view to drop
   * @param if_exists existence flag
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::view_oid_t view_oid, bool if_exists);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the view to drop
   */
  catalog::view_oid_t GetViewOid() const { return view_oid_; }

  /**
   * @return true if "IF EXISTS" was used
   */
  bool IsIfExists() const { return if_exists_; }

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the view to drop
   */
  catalog::view_oid_t view_oid_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;
};

/**
 * Physical operator for Analyze
 */
class Analyze : public OperatorNodeContents<Analyze> {
 public:
  /**
   * @param database_oid OID of the database
   * @param table_oid OID of the table
   * @param columns OIDs of Analyze columns
   * @return
   */
  static Operator Make(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                       std::vector<catalog::col_oid_t> &&columns);

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override;

  bool operator==(const BaseOperatorNodeContents &r) override;
  common::hash_t Hash() const override;

  /**
   * @return OID of the database
   */
  const catalog::db_oid_t &GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the table
   */
  const catalog::table_oid_t &GetTableOid() const { return table_oid_; }

  /**
   * @return columns
   */
  std::vector<catalog::col_oid_t> GetColumns() const { return columns_; }

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the target table
   */
  catalog::table_oid_t table_oid_;

  /**
   * Vector of column to Analyze
   */
  std::vector<catalog::col_oid_t> columns_;
};

}  // namespace optimizer
}  // namespace noisepage
