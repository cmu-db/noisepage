#pragma once

#include <exception>
#include <string>
#include <utility>

namespace noisepage::parser {

constexpr int INVALID_TYPE_ID = 0;

enum class StatementType {
  INVALID = INVALID_TYPE_ID,
  SELECT = 1,
  INSERT = 3,
  UPDATE = 4,
  DELETE = 5,
  CREATE = 6,
  DROP = 7,
  PREPARE = 8,
  EXECUTE = 9,
  RENAME = 11,
  ALTER = 12,
  TRANSACTION = 13,
  COPY = 14,
  ANALYZE = 15,
  VARIABLE_SET = 16,
  CREATE_FUNC = 17,
  EXPLAIN = 18,
  VARIABLE_SHOW = 19,
};

enum class FKConstrMatchType { SIMPLE = 0, PARTIAL = 1, FULL = 2 };

enum class FKConstrActionType {
  INVALID = INVALID_TYPE_ID,
  NOACTION = 1,
  RESTRICT_ = 2,  // TODO(WAN): macro conflict with TPL
  CASCADE = 3,
  SETNULL = 4,
  SETDEFAULT = 5
};

enum class TableReferenceType {
  INVALID = INVALID_TYPE_ID,
  NAME = 1,          // table name
  SELECT = 2,        // output of select
  JOIN = 3,          // output of join
  CROSS_PRODUCT = 4  // out of cartesian product
};

enum class JoinType {
  INVALID = INVALID_TYPE_ID,
  LEFT = 1,
  RIGHT = 2,
  INNER = 3,
  OUTER = 4,
  SEMI = 5  // IN+Subquery is SEMI
};

enum class IndexType {
  INVALID = INVALID_TYPE_ID,
  BWTREE = 1,
  HASH = 2,
  BPLUSTREE = 3,
};

enum class InsertType { INVALID = INVALID_TYPE_ID, VALUES = 1, SELECT = 2 };

enum class ExternalFileFormat { CSV, BINARY };

// CREATE FUNCTION helpers

enum class PLType {
  INVALID = INVALID_TYPE_ID,
  PL_PGSQL = 1,  // UDF language: Pl_PGSQL
  PL_C = 2       // UDF language: PL_C
};

enum class AsType { INVALID = INVALID_TYPE_ID, EXECUTABLE = 1, QUERY_STRING = 2 };

/**
 * The type of the common table expression.
 *
 * This is determined during parsing, as a function of the syntactic type
 * of the expression combined with the actual structure of the expression.
 * For example, the CTE
 *
 *  WITH RECURISVE x(i) AS (SELECT 1) ...
 *
 * is syntactically recursive, but does not actually possess
 * an inductive structure. Therefore, this CTE would have the
 * type `CteType::RECURSIVE`
 *
 * In contrast, the CTE
 *
 *  WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i FROM x WHERE i < 5) ...
 *
 * is both syntactically and structurally recursive, so we
 * would have that the type of this CTE is CteType::STRUCTURALLY_RECURSIVE.
 *
 * The upside is that STRUCTURALLY_RECURSIVE is a strictly stronger
 * condition than RECURSIVE, and similarly STRUCTURALLY_ITERATIVE is
 * a strictly stronger condition than ITERATIVE.
 *
 * We must distinguish between these two kinds of types because they
 * are relevant in different contexts. The syntactic type of the CTE
 * allows us to make decisions during binding regarding references
 * that are visible to a temporary table definition. The structural
 * type of the CTE is used during code generation.
 */
enum class CteType { INVALID, SIMPLE, RECURSIVE, ITERATIVE, STRUCTURALLY_RECURSIVE, STRUCTURALLY_ITERATIVE };

}  // namespace noisepage::parser
