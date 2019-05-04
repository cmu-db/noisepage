#pragma once

#include <exception>
#include <string>
#include <utility>

namespace terrier::parser {

constexpr int INVALID_TYPE_ID = 0;

enum class StatementType {
  INVALID = INVALID_TYPE_ID,  // invalid statement type
  SELECT = 1,                 // select statement type
  INSERT = 3,                 // insert statement type
  UPDATE = 4,                 // update statement type
  DELETE = 5,                 // delete statement type
  CREATE = 6,                 // create statement type
  DROP = 7,                   // drop statement type
  PREPARE = 8,                // prepare statement type
  EXECUTE = 9,                // execute statement type
  RENAME = 11,                // rename statement type
  ALTER = 12,                 // alter statement type
  TRANSACTION = 13,           // transaction statement type,
  COPY = 14,                  // copy type
  ANALYZE = 15,               // analyze type
  VARIABLE_SET = 16,          // variable set statement type
  CREATE_FUNC = 17,           // create func statement type
  EXPLAIN = 18                // explain statement type
};

enum class FKConstrMatchType { SIMPLE = 0, PARTIAL = 1, FULL = 2 };

enum class FKConstrActionType {
  INVALID = INVALID_TYPE_ID,  // invalid
  NOACTION = 1,
  RESTRICT_ = 2,  // TODO(WAN): macro conflict with TPL
  CASCADE = 3,
  SETNULL = 4,
  SETDEFAULT = 5
};

enum class TableReferenceType {
  INVALID = INVALID_TYPE_ID,  // invalid table reference type
  NAME = 1,                   // table name
  SELECT = 2,                 // output of select
  JOIN = 3,                   // output of join
  CROSS_PRODUCT = 4           // out of cartesian product
};

enum class JoinType {
  INVALID = INVALID_TYPE_ID,  // invalid join type
  LEFT = 1,                   // left
  RIGHT = 2,                  // right
  INNER = 3,                  // inner
  OUTER = 4,                  // outer
  SEMI = 5                    // IN+Subquery is SEMI
};

enum class IndexType {
  INVALID = INVALID_TYPE_ID,  // invalid index type
  BWTREE = 1,                 // bwtree
  HASH = 2,                   // hash
  SKIPLIST = 3,               // skiplist
  ART = 4,                    // ART
};

enum class InsertType {
  INVALID = INVALID_TYPE_ID,  // invalid insert type
  VALUES = 1,                 // values
  SELECT = 2                  // select
};

enum class ExternalFileFormat { CSV, BINARY };

// CREATE FUNCTION helpers

enum class PLType {
  INVALID = INVALID_TYPE_ID,
  PL_PGSQL = 1,  // UDF language: Pl_PGSQL
  PL_C = 2       // UDF language: PL_C
};

enum class AsType { INVALID = INVALID_TYPE_ID, EXECUTABLE = 1, QUERY_STRING = 2 };

}  // namespace terrier::parser
