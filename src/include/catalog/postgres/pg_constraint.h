#pragma once

#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define CONSTRAINT_TABLE_OID table_oid_t(61)
#define CONSTRAINT_OID_INDEX_OID index_oid_t(62)
#define CONSTRAINT_NAME_INDEX_OID index_oid_t(63)
#define CONSTRAINT_NAMESPACE_INDEX_OID index_oid_t(64)
#define CONSTRAINT_TABLE_INDEX_OID index_oid_t(65)
#define CONSTRAINT_INDEX_INDEX_OID index_oid_t(66)
#define CONSTRAINT_FOREIGNTABLE_INDEX_OID index_oid_t(67)

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define CONOID_COL_OID col_oid_t(1)         // INTEGER (pkey)
#define CONNAME_COL_OID col_oid_t(2)        // VARCHAR
#define CONNAMESPACE_COL_OID col_oid_t(3)   // INTEGER (fkey: pg_namespace)
#define CONTYPE_COL_OID col_oid_t(4)        // CHAR
#define CONDEFERRABLE_COL_OID col_oid_t(5)  // BOOLEAN
#define CONDEFERRED_COL_OID col_oid_t(6)    // BOOLEAN
#define CONVALIDATED_COL_OID col_oid_t(7)   // BOOLEAN
#define CONRELID_COL_OID col_oid_t(8)       // INTEGER (fkey: pg_class)
#define CONINDID_COL_OID col_oid_t(9)       // INTEGER (fkey: pg_class)
#define CONFRELID_COL_OID col_oid_t(10)     // INTEGER (fkey: pg_class)
#define CONBIN_COL_OID col_oid_t(11)        // BIGINT (assumes 64-bit pointers)
#define CONSRC_COL_OID col_oid_t(12)        // VARCHAR

enum class ConstraintType : char {
  CHECK = 'c',
  FOREIGN_KEY = 'f',
  PRIMARY_KEY = 'p',
  UNIQUE = 'u',
  TRIGGER = 't',
  EXCLUSION = 'x',
};

}  // namespace terrier::catalog::postgres
