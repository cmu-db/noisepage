#pragma once

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "type/type_id.h"

namespace terrier::catalog::postgres {

#define COLUMN_TABLE_OID table_oid_t(41)
#define COLUMN_OID_INDEX_OID index_oid_t(42)
#define COLUMN_NAME_INDEX_OID index_oid_t(43)
#define COLUMN_CLASS_INDEX_OID index_oid_t(44)

/*
 * Column names of the form "ATT[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "ATT_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define ATTNUM_COL_OID col_oid_t(1)      // INTEGER (pkey) [col_oid_t]
#define ATTRELID_COL_OID col_oid_t(2)    // INTEGER (fkey: pg_class) [table_oid_t]
#define ATTNAME_COL_OID col_oid_t(3)     // VARCHAR
#define ATTTYPID_COL_OID col_oid_t(4)    // INTEGER (fkey: pg_type) [type_oid_t]
#define ATTLEN_COL_OID col_oid_t(5)      // SMALLINT
#define ATTNOTNULL_COL_OID col_oid_t(6)  // BOOLEAN
// The following columns come from 'pg_attrdef' but are included here for
// simplicity.  PostgreSQL splits out the table to allow more fine-grained
// locking during DDL operations which is not an issue in this system
#define ADBIN_COL_OID col_oid_t(7)  // BIGINT (assumes 64-bit pointers)
#define ADSRC_COL_OID col_oid_t(8)  // VARCHAR

/**
 * Helper classes
 */
class AttributeHelper {
 public:
  template <typename Column>
  static std::unique_ptr<Column> MakeColumn(storage::ProjectedRow *pr, storage::ProjectionMap table_pm) {
    auto col_oid = *reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(table_pm[ATTNUM_COL_OID]));
    auto col_name = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(table_pm[ATTNAME_COL_OID]));
    auto col_type = *reinterpret_cast<type::TypeId *>(pr->AccessForceNotNull(table_pm[ATTTYPID_COL_OID]));
    auto col_len = *reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(table_pm[ATTLEN_COL_OID]));
    auto col_null = !(*reinterpret_cast<bool *>(pr->AccessForceNotNull(table_pm[ATTNOTNULL_COL_OID])));
    auto *col_expr =
        reinterpret_cast<const parser::AbstractExpression *>(pr->AccessForceNotNull(table_pm[ADBIN_COL_OID]));
    std::unique_ptr<Column> col;
    if constexpr (std::is_same_v<Column, IndexSchema::Column>) {
      if (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY) {
        col = std::make_unique<IndexSchema::Column>(col_type, col_null, *col_expr, col_len);
      } else {
        col = std::make_unique<IndexSchema::Column>(col_type, col_null, *col_expr);
      }
      col->SetOid(indexkeycol_oid_t(col_oid));
    } else {  // NOLINT
      std::string name(reinterpret_cast<const char *>(col_name->Content()), col_name->Size());
      if (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY) {
        col = std::make_unique<Schema::Column>(name, col_type, col_null, *col_expr, col_len);
      } else {
        col = std::make_unique<Schema::Column>(name, col_type, col_null, *col_expr);
      }
      col->SetOid(col_oid_t(col_oid));
    }
    return col;
  }

  static storage::VarlenEntry CreateVarlen(const std::string &str) {
    storage::VarlenEntry varlen;
    if (str.size() > storage::VarlenEntry::InlineThreshold()) {
      byte *contents = common::AllocationUtil::AllocateAligned(str.size());
      std::memcpy(contents, str.data(), str.size());
      varlen = storage::VarlenEntry::Create(contents, static_cast<uint32_t>(str.size()), true);
    } else {
      varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(str.data()),
                                                  static_cast<uint32_t>(str.size()));
    }
    return varlen
  }

  template <typename Column>
  static storage::VarlenEntry MakeNameVarlen(const Column &col) {
    if constexpr (std::is_class_v<Column, IndexSchema::Column>) {
      return CreateVarlen(std::to_string(col.GetOid()));
    } else {  // NOLINT
      return CreateVarlen(col.GetName());
    }
  }
};
}  // namespace terrier::catalog::postgres
