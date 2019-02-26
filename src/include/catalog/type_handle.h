#pragma once

#include <type/value.h>
#include "catalog_defs.h"
#include "catalog_sql_table.h"

namespace terrier::catalog {

class Catalog;

/**
 * A type handle contains information about data types.
 *
 * pg_type:
 *      oid | typname | typlen | typtype | typcategory
 */
class TypeHandle {
 public:
  /**
   * A type entry represents a row in pg_type catalog.
   */
  class TypeEntry {
   public:
    /**
     * Constructs a type entry.
     * @param oid the col_oid of the type
     * @param entry the row as a vector of values
     */
    TypeEntry(type_oid_t oid, std::vector<type::Value> entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Get the value for a given column.
     */
    const type::Value &GetColumn(int32_t col_num) { return entry_[col_num]; }

    /**
     * Return the col_oid of the type.
     */
    type_oid_t GetTypeOid() { return oid_; }

   private:
    type_oid_t oid_;
    std::vector<type::Value> entry_;
  };

  /**
   * Construct a type handle. It keeps a pointer to the pg_type sql table.
   */
  explicit TypeHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_type);

  std::shared_ptr<TypeEntry> GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid);

 private:
  Catalog *catalog_;
  std::shared_ptr<catalog::SqlTableRW> pg_type_rw_;
};

}  // namespace terrier::catalog
