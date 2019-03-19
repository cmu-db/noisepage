#pragma once

#include <type/value.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"

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
  TypeHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_type);

  /**
   * Get the oid of a type given its name.
   */
  type_oid_t TypeToOid(transaction::TransactionContext *txn, const std::string &type);

  /**
   * Get a type entry from pg_type handle
   *
   * @param txn the transaction to run
   * @param oid type entry oid
   * @return a shared pointer to the type entry
   */
  std::shared_ptr<TypeEntry> GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid);

  /**
   * Add a type entry into pg_type handle.
   */
  void AddEntry(transaction::TransactionContext *txn, type_oid_t oid, const std::string &typname,
                namespace_oid_t typnamespace, int32_t typlen, const std::string &typtype);

  /**
   * Get a type entry from pg_type handle by name.
   */
  std::shared_ptr<TypeEntry> GetTypeEntry(transaction::TransactionContext *txn, const std::string &type);

  /**
   * Get a type entry from pg_type handle by name.
   */
  std::shared_ptr<TypeHandle::TypeEntry> GetTypeEntry(transaction::TransactionContext *txn, const type::Value &type);

  /**
   * Create storage table
   */
  static std::shared_ptr<catalog::SqlTableRW> Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                     db_oid_t db_oid, const std::string &name);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_type_rw_->Dump(txn); }

  /**
   * Get used schema columns.
   * @return a vector of used schema columns.
   */
  const std::vector<SchemaCol> &GetSchemaColumns() { return schema_cols_; }

  /**
   * Get unused schema columns.
   * @return a vector of unused schema columns.
   */
  const std::vector<SchemaCol> &GetUnusedSchemaColumns() { return unused_schema_cols_; }

  // TODO(yeshengm): we have to add support for UDF in the future
 private:
  // Catalog *catalog_;
  std::shared_ptr<catalog::SqlTableRW> pg_type_rw_;
  static const std::vector<SchemaCol> schema_cols_;
  static const std::vector<SchemaCol> unused_schema_cols_;
};

}  // namespace terrier::catalog
