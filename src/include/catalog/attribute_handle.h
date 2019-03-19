#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/table_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
struct SchemaCol;

/**
 * A attribute handle contains information about all the attributes in a table. It is used to
 * retrieve attribute related information. It presents a part of pg_attribute.
 *
 * pg_attribute:
 *      oid | attrelid | attname | atttypid | attlen | attnum
 */
class AttributeHandle {
 public:
  /**
   * A attribute entry represent a row in pg_attribute catalog.
   */
  class AttributeEntry {
   public:
    /**
     * Constructs a attribute entry.
     * @param oid the col_oid of the attribute
     * @param entry: the row as a vector of values
     */
    AttributeEntry(col_oid_t oid, std::vector<type::Value> entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Get the value for a given column
     * @param col_num the column index
     * @return the value of the column
     */
    const type::Value &GetColumn(int32_t col_num) { return entry_[col_num]; }

    /**
     * Return the col_oid of the attribute
     * @return col_oid of the attribute
     */
    col_oid_t GetAttributeOid() { return oid_; }

   private:
    col_oid_t oid_;
    std::vector<type::Value> entry_;
  };

  /**
   * Construct an attribute handle
   * @param catalog catalog ptr
   * @param pg_attribute a pointer to pg_attribute sql table rw helper instance
   */
  explicit AttributeHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_attribute)
      : pg_attribute_hrw_(std::move(pg_attribute)) {}

  /**
   * Construct an attribute handle. It keeps a pointer to the pg_attribute sql table.
   * @param table a pointer to SqlTableRW
   * @param pg_attribute a pointer to pg_attribute sql table rw helper instance
   * Deprecate
   */
  explicit AttributeHandle(SqlTableRW *table, std::shared_ptr<catalog::SqlTableRW> pg_attribute)
      : table_(table), pg_attribute_hrw_(std::move(pg_attribute)) {}

  /**
   * Convert a attribute string to its oid representation
   * @param name the attribute
   * @param txn the transaction context
   * @return the col oid
   */
  col_oid_t NameToOid(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Get a attribute entry for a given col_oid. It's essentially equivalent to reading a
   * row from pg_attribute. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param oid the col_oid of the database the transaction wants to read
   * @return a shared pointer to Attribute entry; NULL if the attribute doesn't exist in
   * the database
   */
  std::shared_ptr<AttributeEntry> GetAttributeEntry(transaction::TransactionContext *txn, col_oid_t oid);

  /**
   * Get a attribute entry for a given attribute. It's essentially equivalent to reading a
   * row from pg_attribute. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the attribute of the database the transaction wants to read
   * @return a shared pointer to Attribute entry; NULL if the attribute doesn't exist in
   * the database
   */
  std::shared_ptr<AttributeEntry> GetAttributeEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Create the storage table
   */
  static std::shared_ptr<catalog::SqlTableRW> Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                     db_oid_t db_oid, const std::string &name);

  /**
   * Add a row
   */
  // void AddEntry(transaction::TransactionContext *txn, int32_t oid, int32_t table_oid, const std::string &name);

  // start Debug methods

  /**
   * Dump the contents of the table
   * @param txn
   */
  void Dump(transaction::TransactionContext *txn) { pg_attribute_hrw_->Dump(txn); }
  // end Debug methods

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  /** Unused schema columns */
  static const std::vector<SchemaCol> unused_schema_cols_;

 private:
  // Catalog *catalog_;
  SqlTableRW *table_;
  std::shared_ptr<catalog::SqlTableRW> pg_attribute_hrw_;
};

}  // namespace terrier::catalog
