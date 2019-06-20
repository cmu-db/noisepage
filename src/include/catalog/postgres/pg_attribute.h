#pragma once

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
#define ATTNUM_COL_OID col_oid_t(1)     // INTEGER (pkey) [col_oid_t]
#define ATTRELID_COL_OID col_oid_t(2)   // INTEGER (fkey: pg_class) [table_oid_t]
#define ATTNAME_COL_OID col_oid_t(3)    // VARCHAR
#define ATTTYPID_COL_OID col_oid_t(4)   // INTEGER (fkey: pg_type) [type_oid_t]
#define ATTLEN_COL_OID col_oid_t(5)     // SMALLINT
#define ATTNOTNULL_COL_OID col_oid_t(6) // BOOLEAN
// The following columns come from 'pg_attrdef' but are included here for
// simplicity.  PostgreSQL splits out the table to allow more fine-grained
// locking during DDL operations which is not an issue in this system
#define ADBIN_COL_OID col_oid_t(7)      // BIGINT (assumes 64-bit pointers)
#define ADSRC_COL_OID col_oid_t(8)      // VARCHAR

/**
 * This is a thin wrapper around projections into pg_attribute.  The interface
 * is intended to  be generic enough that the underlying table schemas could
 * be replaced with a different implementation and not significantly affect
 * the core catalog code.
 *
 * @warning Only the catalog should be instantiating or directly handling these
 * objects.  All other users of the catalog should be using the internal C++
 * API.
 */
class AttributeEntry {
 public:
  /**
   * Prepares an object to wrap projections into the type table
   * @param txn owning all of the operations
   * @param pg_attribute_table into which we are fetching entries
   */
  AttributeEntry(transaction::TransactionContext *txn, storage::SqlTable *pg_attribute_table);

  /**
   * Destructor for the AttributeEntry.
   */
  ~AttributeEntry();

  /**
   * Loads the indicated row into the entry instance
   * @param slot to interpret
   * @return true if the slot is visible to the transaction, otherwise false
   */
  bool Select(storage::TupleSlot slot) {
    slot_ = slot;
    return table_->Select(txn_, slot, row_);
  }

  /**
   * Insert the prepared entry into the type table
   * @return the tuple slot into which the data was inserted
   * @warning This call assumes constraint checks (i.e. OID and name uniqueness)
   * have already occurred and does not perform any additional checks.
   */
  storage::TupleSlot Insert() {
    return table_->Insert(txn_, row_);
  }

  /**
   * Applies the updates staged in the entry to the originally selected slot
   * @param slot to update
   * @return true if the operation succeeds, otherwise false
   * @warning This call assumes constraint checks (i.e. OID and name uniqueness)
   * have already occurred and does not perform any additional checks.
   * @warning This function should never be called given the current table
   * implementation because all columns are indexed.  It is included only to
   * provide a stable API as more of Postgres' catalog is added over time.
   */
  bool Update() {
    return table_->Update(txn_, slot_, row_);
  }

  /**
   * Logically deletes the previously selected entry from the type table
   * @return true if the operation succeeds, otherwise false
   * @warning This call does not modify any other tables and therefore does
   * not handle cascading deletes in the catalog.  The caller is responsible
   * for ensuring all references to the deleted type are removed prior
   * to committing.  Failure would result in objects being unreachable by name.
   */
  bool Delete() {
    return table_->Delete(txn_, slot_);
  }

  /**
   * Sets the corresponding field of the entry to null
   * @param column OID of the field
   */
  void SetNull(col_oid_t column) {
    row_.SetNull(projection_map_[column]);
  }

  /**
   * @return the OID assigned to the given entry
   */
  col_oid_t GetOid() {
    col_oid_t *oid_ptr =
      reinterpret_cast<col_oid_t *>(row_.AccessWithNullCheck(projection_map_[ATTNUM_COL_OID]));
    return (oid_ptr == nullptr) ? INVALID_TYPE_OID : *oid_ptr;
  }

  /**
   * Sets the oid value for the current entry
   * @param oid to give to the entry
   * @warning Only the corresponding DatabaseCatalog object should ever call this
   * function as it is the deconfliction point for OIDs within a database.
   */
  void SetOid(col_oid_t oid) {
    col_oid_t *oid_ptr = reinterpret_cast<col_oid_t *>(row_.AccessForceNotNull(projection_map_[ATTNUM_COL_OID]));
    *oid_ptr = oid;
  }

  /**
   * @return a string view of the column's name.
   */
  const std::string_view GetName();

  /**
   * Sets the name field of the entry.  This function must have complete ownership
   * of the string passed as it will transfer ownership to the underlying varlen
   * that it creates.
   * @param name of the column
   */
  void SetName(std::string name);

  /**
   * @return OID of owning table
   */
  table_oid_t GetTable();

  /**
   * Sets owning table for the current entry
   * @param table OID of table
   */
  void SetTable(table_oid_t table);

  /**
   * @return the type of column this is (basic, user-defined, etc.)
   */
  type::TypeId GetType();

  /**
   * Sets the type of this column
   * @param type of the column
   */
  void SetType(type::TypeId type);

  /**
   * @return the field width of the column in bytes
   */
  uint16_t GetSize();

  /**
   * Sets the field width of the column
   * @param type_size of the column in bytes
   */
  void SetSize(uint16_t type_size);

  /**
   * @return whether the column can be null (false)
   */
  bool IsNotNull();

  /**
   * Sets whether or not the column can be null
   * @param is_not_null true if null values are not allowed in this column
   */
  void SetNotNull(bool is_not_null);

  /**
   * Set the pointer for the cached expression object
   * @param expression to store
   */
  void SetExpressionPointer(const parser::AbstractExpression *expression);

  /**
   * @return pointer to the cached expression object
   */
  const parser::AbstractExpression *GetExpressionPointer();

  /**
   * Set the serialization of the expression
   * @param expression to store
   */
  void SetExpressionString(const std::string &expression);

  /**
   * @return the serialized representation of the expression
   */
  const std::string_view GetExpressionString();

 private:
  storage::ProjectedRow *row_;
  storage::ProjectionMap *projection_map_;

  transaction::TransactionContext *txn_;
  storage::SqlTable *table_;

  storage::TupleSlot slot_;
};
} // namespace terrier::catalog::postgres
