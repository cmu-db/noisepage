#pragma once

#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

/*
 * Column names of the form "TYP[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "TYP_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define TYPOID_COL_OID col_oid_t(1)       // INTEGER (pkey)
#define TYPNAME_COL_OID col_oid_t(2)      // VARCHAR
#define TYPNAMESPACE_COL_OID col_oid_t(3) // INTEGER (fkey: pg_namespace)
#define TYPLEN_COL_OID col_oid_t(4)       // SMALLINT
#define TYPBYVAL_COL_OID col_oid_t(5)     // BOOLEAN
#define TYPTYPE_COL_OID col_oid_t(6)      // CHAR

enum class Type : char {
  BASE = 'b',
  COMPOSITE = 'c',
  DOMAIN = 'd',
  ENUM = 'e',
  PSEUDO = 'p',
  RANGE = 'r',
}

/**
 * Get a new schema object that describes the pg_type table
 * @return the pg_type schema object
 */
Schema GetDatabaseTableSchema();

/**
 * Instantiate a new SqlTable for pg_type
 * @param block_store to back the table's memory requirements
 * @return pointer to the new pg_type table
 */
storage::SqlTable *CreateDatabaseTable(storage::BlockStore *block_store);

/**
 * This is a thin wrapper around projections into pg_type.  The interface
 * is intended to  be generic enough that the underlying table schemas could
 * be replaced with a different implementation and not significantly affect
 * the core catalog code.
 *
 * @warning Only the catalog should be instantiating or directly handling these
 * objects.  All other users of the catalog should be using the internal C++
 * API.
 */
class TypeEntry {
 public:
  /**
   * Prepares an object to wrap projections into the type table
   * @param txn owning all of the operations
   * @param pg_namespace_table into which we are fetching entries
   */
  TypeEntry(transaction::TransactionContext *txn_, storage::SqlTable *pg_database_table);

  /**
   * Destructor for the TypeEntry.
   */
  ~TypeEntry() {
    delete projection_map_;
    delete[] row_;
  }

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
   * @return the OID assigned to the given entry
   */
  type_oid_t GetOid() {
    type_oid_t *oid_ptr =
      reinterpret_cast<type_oid_t *>(row_.AccessWithNullCheck(projection_map_[DATOID_COL_OID]));
    return (oid_ptr == nullptr) ? INVALID_TYPE_OID : *old_ptr;
  }

  /**
   * Sets the oid value for the current entry
   * @param oid to give to the entry
   * @warning Only the corresponding DatabaseCatalog object should ever call this
   * function as it is the deconfliction point for OIDs within a database.
   */
  void SetOid(type_oid_t oid) {
    col_oid_t *oid_ptr = reinterpret_cast<type_oid_t *>(row_.AccessForceNotNull(projection_map_[DATOID_COL_OID]));
    *oid_ptr = oid;
  }

  /**
   * @return a string view of the type's name.
   */
  const std::string_view GetName();

  /**
   * Sets the name field of the entry.  This function must have complete ownership
   * of the string passed as it will transfer ownership to the underlying varlen
   * that it creates.
   * @param name of the type
   */
  void SetName(std::string name);

  /**
   * @return OID of owning namespace
   */
  namespace_oid_t GetNamespace();

  /**
   * Sets owning namespace for the current entry
   * @param ns OID of namespace
   */
  void SetNamespace(namespace_oid_t ns);

  /**
   * @return the field width of the type in bytes
   */
  uint16_t GetSize();

  /**
   * Sets owning namespace for the current entry
   * @param type_size of the type in bytes
   */
  void SetSize(uint16_t type_size);

  /**
   * @return whether the type is passed by value (true) or by reference (false)
   */
  bool IsByValue();

  /**
   * Sets whether or not the type is passed by value
   * @param is_by_value indicating whether the type gets passed by value
   */
  void SetByValue(bool is_by_value);

  /**
   * @return the type of type this is (basic, user-defined, etc.)
   */
  Type GetType();

  /**
   * Sets the type of this type
   * @param type of the type
   */
  void SetType(Type type);

 private:
  storage::ProjectedRow *row_;
  storage::ProjectionMap *projection_map_;

  transaction::TransactionContext *txn_;
  storage::SqlTable *table_;

  storage::TupleSlot slot_;
};
} // namespace terrier::catalog::postgres
