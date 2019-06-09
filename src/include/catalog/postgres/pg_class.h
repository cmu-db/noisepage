#pragma once

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define CLASS_TABLE_OID table_oid_t(21);
#define CLASS_OID_INDEX_OID index_oid_t(22);
#define CLASS_NAME_INDEX_OID index_oid_t(23);
#define CLASS_NAMESPACE_INDEX_OID index_oid_t(24);

/*
 * Column names of the form "REL[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "REL_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define RELOID_COL_OID col_oid_t(1)         // INTEGER (pkey)
#define RELNAME_COL_OID col_oid_t(2)        // VARCHAR
#define RELNAMESPACE_COL_OID col_oid_t(3)   // INTEGER (fkey: pg_namespace)
#define RELKIND_COL_OID col_oid_t(4)        // CHAR
#define REL_SCHEMA_COL_OID col_oid_t(5)     // BIGINT (assumes 64-bit pointers)
#define REL_PTR_COL_OID col_oid_t(6)        // BIGINT (assumes 64-bit pointers)
#define REL_NEXTCOLOID_COL_OID col_oid_t(7) // INTEGER

enum class ClassKind : char {
  REGULAR_TABLE = 'r',
  INDEX = 'i',
  SEQUENCE = 'S',
  VIEW = 'v',
  MATERIALIZED_VIEW = 'm',
  COMPOSITE_TYPE = 'c',
  TOAST_TABLE = 't',
  FOREIGN_TABLE = 'f',
}

/**
 * This is a thin wrapper around projections into pg_class.  The interface
 * is intended to  be generic enough that the underlying table schemas could
 * be replaced with a different implementation and not significantly affect
 * the core catalog code.
 *
 * @warning Only the catalog should be instantiating or directly handling these
 * objects.  All other users of the catalog should be using the internal C++
 * API.
 */
class ClassEntry {
 public:
  /**
   * Prepares an object to wrap projections into the class table
   * @param txn owning all of the operations
   * @param pg_class_table into which we are fetching entries
   */
  ClassEntry(transaction::TransactionContext *txn, storage::SqlTable *pg_class_table);

  /**
   * Destructor for the ClassEntry.
   */
  ~ClassEntry();

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
  type_oid_t GetOid() {
    type_oid_t *oid_ptr =
      reinterpret_cast<type_oid_t *>(row_.AccessWithNullCheck(projection_map_[RELOID_COL_OID]));
    return (oid_ptr == nullptr) ? INVALID_TYPE_OID : *oid_ptr;
  }

  /**
   * Sets the oid value for the current entry
   * @param oid to give to the entry
   * @warning Only the corresponding DatabaseCatalog object should ever call this
   * function as it is the deconfliction point for OIDs within a database.
   */
  void SetOid(type_oid_t oid) {
    col_oid_t *oid_ptr = reinterpret_cast<type_oid_t *>(row_.AccessForceNotNull(projection_map_[RELOID_COL_OID]));
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
   * @return the kind of database object this is
   */
  ClassKind GetKind();

  /**
   * Sets the kind of class this is
   * @param kind of the class
   */
  void SetKind(ClassKind kind);

  /**
   * @return the pointer to the schema object associated with the entry
   */
  Schema *GetTableSchema();

  /**
   * Sets the cached schema object for the current entry
   * @param schema pointer describing the table
   */
  void SetTableSchema(Schema *schema);


  /**
   * @return the pointer to the schema object associated with the entry
   */
  storage::SqlTable *GetTablePointer();

  /**
   * Sets the table pointer for the current entry
   * @param table pointer
   */
  void SetTablePointer(storage::SqlTable *table);

  /**
   * @return the pointer to the schema object associated with the entry
   */
  IndexKeySchema *GetIndexKeySchema();

  /**
   * Sets the cached schema object for the current entry
   * @param schema pointer describing the table
   */
  void SetIndexKeySchema(IndexKeySchema *schema);


  /**
   * @return the pointer to the schema object associated with the entry
   */
  storage::index::Index *GetIndexPointer();

  /**
   * Sets the index pointer for the current entry
   * @param index pointer
   */
  void SetIndexPointer(storage::index::Index *index);

 private:
  storage::ProjectedRow *row_;
  storage::ProjectionMap *projection_map_;

  transaction::TransactionContext *txn_;
  storage::SqlTable *table_;

  storage::TupleSlot slot_;
};
} // namespace terrier::catalog::postgres
