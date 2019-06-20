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
#define CONOID_COL_OID col_oid_t(1)        // INTEGER (pkey)
#define CONNAME_COL_OID col_oid_t(2)       // VARCHAR
#define CONNAMESPACE_COL_OID col_oid_t(3)  // INTEGER (fkey: pg_namespace)
#define CONTYPE_COL_OID col_oid_t(4)       // CHAR
#define CONDEFERRABLE_COL_OID col_oid_t(5) // BOOLEAN
#define CONDEFERRED_COL_OID col_oid_t(6)   // BOOLEAN
#define CONVALIDATED_COL_OID col_oit_t(7)  // BOOLEAN
#define CONRELID_COL_OID col_oid_t(8)      // INTEGER (fkey: pg_class)
#define CONINDID_COL_OID col_oid_t(9)      // INTEGER (fkey: pg_class)
#define CONFRELID_COL_OID col_oid_t(10)    // INTEGER (fkey: pg_class)
#define CONBIN_COL_OID col_oid_t(11)       // BIGINT (assumes 64-bit pointers)
#define CONSRC_COL_OID col_oid_t(12)       // VARCHAR

enum class ConstraintType : char {
  CHECK = 'c',
  FOREIGN_KEY = 'f',
  PRIMARY_KEY = 'p',
  UNIQUE = 'u',
  TRIGGER = 't',
  EXCLUSION = 'x',
}

/**
 * This is a thin wrapper around projections into pg_constraint.  The interface
 * is intended to  be generic enough that the underlying table schemas could
 * be replaced with a different implementation and not significantly affect
 * the core catalog code.
 *
 * @warning Only the catalog should be instantiating or directly handling these
 * objects.  All other users of the catalog should be using the internal C++
 * API.
 */
class ConstraintEntry {
 public:
  /**
   * Prepares an object to wrap projections into the class table
   * @param txn owning all of the operations
   * @param pg_constraint_table into which we are fetching entries
   */
  ConstraintEntry(transaction::TransactionContext *txn, storage::SqlTable *pg_constraint_table);

  /**
   * Destructor for the ConstraintEntry.
   */
  ~ConstraintEntry();

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
  constraint_oid_t GetOid() {
    constraint_oid_t *oid_ptr =
      reinterpret_cast<constraint_oid_t *>(row_.AccessWithNullCheck(projection_map_[CONOID_COL_OID]));
    return (oid_ptr == nullptr) ? INVALID_TYPE_OID : *oid_ptr;
  }

  /**
   * Sets the oid value for the current entry
   * @param oid to give to the entry
   * @warning Only the corresponding DatabaseCatalog object should ever call this
   * function as it is the deconfliction point for OIDs within a database.
   */
  void SetOid(constraint_oid_t oid) {
    col_oid_t *oid_ptr =
      reinterpret_cast<constraint_oid_t *>(row_.AccessForceNotNull(projection_map_[CONOID_COL_OID]));
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
   * @return the type of constraint this is
   */
  ConstraintType GetType();

  /**
   * Sets the constraint type for the entry
   * @param type of the type in bytes
   */
  void SetType(ConstraintType type);


  /**
   * @return whether this constraint can be deferred
   */
  bool IsDeferrable();

  /**
   * Sets whether the constraint can be deferred
   * @param is_deferrable is true if it can be deferred
   */
  void SetDeferrable(bool is_deferrable);

  /**
   * @return whether this constraint is deferred
   */
  bool IsDeferred();

  /**
   * Sets whether the constraint is deferred
   * @param is_deferred is true if the constraint check is evaluated lazily
   */
  void SetDeferred(bool is_deferred);

  /**
   * @return whether this constraint has been validated
   */
  bool IsValidated();

  /**
   * Sets whether the constraint has been validated
   * @param is_deferrable is true if the constraint is valid
   */
  void SetValidated(bool is_validated);


  /**
   * @return the table which this constraint applies to
   */
  table_oid_t GetConstrainedTable();

  /**
   * Sets the constrained table field for the constraint
   * @param table this constraint applies to
   */
  void SetConstrainedTable(table_oid_t table);

  /**
   * @return the index needed to check this constraint
   */
  index_oid_t GetSupportingIndex();

  /**
   * Sets the index field for the constraint
   * @param index used by the constraint
   */
  void SetSupportingIndex(index_oid_t index);

  /**
   * @return the foreign table used for this constraint
   */
  table_oid_t GetForeignTable();

  /**
   * Sets the foreign table field for the constraint
   * @param table referenced by the constraint for foreign keys
   */
  void SetForeignTable(table_oid_t table);

  /**
   * Set the pointer for the cached expression object
   * @param expression to store
   */
  void SetExpressionPointer(const AbstractExpression *expression);

  /**
   * @return pointer to the cached expression object
   */
  const AbstractExpression *GetExpressionPointer();

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
