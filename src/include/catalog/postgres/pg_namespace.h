#pragma once

#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

/*
 * Column names of the form "NSP[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "NSP_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define NSPOID_COL_OID col_oid_t(1)  // INTEGER (pkey)
#define NSPNAME_COL_OID col_oid_t(2) // VARCHAR

/**
 * Get a new schema object that describes the pg_namespace table
 * @return the pg_namespace schema object
 */
Schema GetNamespaceTableSchema();

/**
 * Instantiate a new SqlTable for pg_namespace
 * @param block_store to back the table's memory requirements
 * @return pointer to the new pg_namespace table
 */
storage::SqlTable *CreateNamespaceTable(storage::BlockStore *block_store);

/**
 * This is a thin wrapper around projections into pg_namespace.  The interface
 * is intended to  be generic enough that the underlying table schemas could
 * be replaced with a different implementation and not significantly affect
 * the core catalog code.
 *
 * @warning Only the catalog should be instantiating or directly handling these
 * objects.  All other users of the catalog should be using the internal C++
 * API.
 */
class NamespaceEntry {
 public:
  /**
   * Prepares an object to wrap projections into the namespace table
   * @param txn owning all of the operations
   * @param pg_namespace_table into which we are fetching entries
   */
  NamespaceEntry(transaction::TransactionContext *txn_, storage::SqlTable *pg_namespace_table);

  /**
   * Destructor for the NamespaceEntry.
   */
  ~NamespaceEntry() {
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
   * Insert the prepared entry into the namespace table
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
    TERRIER_ASSERT(true, "Indexed column changes require a delete and insert")
    return table_->Update(txn_, slot_, row_);
  }

  /**
   * Logically deletes the previously selected entry from the namespace table
   * @return true if the operation succeeds, otherwise false
   * @warning This call does not modify any other tables and therefore does
   * not handle cascading deletes in the catalog.  The caller is responsible
   * for ensuring all references to the deleted namespace are removed prior
   * to committing.  Failure would result in objects being unreachable by name.
   */
  bool Delete() {
    return table_->Delete(txn_, slot_);
  }

  /**
   * @return the OID assigned to the given entry
   */
  namespace_oid_t GetOid() {
    namespace_oid_t *oid_ptr =
      reinterpret_cast<namespace_oid_t *>(row_.AccessWithNullCheck(projection_map_[NSPOID_COL_OID]));
    return (oid_ptr == nullptr) ? INVALID_NAMESPACE_OID : *old_ptr;
  }

  /**
   * Sets the oid value for the current entry
   * @param oid to give to the entry
   * @warning Only the corresponding DatabaseCatalog object should ever call this
   * function as it is the deconfliction point for OIDs within a database.
   */
  void SetOid(namespace_oid_t oid) {
    col_oid_t *oid_ptr = reinterpret_cast<namespace_oid_t *>(row_.AccessForceNotNull(projection_map_[NSPOID_COL_OID]));
    *oid_ptr = oid;
  }

  /**
   * @return a string view of the namespace's name.
   */
  const std::string_view GetName();

  /**
   * Sets the name field of the entry.  This function must have complete ownership
   * of the string passed as it will transfer ownership to the underlying varlen
   * that it creates.
   * @param name of the namespace
   */
  void SetName(std::string name);

 private:
  storage::ProjectedRow *row_;
  storage::ProjectionMap *projection_map_;

  transaction::TransactionContext *txn_;
  storage::SqlTable *table_;

  storage::TupleSlot slot_;
};
} // namespace terrier::catalog::postgres
