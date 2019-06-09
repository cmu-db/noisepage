#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define INDEX_TABLE_OID table_oid_t(31);
#define INDEX_OID_INDEX_OID index_oid_t(32);
#define INDEX_TABLE_INDEX_OID index_oid_t(33);

/*
 * Column names of the form "IND[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "IND_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define INDOID_COL_OID col_oid_t(1)         // INTEGER (pkey, fkey: pg_class)
#define INDRELID_COL_OID col_oid_t(2)       // INTEGER (fkey: pg_class)
#define INDISUNIQUE_COL_OID col_oid_t(3)    // BOOLEAN
#define INDISPRIMARY_COL_OID col_oid_t(4)   // BOOLEAN
#define INDISEXCLUSION_COL_OID col_oid_t(5) // BOOLEAN
#define INDIMMEDIATE_COL_OID col_oid_t(6)   // BOOLEAN
#define INDISVALID_COL_OID col_oid_t(7)     // BOOLEAN
#define INDISREADY_COL_OID col_oid_t(8)     // BOOLEAN
#define INDISLIVE_COL_OID col_oid_t(9)      // BOOLEAN

/**
 * This is a thin wrapper around projections into pg_index.  The interface
 * is intended to  be generic enough that the underlying table schemas could
 * be replaced with a different implementation and not significantly affect
 * the core catalog code.
 *
 * @warning Only the catalog should be instantiating or directly handling these
 * objects.  All other users of the catalog should be using the internal C++
 * API.
 */
class IndexEntry {
 public:
  /**
   * Prepares an object to wrap projections into the namespace table
   * @param txn owning all of the operations
   * @param pg_index_table into which we are fetching entries
   */
  IndexEntry(transaction::TransactionContext *txn, storage::SqlTable *pg_index_table);

  /**
   * Destructor for the IndexEntry.
   */
  ~IndexEntry();

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
   * Sets the corresponding field of the entry to null
   * @param column OID of the field
   */
  void SetNull(col_oid_t column) {
    row_.SetNull(projection_map_[column]);
  }

  /**
   * @return the OID assigned to the given entry
   */
  namespace_oid_t GetOid() {
    namespace_oid_t *oid_ptr =
      reinterpret_cast<namespace_oid_t *>(row_.AccessWithNullCheck(projection_map_[INDOID_COL_OID]));
    return (oid_ptr == nullptr) ? INVALID_NAMESPACE_OID : *oid_ptr;
  }

  /**
   * Sets the oid value for the current entry
   * @param oid to give to the entry
   * @warning Only the corresponding DatabaseCatalog object should ever call this
   * function as it is the deconfliction point for OIDs within a database.
   */
  void SetOid(namespace_oid_t oid) {
    col_oid_t *oid_ptr = reinterpret_cast<namespace_oid_t *>(row_.AccessForceNotNull(projection_map_[INDOID_COL_OID]));
    *oid_ptr = oid;
  }

  /**
   * @return OID of table which is indexed
   */
  table_oid_t GetTable();

  /**
   * Sets the indexed table for the current entry
   * @param table OID of table
   */
  void SetTable(table_oid_t table);

  /**
   * @return whether the index stores unique key entries
   */
  bool IsUnique();

  /**
   * Sets the field in the entry
   * @param is_unique indicating whether only unique keys are allowed
   */
  void SetUnique(bool is_unique);

  /**
   * @return whether the index is on the primary key for the table
   */
  bool IsPrimary();

  /**
   * Sets the field in the entry
   * @param is_primary indicating whether this indexes a primary key
   */
  void SetPrimary(bool is_primary);

  /**
   * @return whether the index lists excluded entries
   */
  bool IsExclusion();

  /**
   * Sets the field in the entry
   * @param is_exclusion indicating whether prohibited values are indexed
   */
  void SetExclusion(bool is_exclusion);

  /**
   * @return whether the index will fail immediately if its a constraint
   */
  bool IsImmediate();

  /**
   * Sets the field in the entry
   * @param is_immediate indicating whether the index will indicate failure immediately
   */
  void SetImmediate(bool is_immediate);

  /**
   * @return whether the index has been allocated and valid for inserts
   */
  bool IsValid();

  /**
   * Sets the field in the entry
   * @param is_valid indicating whether the index can accept inserts
   */
  void SetValid(bool is_valid);

  /**
   * @return whether the index is ready for lookups
   */
  bool IsReady();

  /**
   * Sets the field in the entry
   * @param is_ready indicating whether operations will succeed/fail correctly
   */
  void SetIsReady(bool is_ready);

  /**
   * @return whether the index is visible
   */
  bool IsLive();

  /**
   * Sets the field in the entry
   * @param is_live indicating whether the index is visible
   */
  void SetLive(bool is_live);

 private:
  storage::ProjectedRow *row_;
  storage::ProjectionMap *projection_map_;

  transaction::TransactionContext *txn_;
  storage::SqlTable *table_;

  storage::TupleSlot slot_;
};
} // namespace terrier::catalog::postgres
