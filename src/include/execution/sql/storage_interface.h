#pragma once

#include <vector>
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

/**
 * Base class to interact with the storage layer (tables and indexes).
 * This class will be override by the inserter, deleter and updater.
 */
class EXPORT StorageInterface {
 public:
  /**
   * Construct
   * @param exec_ctx The current execution context.
   * @param table_oid The oid of the table to access.
   * @param use_indexes Whether indexes of the table are accessed.
   */
  StorageInterface(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, bool use_indexes);

  /**
   * Destructor to be overriden.
   */
  virtual ~StorageInterface();

  /**
   * @return The table's projected row.
   */
  terrier::storage::ProjectedRow *GetTablePR();

  /**
   * Delete slot from the table.
   * @param tuple_slot slot to delete.
   * @return Whether the deletion was successful.
   */
  bool TableDelete(storage::TupleSlot tuple_slot);

  /**
   * Update a tuple in the table.
   * @param table_tuple_slot tuple slot of the tuple.
   * @return Whether update was successful.
   */
  bool TableUpdate(storage::TupleSlot table_tuple_slot);

  /**
   * Reinsert tuple into table.
   * @return slot where the insertion occurred.
   */
  storage::TupleSlot TableInsert();

  /**
   * @param index_oid OID of the index to access.
   * @return PR of the index.
   */
  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  /**
   * Delete item from the current index.
   * @param table_tuple_slot slot corresponding to the item.
   */
  void IndexDelete(storage::TupleSlot table_tuple_slot);

  /**
   * Insert into the current index
   * @return Whether insertion was successful.
   */
  bool IndexInsert();

 protected:
  /**
   * Oid of the table being accessed.
   */
  catalog::table_oid_t table_oid_;
  /**
   * Table being accessed.
   */
  common::ManagedPointer<terrier::storage::SqlTable> table_;
  /**
   * The current execution context.
   */
  exec::ExecutionContext *exec_ctx_;
  /**
   * Slot of the tuple being modified.
   */
  storage::TupleSlot table_tuple_slot_;
  /**
   * The redo record.
   */
  storage::RedoRecord *table_redo_{nullptr};
  /**
   * Columns being accessed.
   */
  std::vector<catalog::col_oid_t> col_oids_;
  /**
   * Whether indexes will be accessed (used to avoid unnecessary allocation).
   */
  bool use_indexes_;
  /**
   * Maximum size of index projected rows.
   */
  uint32_t max_pr_size_;
  /**
   * The PR's buffer.
   */
  void *index_pr_buffer_;
  /**
   * The index PR.
   */
  storage::ProjectedRow *index_pr_{nullptr};
  /**
   * Current index being accessed.
   */
  common::ManagedPointer<storage::index::Index> curr_index_{nullptr};
};

/**
 * Helper class to perform updates in SQL Tables.
 */
class EXPORT Updater : public StorageInterface {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context.
   * @param table_oid Oid of the table
   * @param col_oids Col oids to updated.
   * @param num_oids Number of column oids.
   * @param is_index_key_update Whether this will update indexed keys.
   */
  explicit Updater(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *col_oids,
                   uint32_t num_oids, bool is_index_key_update);
};

/**
 * Helper class to perform deletes in SQL Tables.
 */
class EXPORT Deleter : public StorageInterface {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context.
   * @param table_oid Oid of the table
   */
  Deleter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid)
      : StorageInterface(exec_ctx, table_oid, true) {}
};

/**
 * Allows insertion from TPL.
 */
class EXPORT Inserter : public StorageInterface {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context
   * @param table_oid The oid of the table to insert into
   */
  explicit Inserter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);
};
}  // namespace terrier::execution::sql
