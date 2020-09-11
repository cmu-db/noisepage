#pragma once

#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/util/execution_common.h"
#include "storage/projected_row.h"

namespace terrier::storage {
class ProjectedRow;
class SqlTable;
class RedoRecord;

namespace index {
class Index;
}  // namespace index

}  // namespace terrier::storage

namespace terrier::execution {

namespace exec {
class ExecutionContext;
}  // namespace exec

namespace sql {

/**
 * Base class to interact with the storage layer (tables and indexes).
 */
class EXPORT StorageInterface {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context.
   * @param table_oid Oid of the table
   * @param col_oids Col oids to updated.
   * @param num_oids Number of column oids.
   * @param need_indexes Whether this will use indexes.
   */
  explicit StorageInterface(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *col_oids,
                            uint32_t num_oids, bool need_indexes);

  /**
   * Destructor.
   */
  ~StorageInterface();

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

  /**
   * InsertUnique into the current index
   * @return Whether insertion was successful.
   */
  bool IndexInsertUnique();

  /**
   * Insert into the current index given a tuple
   * @param table_tuple_slot tuple slot
   * @param unique if this insertion is unique
   * @return Whether insertion was successful.
   */
  bool IndexInsertWithTuple(storage::TupleSlot table_tuple_slot, bool unique);

  /**
   * @returns index heap size
   */
  uint32_t GetIndexHeapSize();

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
  bool need_indexes_;
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
   * Reusable ProjectedRowInitializer for this table access
   */
  storage::ProjectedRowInitializer pri_;

  /**
   * Current index being accessed.
   */
  common::ManagedPointer<storage::index::Index> curr_index_{nullptr};
};
}  // namespace sql
}  // namespace terrier::execution
