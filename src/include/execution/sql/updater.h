#pragma once
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"
namespace terrier::execution::sql {

/**
 * Helper class to perform updates in SQL Tables.
 */
class EXPORT Updater {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context.
   * @param table_name Name of the table
   * @param col_oids Col oids to updated.
   * @param num_oids Number of column oids.
   * @param is_index_key_update Whether this will update indexed keys.
   */
  explicit Updater(exec::ExecutionContext *exec_ctx, std::string table_name, uint32_t *col_oids, uint32_t num_oids,
                   bool is_index_key_update)
      : Updater(exec_ctx, exec_ctx->GetAccessor()->GetTableOid(table_name), col_oids, num_oids, is_index_key_update) {}

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

  /**
   * @return The table's projected row.
   */
  storage::ProjectedRow *GetTablePR();

  /**
   * @param index_oid oid of the index to access
   * @return The projected row of the index.
   */
  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  /**
   * Update a tuple in the table.
   * @param table_tuple_slot tuple slot of the tuple.
   * @return Whether update was successful.
   */
  bool TableUpdate(storage::TupleSlot table_tuple_slot);

  /**
   * Delete a tuple from the table.
   * @param table_tuple_slot tuple slot of the tuple.
   * @return Whether delete was successful.
   */
  bool TableDelete(storage::TupleSlot table_tuple_slot);

  /**
   * Reinsert tuple into table.
   * @return slot where the insertion occurred.
   */
  storage::TupleSlot TableInsert();

  /**
   * Delete an item from the index.
   * @param index_oid oid of the index.
   * @param table_tuple_slot slot of the item to delete.
   */
  void IndexDelete(catalog::index_oid_t index_oid, storage::TupleSlot table_tuple_slot);

  /**
   * Reinsert an item into the index.
   * @param index_oid oid of the index.
   * @return Whether insertion was successful.
   */
  bool IndexInsert(catalog::index_oid_t index_oid);

 private:
  common::ManagedPointer<storage::index::Index> GetIndex(catalog::index_oid_t index_oid);

  uint32_t CalculateMaxIndexPRSize();
  void *GetMaxSizedIndexPRBuffer();

  void PopulateAllColOids(std::vector<catalog::col_oid_t> &all_col_oids);

  storage::ProjectedRow *GetTablePRForColumns(const std::vector<catalog::col_oid_t> &col_oids);

  catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> col_oids_;
  std::vector<catalog::col_oid_t> all_col_oids_;
  exec::ExecutionContext *exec_ctx_;
  common::ManagedPointer<terrier::storage::SqlTable> table_;

  storage::RedoRecord *table_redo_{nullptr};

  void *index_pr_buffer_;
  storage::ProjectedRow *index_pr_{nullptr};

  std::map<terrier::catalog::index_oid_t, common::ManagedPointer<storage::index::Index>> index_cache_;

  bool is_index_key_update_ = false;
};
}  // namespace terrier::execution::sql