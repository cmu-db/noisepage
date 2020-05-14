#pragma once
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/vm/module.h"
#include "execution/vm/module_compiler.h"
#include "storage/arrow_block_metadata.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

/**
 * Typedef for a standard hash map with varlen entry as the key. The map uses deep equality checks (whether
 * the stored underlying byte string is the same) for key comparison.
 */
template <class T>
using VarlenEntryMap = std::unordered_map<VarlenEntry, T, VarlenContentHasher, VarlenContentDeepEqual>;

/**
 * The block compactor is responsible for taking hot data blocks that are considered to be cold, and make them
 * arrow-compatible. In the process, any gaps resulting from deletes or aborted transactions are also eliminated.
 * If the compaction is successful, the block is considered to be fully cold and will be accessed mostly as read-only
 * data.
 */
class BlockCompactor {
 private:
  // A Compaction group is a series of blocks all belonging to the same data table. We compact them together
  // so slots can be freed up. If we only compact single block at a time, deleted slots will never be reclaimed.
  struct CompactionGroup {
    CompactionGroup(transaction::TransactionContext *txn, DataTable *table)
        : txn_(txn),
          table_(table),
          all_cols_initializer_(ProjectedRowInitializer::Create(table_->accessor_.GetBlockLayout(),
                                                                table_->accessor_.GetBlockLayout().AllColumns())),
          read_buffer_(all_cols_initializer_.InitializeRow(
              common::AllocationUtil::AllocateAligned(all_cols_initializer_.ProjectedRowSize()))) {}

    ~CompactionGroup() {
      // Deleting nullptr is just a noop
      delete[] reinterpret_cast<byte *>(read_buffer_);
    }

    // A single compaction task is done within a single transaction
    transaction::TransactionContext *txn_;
    DataTable *table_;
    std::unordered_map<RawBlock *, std::vector<uint32_t>> blocks_to_compact_;
    ProjectedRowInitializer all_cols_initializer_;
    ProjectedRow *read_buffer_;
  };

 public:
  /*
   * Constructor used to initialise the tpl_code to be compiled. This must be created using code generation in the future.
   * This constructor might also have arguments in the future to get table name, col_ids etc.
   */
  BlockCompactor() {
    // tpl code for use in moveTuple. It deletes the tuple from the table and from the index and then inserts the tuple
    // to the table (a specific block) and to the index. It returns true if the delete succeeds (because delete returns
    // false if a concurrent transaction is updating the tuple that is trying to be moved, the only condition where
    // the block compaction should be stopped).
    // This replaces with the index delete/insert added:
    //      cg->table_->InsertInto(common::ManagedPointer(cg->txn_), *record->Delta(), to);
    //      return cg->table_->Delete(common::ManagedPointer(cg->txn_), from);
    // @todo: do we need to consider that insertIndex could fail? Do so now.
    // table_name how to pass a std::string? This is required for StorageInterfaceInitBind
    // fixed length col_ids array passed : HACK
    tpl_code_ = R"(
    fun moveTuple(execCtx: *ExecutionContext, slot_from: *TupleSlot, slot_to: *TupleSlot, col_oids: [1]uint32) -> bool {
      // HACK: The function storageInterfaceInitBind expects an array, cannot accept a pointer
      col_oids[0] = 1
      var storage_interface: StorageInterface
      @storageInterfaceInitBind(&storage_interface, execCtx, "foo", col_oids, true)

      @tableCompactionCopyTupleSlot(&storage_interface, slot_from, slot_to)

      if (!@tableDelete(&storage_interface, slot_from)) {
        @storageInterfaceFree(&storage_interface)
        return false
      }

      return true
    })";
  }

  FAKED_IN_TEST ~BlockCompactor() = default;

  bool MoveTupleTPL(execution::exec::ExecutionContext *exec, TupleSlot from, TupleSlot to, col_id_t *col_oids);

  /**
   * Processes the compaction queue and mark processed blocks as cold if successful. The compaction can fail due
   * to live versions or contention. There will be a brief window where user transactions writing to the block
   * can be aborted, but no readers would be blocked.
   */
  void ProcessCompactionQueue(transaction::DeferredActionManager *deferred_action_manager,
                              transaction::TransactionManager *txn_manager);

  /**
   * Adds a block associated with a data table to the compaction to be processed in the future.
   * @param block the block that needs to be processed by the compactor
   */
  FAKED_IN_TEST void PutInQueue(RawBlock *block) { compaction_queue_.push(block); }

 private:
  bool EliminateGaps(CompactionGroup *cg);

  bool CheckForVersionsAndGaps(const TupleAccessStrategy &accessor, RawBlock *block);

  // Move a tuple and updated associated information in their respective blocks
  bool MoveTuple(CompactionGroup *cg, TupleSlot from, TupleSlot to);

  void GatherVarlens(std::vector<const byte *> *loose_ptrs, RawBlock *block, DataTable *table);

  void CopyToArrowVarlen(std::vector<const byte *> *loose_ptrs, ArrowBlockMetadata *metadata, col_id_t col_id,
                         common::RawConcurrentBitmap *column_bitmap, ArrowColumnInfo *col, VarlenEntry *values);

  void BuildDictionary(std::vector<const byte *> *loose_ptrs, ArrowBlockMetadata *metadata, col_id_t col_id,
                       common::RawConcurrentBitmap *column_bitmap, ArrowColumnInfo *col, VarlenEntry *values);

  void ComputeFilled(const BlockLayout &layout, std::vector<uint32_t> *filled, const std::vector<uint32_t> &empty) {
    // Reconstruct the list of filled slots
    // Since the list of empty slots is sorted, we can use a counter j to keep track of the next empty slot that
    // we have not encountered yet
    for (uint32_t i = 0, j = 0; i < layout.NumSlots(); i++) {
      if (j >= empty.size() || empty[j] != i)
        // Not empty, must be filled
        filled->push_back(i);
      else
        j++;
    }
  }

  std::queue<RawBlock *> compaction_queue_;

  // stores the TPL code that needs to be run in order to perform the compaction
  std::string tpl_code_;
};
}  // namespace terrier::storage
