#include "storage/block_compactor.h"

#include <algorithm>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/sql_table.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_util.h"

namespace noisepage::storage {
void BlockCompactor::ProcessCompactionQueue(transaction::DeferredActionManager *deferred_action_manager,
                                            transaction::TransactionManager *txn_manager) {
  std::queue<RawBlock *> to_process = std::move(compaction_queue_);
  while (!to_process.empty()) {
    RawBlock *block = to_process.front();
    BlockAccessController &controller = block->controller_;
    switch (controller.GetBlockState()->load()) {
      case BlockState::HOT: {
        // TODO(Tianyu): The policy about how to group blocks together into compaction group can be a lot
        // more sophisticated. Compacting more blocks together frees up more memory per compaction run,
        // but makes the compaction transaction larger, which can have performance impact on the rest
        // of the system. As it currently stands, no memory is freed from this one-block-per-group scheme.
        CompactionGroup cg(txn_manager->BeginTransaction(), block->data_table_);
        // TODO(Tianyu): Additionally, frozen blocks can still have empty slots within them. To make sure
        // these memory are not gone forever, we still need to periodically shuffle tuples around within
        // frozen blocks. Although code can be reused for doing the compaction, some logic needs to be
        // written to enqueue these frozen blocks into the compaction queue.
        cg.blocks_to_compact_.emplace(block, std::vector<uint32_t>());
        if (EliminateGaps(&cg)) {
          controller.GetBlockState()->store(BlockState::COOLING);
          // If no compaction was performed, we still need to shut out any potentially racey transactions that
          // are alive at the same time as us flipping the block status flag to cooling. However, we must manually
          // ask the GC to enqueue this block, because no access will be observed from the empty compaction transaction.
          if (cg.txn_->IsReadOnly())
            deferred_action_manager->RegisterDeferredAction([this, block]() { PutInQueue(block); });
          txn_manager->Commit(cg.txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
        } else {
          txn_manager->Abort(cg.txn_);
        }
        break;
      }
      case BlockState::COOLING: {
        if (!CheckForVersionsAndGaps(block->data_table_->accessor_, block)) continue;
        // This is used to clean up any dangling pointers using a deferred action in GC.
        // We need this piece of memory to live on the heap, so its life time extends to
        // beyond this function call.
        auto *loose_ptrs = new std::vector<const byte *>;
        GatherVarlens(loose_ptrs, block, block->data_table_);
        controller.GetBlockState()->store(BlockState::FROZEN);
        // When the old variable length values are no longer visible by running transactions, delete them.
        deferred_action_manager->RegisterDeferredAction([=]() {
          for (auto *loose_ptr : *loose_ptrs) delete[] loose_ptr;
          delete loose_ptrs;
        });
        break;
      }
      case BlockState::FROZEN:
        // This is okay. In a rare race, the block can show up in the compaction queue, be accessed, compacted,
        // and show up again because of the early access.
        break;
      default:
        throw std::runtime_error("unexpected control flow");
    }
    to_process.pop();
  }
}

bool BlockCompactor::EliminateGaps(CompactionGroup *cg) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // This will identify all the present and deleted tuples in a first pass. This should only scan through the bitmap
  // portion of the data. The system writes down the empty slots for every block.
  for (auto &entry : cg->blocks_to_compact_) {
    RawBlock *block = entry.first;
    std::vector<uint32_t> &empty_slots = entry.second;
    NOISEPAGE_ASSERT(block->GetInsertHead() == layout.NumSlots(),
                     "The block should be full to stop inserts from coming in");

    // We will loop through each block and figure out if we are safe to proceed with compaction and identify
    // any gaps
    auto *bitmap = accessor.AllocationBitmap(block);
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++)
      if (!bitmap->Test(offset)) empty_slots.push_back(offset);
  }

  // Within a group, we can calculate the number of blocks exactly we need to store all the filled tuples (we may
  // or may not need to throw away extra blocks when we are done compacting). Then, the algorithm involves selecting
  // the blocks with the least number of empty slots as blocks to "fill into", and the rest as blocks to "take away
  // from". These are not two disjoint sets as we will probably need to shuffle tuples within one block (but only one
  // block within a group needs this)
  std::vector<RawBlock *> all_blocks;
  for (auto &entry : cg->blocks_to_compact_) all_blocks.push_back(entry.first);

  // Sort all the blocks within a group based on the number of filled slots, in descending order.
  std::sort(all_blocks.begin(), all_blocks.end(), [&](RawBlock *a, RawBlock *b) {
    // We know these finds will not return end() because we constructed the vector from the map
    auto a_empty = cg->blocks_to_compact_[a].size();
    auto b_empty = cg->blocks_to_compact_[b].size();
    return a_empty < b_empty;
  });

  cg->all_cols_initializer_.InitializeRow(cg->read_buffer_);
  // We assume that there are a lot more filled slots than empty slots, so we only store the list of empty slots
  // and construct the vector of filled slots on the fly in order to reduce the memory footprint.
  std::vector<uint32_t> filled;
  // Because we constructed the filled list from sequential scan, slots will always appear in order. We
  // essentially will fill gaps in order, by using the real tuples in reverse order. (Take the last tuple to
  // fill the first empty slot)
  for (auto taker = all_blocks.begin(), giver = all_blocks.end(); taker <= giver && taker != all_blocks.end();
       taker++) {
    // Again, we know these finds will not return end() because we constructed the vector from the map
    std::vector<uint32_t> &taker_empty = cg->blocks_to_compact_.find(*taker)->second;

    for (uint32_t empty_offset : taker_empty) {
      if (filled.empty()) {
        giver--;
        ComputeFilled(layout, &filled, cg->blocks_to_compact_.find(*giver)->second);
      }
      TupleSlot empty_slot(*taker, empty_offset);
      // fill the first empty slot with the last filled slot, essentially
      // We will only shuffle tuples within a block if it is the last block to compact. Then, we can stop
      TupleSlot filled_slot(*giver, filled.back());
      filled.pop_back();
      // when the next empty slot is logically after the next filled slot (which implies we are processing
      // an empty slot that would be empty in a compact block)
      if (taker == giver && filled_slot.GetOffset() < empty_slot.GetOffset()) break;
      // A failed move implies conflict
      if (!MoveTuple(cg, filled_slot, empty_slot)) return false;
    }
  }

  // TODO(Tianyu): This compaction process could leave blocks empty within a group and we will need to figure out
  // how those blocks are garbage collected. These blocks should have the same life-cycle as the compacting
  // transaction itself. (i.e. when the txn context is being GCed, we should be able to free these blocks as well)
  // For now we are not implementing this because each compaction group is one block. This suggests the use of
  // deferred action.
  return true;
}

// TODO(Tianyu): Eventually this needs to be rewritten to use the insert and delete executors, so indexes are
// handled correctly.
bool BlockCompactor::MoveTuple(CompactionGroup *cg, TupleSlot from, TupleSlot to) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // Read out the tuple to copy

  if (!cg->table_->Select(common::ManagedPointer(cg->txn_), from, cg->read_buffer_)) return false;
  // TODO(Tianyu): FIXME
  // This is a relic from the days when the logs interacted directly with the DataTable. Since we changed the log
  // records to only have oids, the Compactor no longer has the relevant information to directly construct
  // log records.
  //
  // The right thing to do is this: Instead of directly manipulating the DataTable to do a move, the Compactor needs
  // to go through the query engine logic to get a plan and execute said plan. It probably needs to implement its
  // own special operators that takes physical locations instead of predicates, but it should share logic with the rest
  // of the system when it comes to index updates and such.
  RedoRecord *record = cg->txn_->StageWrite(catalog::db_oid_t(0), catalog::table_oid_t(0), cg->all_cols_initializer_);
  // We recast record->Delta() as a workaround for -Wclass-memaccess
  std::memcpy(static_cast<void *>(record->Delta()), cg->read_buffer_, cg->all_cols_initializer_.ProjectedRowSize());

  // Because the GC will assume all varlen pointers are unique and deallocate the same underlying
  // varlen for every update record, we need to mark subsequent records that reference the same
  // varlen value as not reclaimable so as to not double-free
  for (col_id_t varlen_col_id : layout.Varlens()) {
    // We know this to be true because the projection list has all columns
    auto offset = static_cast<uint16_t>(varlen_col_id.UnderlyingValue() - NUM_RESERVED_COLUMNS);
    auto *entry = reinterpret_cast<VarlenEntry *>(record->Delta()->AccessWithNullCheck(offset));
    if (entry == nullptr) continue;
    if (entry->Size() <= VarlenEntry::InlineThreshold()) {
      *entry = VarlenEntry::CreateInline(entry->Content(), entry->Size());
    } else {
      // TODO(Tianyu): Copying for correctness. This is not yet shown to be expensive, but might be in the future.
      byte *copied = common::AllocationUtil::AllocateAligned(entry->Size());
      std::memcpy(copied, entry->Content(), entry->Size());
      *entry = VarlenEntry::Create(copied, entry->Size(), true);
    }
  }

  // Copy the tuple into the empty slot
  // This operation cannot fail since a logically deleted slot can only be reclaimed by the compaction thread
  accessor.Reallocate(to);
  cg->table_->InsertInto(common::ManagedPointer(cg->txn_), *record->Delta(), to);

  // The delete can fail if a concurrent transaction is updating said tuple. We will have to abort if this is
  // the case.
  return cg->table_->Delete(common::ManagedPointer(cg->txn_), from);
}

bool BlockCompactor::CheckForVersionsAndGaps(const TupleAccessStrategy &accessor, RawBlock *block) {
  const BlockLayout &layout = accessor.GetBlockLayout();

  auto *allocation_bitmap = accessor.AllocationBitmap(block);
  auto *version_ptrs = reinterpret_cast<UndoRecord **>(accessor.ColumnStart(block, VERSION_POINTER_COLUMN_ID));
  // We will loop through each block and figure out if any versions showed up between our current read and the
  // earlier read
  uint32_t num_records = layout.NumSlots();
  bool unallocated_region_start = false;
  for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
    if (!allocation_bitmap->Test(offset)) {
      // This slot is unallocated
      if (!unallocated_region_start) {
        // Mark current reason as empty. The transformation process should abort if we see an allocated
        // slot after this
        unallocated_region_start = true;
        // If it is the first such slot, we should take down its offset, because that is the number
        // of tuples present in the block if the tuples are contiguous with that block.
        num_records = offset;
      }
      // Otherwise, skip
      continue;
    }

    // Not contiguous. If the code reaches here the slot must be allocated, and we have seen an unallocated slot before
    if (unallocated_region_start) {
      return false;
    }

    // Check that there are no versions alive
    auto *record = version_ptrs[offset];
    if (record != nullptr) {
      return false;
    }
  }
  // Check that no other transaction has modified the canary in the block header. If we fail it's okay
  // to leave the block header because someone else must have already flipped it to hot
  auto state = BlockState::COOLING;
  bool ret = block->controller_.GetBlockState()->compare_exchange_strong(state, BlockState::FREEZING);
  // At this point we are guaranteed to complete the transformation process. We can start modifying block
  // header in place.
  if (ret) accessor.GetArrowBlockMetadata(block).NumRecords() = num_records;
  return ret;
}

void BlockCompactor::GatherVarlens(std::vector<const byte *> *loose_ptrs, RawBlock *block, DataTable *table) {
  const TupleAccessStrategy &accessor = table->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  ArrowBlockMetadata &metadata = accessor.GetArrowBlockMetadata(block);

  for (col_id_t col_id : layout.AllColumns()) {
    common::RawConcurrentBitmap *column_bitmap = accessor.ColumnNullBitmap(block, col_id);
    if (!layout.IsVarlen(col_id)) {
      metadata.NullCount(col_id) = 0;
      // Only need to count null for non-varlens
      for (uint32_t i = 0; i < metadata.NumRecords(); i++)
        if (!column_bitmap->Test(i)) metadata.NullCount(col_id)++;
      continue;
    }

    // Otherwise, the column is varlen, need to first check what to do for it
    ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
    auto *values = reinterpret_cast<VarlenEntry *>(accessor.ColumnStart(block, col_id));
    switch (col_info.Type()) {
      case ArrowColumnType::GATHERED_VARLEN:
        CopyToArrowVarlen(loose_ptrs, &metadata, col_id, column_bitmap, &col_info, values);
        break;
      case ArrowColumnType::DICTIONARY_COMPRESSED:
        BuildDictionary(loose_ptrs, &metadata, col_id, column_bitmap, &col_info, values);
        break;
      default:
        throw std::runtime_error("unexpected control flow");
    }
  }
}

void BlockCompactor::CopyToArrowVarlen(std::vector<const byte *> *loose_ptrs, ArrowBlockMetadata *metadata,
                                       col_id_t col_id, common::RawConcurrentBitmap *column_bitmap,
                                       ArrowColumnInfo *col, VarlenEntry *values) {
  uint32_t varlen_size = 0;
  // Read through every tuple and update null count and total varlen size
  metadata->NullCount(col_id) = 0;
  for (uint32_t i = 0; i < metadata->NumRecords(); i++) {
    if (!column_bitmap->Test(i))
      // Update null count
      metadata->NullCount(col_id)++;
    else
      // count the total size of varlens
      varlen_size += values[i].Size();
  }

  // We cannot deallocate the old information yet, because entries in the table may point to values within
  // the old Arrow storage.
  ArrowVarlenColumn new_col(varlen_size, metadata->NumRecords() + 1);
  for (uint32_t i = 0, acc = 0; i < metadata->NumRecords(); i++) {
    new_col.Offsets()[i] = acc;
    if (!column_bitmap->Test(i)) continue;

    // Only do a gather operation if the column is varlen
    VarlenEntry &entry = values[i];
    std::memcpy(new_col.Values() + acc, entry.Content(), entry.Size());

    // Need to GC
    if (entry.NeedReclaim()) loose_ptrs->push_back(entry.Content());

    // Because this change does not change the logical content of the database, and reads of aligned qwords on
    // modern architectures are atomic anyways, this is still safe for possible concurrent readers. The deferred
    // event framework guarantees that readers will not read garbage.
    // TODO(Tianyu): This guarantee is only true when we fix https://github.com/cmu-db/noisepage/issues/402
    if (entry.Size() > VarlenEntry::InlineThreshold())
      entry = VarlenEntry::Create(new_col.Values() + acc, entry.Size(), false);
    acc += entry.Size();
  }
  new_col.Offsets()[metadata->NumRecords()] = new_col.ValuesLength();
  col->VarlenColumn() = std::move(new_col);
}

void BlockCompactor::BuildDictionary(std::vector<const byte *> *loose_ptrs, ArrowBlockMetadata *metadata,
                                     col_id_t col_id, common::RawConcurrentBitmap *column_bitmap, ArrowColumnInfo *col,
                                     VarlenEntry *values) {
  VarlenEntryMap<uint32_t> dictionary;
  // Read through every tuple and update null count and build the dictionary
  uint32_t varlen_size = 0;
  metadata->NullCount(col_id) = 0;
  for (uint32_t i = 0; i < metadata->NumRecords(); i++) {
    if (!column_bitmap->Test(i)) {
      // Update null count
      metadata->NullCount(col_id)++;
      continue;
    }
    auto ret = dictionary.emplace(values[i], 0);
    // If the string has not been seen before, should add it to dictionary when counting total length.
    if (ret.second) varlen_size += values[i].Size();
  }
  ArrowColumnInfo new_col_info;
  new_col_info.Type() = col->Type();
  new_col_info.Indices() = common::AllocationUtil::AllocateAligned<uint64_t>(metadata->NumRecords());
  auto &new_col = new_col_info.VarlenColumn() = {varlen_size, static_cast<uint32_t>(dictionary.size() + 1)};

  // TODO(Tianyu): This is retarded, but apparently you cannot retrieve the index of elements in your
  // c++ map in constant time. Thus we are resorting to primitive means to implement dictionary compression.
  // If anybody feels like it, we can hand-code our dictionary compression entirely or link in somebody's library
  std::vector<VarlenEntry> corpus;
  for (auto &entry : dictionary) corpus.push_back(entry.first);
  std::sort(corpus.begin(), corpus.end(), VarlenContentCompare());
  // Write the dictionary content to Arrow
  for (uint32_t i = 0, acc = 0; i < corpus.size(); i++) {
    VarlenEntry &entry = corpus[i];
    // write down the dictionary code for this entry
    dictionary[entry] = i;
    std::memcpy(new_col.Values() + acc, entry.Content(), entry.Size());
    new_col.Offsets()[i] = acc;
    acc += entry.Size();
  }
  new_col.Offsets()[corpus.size()] = new_col.ValuesLength();

  // Swing all references in the table to point there, and build the encoded column
  for (uint32_t i = 0; i < metadata->NumRecords(); i++) {
    if (!column_bitmap->Test(i)) continue;
    // Only do a gather operation if the column is varlen
    VarlenEntry &entry = values[i];
    // Need to GC
    if (entry.NeedReclaim()) loose_ptrs->push_back(entry.Content());
    uint64_t dictionary_code = new_col_info.Indices()[i] = dictionary[entry];

    byte *dictionary_word = new_col.Values() + new_col.Offsets()[dictionary_code];
    NOISEPAGE_ASSERT(memcmp(dictionary_word, entry.Content(), entry.Size()) == 0,
                     "varlen entry should be equal to the dictionary word it is encoded as ");
    // Similar to in CopyToArrowVarlen this is safe even when there are concurrent readers
    if (entry.Size() > VarlenEntry::InlineThreshold())
      entry = VarlenEntry::Create(dictionary_word, entry.Size(), false);
  }
  *col = std::move(new_col_info);
}

}  // namespace noisepage::storage
