#include "storage/block_compactor.h"
#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/index/bwtree_index.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
namespace terrier::storage {
namespace {
// for empty callback
void NoOp(void * /* unused */) {}
}  // namespace

void BlockCompactor::ProcessCompactionQueue(transaction::TransactionManager *txn_manager) {
  std::forward_list<RawBlock *> to_process = std::move(compaction_queue_);
  CompactionGroup *cg = nullptr;
  for (auto &block : to_process) {
    BlockAccessController &controller = block->controller_;
    switch (controller.CurrentBlockState()) {
      case BlockState::HOT: {
        if (cg == nullptr) cg = new CompactionGroup(txn_manager->BeginTransaction(), block->data_table_);
        if (block->data_table_ != cg->table_) throw std::runtime_error("need to remove hack");
        // TODO(Tianyu): This is probably fine for now, but we will want to not only compact within a block
        // but also across blocks to eventually free up slots
        cg->blocks_to_compact_.emplace(block, std::vector<uint32_t>());
        break;
      }
      case BlockState::COOLING: {
        if (!CheckForVersionsAndGaps(block->data_table_->accessor_, block)) {
          continue;
        }
        // TODO(Tianyu): The use of transaction here is pretty sketchy
        transaction::TransactionContext *txn = txn_manager->BeginTransaction();
        GatherVarlens(txn, block, block->data_table_);
        controller.GetBlockState()->store(BlockState::FROZEN);
        txn_manager->Commit(txn, NoOp, nullptr);
        break;
      }
      case BlockState::FROZEN:
        // okay
        break;
      default:
        throw std::runtime_error("unexpected control flow");
    }
  }
  if (cg == nullptr) return;
  if (EliminateGaps(cg)) {
    // Has to mark block as cooling before transaction commit, so we have a guarantee that
    // any older transactions
    for (auto &entry : cg->blocks_to_compact_) {
      RawBlock *block = entry.first;
      BlockAccessController &controller = block->controller_;
      controller.GetBlockState()->store(BlockState::COOLING);
    }

    //    if (cg->txn_->IsReadOnly()) {
    //      cg->txn_->compacted_ = block;
    //      cg->txn_->table_ = block->data_table_;
    //    }
    txn_manager->Commit(cg->txn_, NoOp, nullptr);
  } else {
    txn_manager->Abort(cg->txn_);
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
    TERRIER_ASSERT(block->insert_head_ == layout.NumSlots(), "The block should be full to stop inserts from coming in");

    // We will loop through each block and figure out if we are safe to proceed with compaction and identify
    // any gaps
    auto *bitmap = accessor.AllocationBitmap(block);
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++)
      if (!bitmap->Test(offset)) empty_slots.push_back(offset);
  }

  // TODO(Tianyu): This process can probably be optimized further for the least amount of movements of tuples. But we
  // are probably close enough to optimal that it does not matter that much
  // Within a group, we can calculate the number of blocks exactly we need to store all the filled tuples (we may
  // or may not need to throw away extra blocks when we are done compacting). Then, the algorithm involves selecting
  // the blocks with the least number of empty slots as blocks to "fill into", and the rest as blocks to "take away
  // from". These are not two disjoint sets as we will probably need to shuffle tuples within one block to have
  // perfectly compact groups (but only one block within a group needs this)
  std::vector<RawBlock *> all_blocks;
  for (auto &entry : cg->blocks_to_compact_) all_blocks.push_back(entry.first);

  // Sort all the blocks within a group based on the number of filled slots, in descending order.
  std::sort(all_blocks.begin(), all_blocks.end(), [&](RawBlock *a, RawBlock *b) {
    auto a_empty = cg->blocks_to_compact_[a].size();
    auto b_empty = cg->blocks_to_compact_[b].size();
    // We know these finds will not return end() because we constructed the vector from the map
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
  // For now we are not implementing this because each compaction group is one block.
  return true;
}

bool BlockCompactor::MoveTuple(CompactionGroup *cg, TupleSlot from, TupleSlot to) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // Read out the tuple to copy

  if (!cg->table_->Select(cg->txn_, from, cg->read_buffer_)) return false;
  RedoRecord *record = cg->txn_->StageWrite(cg->table_, to, cg->all_cols_initializer_);
  std::memcpy(record->Delta(), cg->read_buffer_, cg->all_cols_initializer_.ProjectedRowSize());

  // Because the GC will assume all varlen pointers are unique and deallocate the same underlying
  // varlen for every update record, we need to mark subsequent records that reference the same
  // varlen value as not reclaimable so as to not double-free
  for (col_id_t varlen_col_id : layout.Varlens()) {
    // We know this to be true because the projection list has all columns
    auto offset = static_cast<uint16_t>(!varlen_col_id - NUM_RESERVED_COLUMNS);
    auto *entry = reinterpret_cast<VarlenEntry *>(record->Delta()->AccessWithNullCheck(offset));
    if (entry == nullptr) continue;
    if (entry->Size() <= VarlenEntry::InlineThreshold()) {
      *entry = VarlenEntry::CreateInline(entry->Content(), entry->Size());
    } else {
      // TODO(Tianyu): Copying for correctness. This can potentially be expensive
      byte *copied = common::AllocationUtil::AllocateAligned(entry->Size());
      std::memcpy(copied, entry->Content(), entry->Size());
      *entry = VarlenEntry::Create(copied, entry->Size(), true);
    }
  }

  // Copy the tuple into the empty slot
  // This operation cannot fail since a logically deleted slot can only be reclaimed by the compaction thread
  accessor.Reallocate(to);
  cg->table_->InsertInto(cg->txn_, *record->Delta(), to);

  // The delete can fail if a concurrent transaction is updating said tuple. We will have to abort if this is
  // the case.
  bool ret = cg->table_->Delete(cg->txn_, from);
  if (!ret) return false;
  tuples_moved_++;
  return true;
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

void BlockCompactor::GatherVarlens(transaction::TransactionContext *txn, RawBlock *block, DataTable *table) {
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
        CopyToArrowVarlen(txn, &metadata, col_id, column_bitmap, &col_info, values);
        break;
      case ArrowColumnType::DICTIONARY_COMPRESSED:
        BuildDictionary(txn, &metadata, col_id, column_bitmap, &col_info, values);
        break;
      default:
        throw std::runtime_error("unexpected control flow");
    }
  }
}

void BlockCompactor::CopyToArrowVarlen(transaction::TransactionContext *txn, ArrowBlockMetadata *metadata,
                                       col_id_t col_id, common::RawConcurrentBitmap *column_bitmap,
                                       ArrowColumnInfo *col, VarlenEntry *values) {
  uint32_t varlen_size = 0;
  // Read through every tuple and update null count and total varlen size
  for (uint32_t i = 0; i < metadata->NumRecords(); i++) {
    metadata->NullCount(col_id) = 0;
    if (!column_bitmap->Test(i))
      // Update null count
      metadata->NullCount(col_id)++;
    else
      // count the total size of varlens
      varlen_size += values[i].Size();
  }

  // TODO(Tianyu): Rewrite
  ArrowVarlenColumn new_col(varlen_size, metadata->NumRecords() + 1);

  for (uint32_t i = 0, acc = 0; i < metadata->NumRecords(); i++) {
    if (!column_bitmap->Test(i)) continue;
    // Only do a gather operation if the column is varlen
    VarlenEntry &entry = values[i];
    std::memcpy(new_col.Values() + acc, entry.Content(), entry.Size());
    new_col.Offsets()[i] = acc;

    // Need to GC
    if (entry.NeedReclaim()) txn->loose_ptrs_.push_back(entry.Content());

    // TODO(Tianyu): Describe why this is still safe
    if (entry.Size() > VarlenEntry::InlineThreshold())
      entry = VarlenEntry::Create(new_col.Values() + acc, entry.Size(), false);
    acc += entry.Size();
  }
  new_col.Offsets()[metadata->NumRecords()] = new_col.ValuesLength();
  col->VarlenColumn() = std::move(new_col);
}

void BlockCompactor::BuildDictionary(transaction::TransactionContext *txn, ArrowBlockMetadata *metadata,
                                     col_id_t col_id, common::RawConcurrentBitmap *column_bitmap, ArrowColumnInfo *col,
                                     VarlenEntry *values) {
  VarlenEntryMap<uint32_t> dictionary;
  // Read through every tuple and update null count and build the dictionary
  uint32_t varlen_size = 0;
  for (uint32_t i = 0; i < metadata->NumRecords(); i++) {
    metadata->NullCount(col_id) = 0;
    if (!column_bitmap->Test(i)) {
      // Update null count
      metadata->NullCount(col_id)++;
      continue;
    }
    auto ret = dictionary.emplace(values[i], 0);
    // If the string has not been seen before, should add it to dictionary when counting total length.
    if (ret.second) varlen_size += values[i].Size();
  }

  // TODO(Tianyu): Rewrite

  uint32_t *new_indices = common::AllocationUtil::AllocateAligned<uint32_t>(metadata->NumRecords());
  ArrowVarlenColumn new_col(varlen_size, metadata->NumRecords() + 1);

  // TODO(Tianyu): This is retarded, but apparently you cannot retrieve the index of elements in your
  // c++ map in constant time. Thus we are resorting to primitive means.
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
  new_col.Offsets()[metadata->NumRecords()] = new_col.ValuesLength();

  // Swing all references in the table to point there, and build the encoded column
  for (uint32_t i = 0; i < metadata->NumRecords(); i++) {
    if (!column_bitmap->Test(i)) continue;
    // Only do a gather operation if the column is varlen
    VarlenEntry &entry = values[i];
    // Need to GC
    if (entry.NeedReclaim()) txn->loose_ptrs_.push_back(entry.Content());
    uint32_t dictionary_code = new_indices[i] = dictionary[entry];

    byte *dictionary_word = new_col.Values() + new_col.Offsets()[dictionary_code];
    TERRIER_ASSERT(memcmp(dictionary_word, entry.Content(), entry.Size()) == 0,
                   "varlen entry should be equal to the dictionary word it is encoded as ");
    // TODO(Tianyu): Describe why this is still safe
    if (entry.Size() > VarlenEntry::InlineThreshold())
      entry = VarlenEntry::Create(dictionary_word, entry.Size(), false);
  }
  col->Deallocate();
  col->Indices() = new_indices;
  col->VarlenColumn() = std::move(new_col);
}

}  // namespace terrier::storage
