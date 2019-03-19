#include "storage/block_compactor.h"

namespace terrier::storage {
namespace {
// for empty callback
void NoOp(void */* unused */) {}
}

void BlockCompactor::ProcessCompactionQueue(transaction::TransactionManager *txn_manager) {
  std::forward_list<std::pair<RawBlock *, DataTable *>> to_process = std::move(compaction_queue_);

  for (auto &entry : to_process) {
    // Block can still be inserted into. Hands off.
    if (entry.first->insert_head_ != entry.second->accessor_.GetBlockLayout().NumSlots()) continue;
    BlockAccessController &controller = entry.first->controller_;
    // Then the last transactional access must be the compacting transaction, which we can safely ignore.
    if (controller.IsFrozen()) continue;
    // TODO(Tianyu): This is probably fine for now, but we will want to not only compact within a block
    // but also across blocks to eventually free up slots
    CompactionGroup cg(txn_manager->BeginTransaction(), entry.second);
    cg.AddBlock(entry.first);
    if (CheckForActiveVersionsAndGaps(&cg)) {
      if (EliminateGaps(&cg)) {
        if (GatherVarlens(&cg)) {
          Cleanup(&cg, true);
          controller.MarkFrozen();
          // We should not need to wait for logs to finish as this is purely internal
          txn_manager->Commit(cg.txn_, NoOp, nullptr);
          continue;
        }
      }
    }
    // Otherwise, we just abort the transaction and all changes will be undone.
    Cleanup(&cg, false);
    txn_manager->Abort(cg.txn_);
  }
}

bool BlockCompactor::CheckForActiveVersionsAndGaps(CompactionGroup *cg) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  for (auto &entry : cg->blocks_to_compact_) {
    RawBlock *block = entry.first;
    BlockCompactionTask &task = entry.second;
    TERRIER_ASSERT(block->insert_head_ == layout.NumSlots(),
                   "The block should be full to stop inserts from coming in");
    ArrowBlockMetadata &metadata = accessor.GetArrowBlockMetadata(block);
    // We will loop through each block and figure out if we are safe to proceed with compaction and identify
    // any gaps
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
      TupleSlot slot(block, offset);
      // TODO(Tianyu): Should we abort right away in the case of a conflict, or simply throw the offending block
      // out of the compaction group and proceed with the rest? Will not matter until we actually start making groups
      // larger than 1.
      // If there is a version pointer in the table, maybe it is not fully cold yet, so hands off
      if (cg->table_->AtomicallyReadVersionPtr(slot, accessor) != nullptr) return false;

      bool allocated = accessor.Allocated(slot);
      // A logically deleted column implies that some changes are happening since the GC put this block into the
      // compaction queue. We should not do anything to this block further.
      if (allocated && accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID)) return false;

      // Push this slots to be either in the list of empty slots of filled slots
      if (!allocated) {
        task.empty_.push_back(slot);
      } else {
        task.new_block_metadata_->NumRecords()++;
        task.filled_.push_back(slot);
        // If the tuple is present, we also need to count the size of all the varlens and nulls. This count needs to
        // be adjusted as we shuffle tuples around blocks to fill gaps and will not be final count.
        // TODO(Tianyu): Can modify to use scan instead of select for performance
        bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, slot, cg->read_buffer_);
        TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
        for (uint16_t i = 0; i < cg->read_buffer_->NumColumns(); i++) {
          col_id_t col_id = cg->read_buffer_->ColumnIds()[i];
          byte *attr = cg->read_buffer_->AccessWithNullCheck(i);
          if (attr == nullptr) {
            task.new_block_metadata_->NullCount(col_id)++;
            continue;
          }
          ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
          switch (col_info.type_) {
            case ArrowColumnType::GATHERED_VARLEN: {
              TERRIER_ASSERT(layout.IsVarlen(col_id), "Only varlens can be gathered");
              auto *varlen = reinterpret_cast<VarlenEntry *>(attr);
              task.total_varlen_sizes_[col_id] += varlen->Size();
              break;
            }
            case ArrowColumnType::DICTIONARY_COMPRESSED: {
              TERRIER_ASSERT(layout.IsVarlen(col_id), "Only varlens can be dictionary compressed");
              auto *varlen = reinterpret_cast<VarlenEntry *>(attr);
              auto &freq_count = task.dictionary_corpus_[col_id][*varlen];
              if (freq_count == 0) task.total_varlen_sizes_[col_id] += varlen->Size();
              freq_count++;
              break;
            }
            case ArrowColumnType::FIXED_LENGTH:break;  // no operation required
            default:throw std::runtime_error("Unexpected arrow column type");
          }
        }
      }
    }
  }
  return true;
}

bool BlockCompactor::EliminateGaps(CompactionGroup *cg) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // This initializes an update that will copy over all the relevant columns in this block
  // TODO(Tianyu): Fish out the thing from tests and use that
  std::vector<col_id_t> all_cols;
  for (uint16_t i = 1; i < layout.NumColumns(); i++) all_cols.emplace_back(i);

  // TODO(Tianyu): This process can probably be optimized further for the least amount of movements of tuples. But we
  // are probably close enough to optimal that it does not matter that much
  // Within a group, we can calculate the number of blocks exactly we need to store all the filled tuples (we may
  // or may not need to throw away extra blocks when we are done compacting). Then, the algorithm involves selecting
  // the blocks with the least number of empty slots as blocks to "fill into", and the rest as blocks to "take away
  // from". These are not two disjoint sets as we will probably need to shuffle tuples within one block to have
  // perfectly compact groups (but only one block within a group needs this)
  std::vector<RawBlock *> all_blocks;
  uint32_t num_filled = 0;
  for (auto &entry : cg->blocks_to_compact_) {
    all_blocks.push_back(entry.first);
    num_filled += static_cast<uint32_t>(entry.second.filled_.size());
  }
  // Sort all the blocks within a group based on the number of filled slots, indescending order.
  std::sort(all_blocks.begin(), all_blocks.end(), [&](RawBlock *a, RawBlock *b) {
    // We know these finds will not return end() because we constructed the vector from the map
    return cg->blocks_to_compact_.find(a)->second.filled_.size() >
        cg->blocks_to_compact_.find(b)->second.filled_.size();
  });

  // Because we constructed the two lists from sequential scan, slots will always appear in order. We
  // essentially will fill gaps in order, by using the real tuples in reverse order. (Take the last tuple to
  // fill the first empty slot)
  for (auto taker = all_blocks.begin(), giver = all_blocks.end() - 1; taker <= giver; taker++) {
    // Again, we know these finds will not return end() because we constructed the vector from the map
    BlockCompactionTask &taker_bct = cg->blocks_to_compact_.find(*taker)->second;
    BlockCompactionTask &giver_bct = cg->blocks_to_compact_.find(*giver)->second;
    for (TupleSlot empty_slot : taker_bct.empty_) {
      // fill the first empty slot with the last filled slot, essentially
      TupleSlot filled_slot = giver_bct.filled_.back();
      // We will only shuffle tuples within a block if it is the last block to compact. Then, we can stop
      // when the next empty slot is logically after the next filled slot (which implies we are processing
      // an empty slot that would be empty in a compact block)
      if (taker == giver && filled_slot.GetOffset() < empty_slot.GetOffset()) break;
      RedoRecord *record = cg->txn_->StageWrite(cg->table_, empty_slot, cg->all_cols_initializer_);
      bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, filled_slot, record->Delta());
      TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
      accessor.Reallocate(empty_slot);
      // This operation cannot fail since a logically deleted slot can only be reclaimed by the compaction thread
      cg->table_->InsertInto(cg->txn_, *record->Delta(), empty_slot);
      // The delete can fail if a concurrent transaction is updating said tuple. We will have to abort if this is
      // the case.
      if (!cg->table_->Delete(cg->txn_, filled_slot)) return false;
      giver_bct.filled_.pop_back();
      // Also update the total size of varlens in the blocks accordingly
      for (col_id_t col_id : layout.Varlens()) {
        // Since we initialized the projection list to be all the columns, we know this relation to hold.
        auto projection_list_index = static_cast<uint16_t>((!col_id) - NUM_RESERVED_COLUMNS);
        auto *varlen = reinterpret_cast<VarlenEntry *>(record->Delta()->AccessWithNullCheck(projection_list_index));
        if (varlen == nullptr) {
          taker_bct.new_block_metadata_->NullCount(col_id)++;
          giver_bct.new_block_metadata_->NullCount(col_id)--;
        } else {
          ArrowColumnInfo &col_info = taker_bct.new_block_metadata_->GetColumnInfo(layout, col_id);
          switch (col_info.type_) {
            case ArrowColumnType::GATHERED_VARLEN:giver_bct.total_varlen_sizes_[col_id] -= varlen->Size();
              taker_bct.total_varlen_sizes_[col_id] += varlen->Size();
              break;
            case ArrowColumnType::DICTIONARY_COMPRESSED: {
              auto &giver_freq_count = giver_bct.dictionary_corpus_[col_id][*varlen];
              giver_freq_count--;
              if (giver_freq_count == 0) {
                giver_bct.total_varlen_sizes_[col_id] -= varlen->Size();
                giver_bct.dictionary_corpus_[col_id].erase(*varlen);
              }
              auto &taker_freq_count = taker_bct.dictionary_corpus_[col_id][*varlen];
              if (taker_freq_count == 0) taker_bct.total_varlen_sizes_[col_id] += varlen->Size();
              taker_freq_count++;
              break;
            }
            case ArrowColumnType::FIXED_LENGTH:break;  // no operation required
            default:throw std::runtime_error("Unexpected arrow column type");
          }
        }
      }
      taker_bct.new_block_metadata_->NumRecords()++;
      giver_bct.new_block_metadata_->NumRecords()--;
      if (giver_bct.filled_.empty()) giver--;
    }
  }

  // TODO(Tianyu): This compaction process could leave blocks empty within a group and we will need to figure out
  // how those blocks are garbage collected. These blocks should have the same life-cycle as the compacting
  // transaction itself. (i.e. when the txn context is being GCed, we should be able to free these blocks as well)
  // For now we are not implementing this because each compaction group is one block.

  return true;
}

void BlockCompactor::AllocateBuffers(RawBlock *block,
                                     const BlockLayout &layout,
                                     BlockCompactor::BlockCompactionTask *bct) {
  for (col_id_t col_id : layout.Varlens()) {
    ArrowColumnInfo &col_info = bct->new_block_metadata_->GetColumnInfo(layout, col_id);
    switch (col_info.type_) {
      case ArrowColumnType::GATHERED_VARLEN:
        col_info.varlen_column_.Allocate(bct->new_block_metadata_->NumRecords(),
                                         bct->total_varlen_sizes_[col_id]);
        break;
      case ArrowColumnType::DICTIONARY_COMPRESSED:
        col_info.varlen_column_.Allocate(static_cast<uint32_t>(bct->dictionary_corpus_[col_id].size()),
                                         bct->total_varlen_sizes_[col_id]);
        col_info.indices_ = reinterpret_cast<uint32_t *>(
            common::AllocationUtil::AllocateAligned(bct->new_block_metadata_->NumRecords() * sizeof(uint32_t)));
        break;
      case ArrowColumnType::FIXED_LENGTH:break;
      default:throw std::runtime_error("Unexpected arrow column type");
    }
  }
}

void BlockCompactor::InitializeMappings(RawBlock *block,
                                        const BlockLayout &layout,
                                        BlockCompactor::BlockCompactionTask *bct,
                                        std::unordered_map<col_id_t, uint32_t> *acc,
                                        std::unordered_map<col_id_t, VarlenEntryMap<uint32_t>> *index_map) {
  for (col_id_t varlen_col_id : layout.Varlens()) {
    ArrowColumnInfo &col_info = bct->new_block_metadata_->GetColumnInfo(layout, varlen_col_id);
    switch (col_info.type_) {
      case ArrowColumnType::GATHERED_VARLEN:(*acc)[varlen_col_id] = 0;
        break;
      case ArrowColumnType::DICTIONARY_COMPRESSED: {
        std::vector<VarlenEntry> unique_varlens;
        for (const auto &entry : bct->dictionary_corpus_[varlen_col_id]) {
          unique_varlens.emplace_back(entry.first);
        }
        std::sort(unique_varlens.begin(), unique_varlens.end(), VarlenContentCompare());
        uint32_t offset = 0;
        for (uint32_t idx = 0; idx < unique_varlens.size(); idx++) {
          VarlenEntry &varlen = unique_varlens[idx];
          (*index_map)[varlen_col_id][varlen] = idx;
          col_info.varlen_column_.offsets_[idx] = offset;
          std::memcpy(col_info.varlen_column_.values_ + offset, varlen.Content(), varlen.Size());
          offset += varlen.Size();
        }
        TERRIER_ASSERT(offset == bct->total_varlen_sizes_[varlen_col_id],
                       "Final offset should be the same as the total size");
        col_info.varlen_column_.offsets_[unique_varlens.size()] = offset;
        break;
      }
      case ArrowColumnType::FIXED_LENGTH:break;
      default:throw std::runtime_error("Unexpected arrow column type");
    }
  }
}

bool BlockCompactor::UpdateVarlens(RawBlock *block,
                                   const BlockLayout &layout,
                                   BlockCompactor::BlockCompactionTask *bct,
                                   BlockCompactor::CompactionGroup *cg,
                                   std::unordered_map<col_id_t, uint32_t> *acc,
                                   std::unordered_map<col_id_t, VarlenEntryMap<uint32_t >> *index_map) {
  ProjectedRowInitializer varlens_initializer(layout, layout.Varlens());
  varlens_initializer.InitializeRow(cg->read_buffer_);  // Okay to reuse this buffer: it is guaranteed to be larger
  for (uint32_t offset = 0; offset < bct->new_block_metadata_->NumRecords(); offset++) {
    TupleSlot slot(block, offset);
    // we will need to copy varlen into the allocated buffer. If we cannot, there is a conflict and we should
    // abort.
    bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, slot, cg->read_buffer_);
    TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
    RedoRecord *update = cg->txn_->StageWrite(cg->table_, slot, varlens_initializer);
    for (uint16_t i = 0; i < cg->read_buffer_->NumColumns(); i++) {
      col_id_t col_id = cg->read_buffer_->ColumnIds()[i];
      TERRIER_ASSERT(layout.IsVarlen(col_id), "Read buffer should only have varlens");
      ArrowColumnInfo &col_info = bct->new_block_metadata_->GetColumnInfo(layout, col_id);
      auto *varlen = reinterpret_cast<VarlenEntry *>(cg->read_buffer_->AccessWithNullCheck(i));
      if (varlen == nullptr) {
        update->Delta()->SetNull(i);
      } else {
        switch (col_info.type_) {
          case ArrowColumnType::GATHERED_VARLEN: {
            // If the old varlen is not gathered, it will be GCed by the normal transaction code path. Otherwise,
            // we will replace and delete the old varlen buffer and we should be okay
            memcpy(col_info.varlen_column_.values_ + (*acc)[col_id], varlen->Content(), varlen->Size());
            *reinterpret_cast<VarlenEntry *>(update->Delta()->AccessForceNotNull(i)) =
                varlen->Size() > VarlenEntry::InlineThreshold()
                ? VarlenEntry::Create(col_info.varlen_column_.values_ + (*acc)[col_id], varlen->Size(), false)
                : VarlenEntry::CreateInline(col_info.varlen_column_.values_ + (*acc)[col_id], varlen->Size());
            (*acc)[col_id] += varlen->Size();
            break;
          }
          case ArrowColumnType::DICTIONARY_COMPRESSED: {
            uint32_t index = col_info.indices_[offset] = (*index_map)[col_id][*varlen];
            *reinterpret_cast<VarlenEntry *>(update->Delta()->AccessForceNotNull(i)) =
                varlen->Size() > VarlenEntry::InlineThreshold()
                ? VarlenEntry::Create(col_info.varlen_column_.values_ + col_info.varlen_column_.offsets_[index],
                                      varlen->Size(), false)
                : VarlenEntry::CreateInline(
                    col_info.varlen_column_.values_ + col_info.varlen_column_.offsets_[index], varlen->Size());
            break;
          }
          case ArrowColumnType::FIXED_LENGTH:break;
          default:throw std::runtime_error("Unexpected arrow column type");
        }
      }
    }
    if (!cg->table_->Update(cg->txn_, slot, *update->Delta())) return false;
  }
  return true;
}

bool BlockCompactor::GatherVarlens(CompactionGroup *cg) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  if (layout.Varlens().empty()) {
    // Although there are no varlens, We still need to install an empty update as lock. If we cannot, there is a
    // conflict and we should abort.
    for (auto &entry : cg->blocks_to_compact_) {
      RawBlock *block = entry.first;
      BlockCompactionTask &bct = entry.second;
      for (uint32_t offset = 0; offset < bct.new_block_metadata_->NumRecords(); offset++)
        if (!cg->table_->Lock(cg->txn_, {block, offset})) return false;
    }
  } else {
    // Varlens need to be copied.
    for (auto &entry : cg->blocks_to_compact_) {
      RawBlock *block = entry.first;
      BlockCompactionTask &bct = entry.second;
      // Allocates the buffers according to each column type.
      AllocateBuffers(block, layout, &bct);
      // This will accumulate the prefix sum of varlen sizes that we need as the offsets array for arrow
      std::unordered_map<col_id_t, uint32_t> acc;
      std::unordered_map<col_id_t, VarlenEntryMap<uint32_t>> index_map;
      // Initialize acc and index_map
      InitializeMappings(block, layout, &bct, &acc, &index_map);
      // Scan through all the tuples and copy them if there are varlens.
      if (!UpdateVarlens(block, layout, &bct, cg, &acc, &index_map)) {
        return false;  // Transaction could not safely update varlens.
      }
      // Need to write one last offsets to denote the end of the last varlen
      for (col_id_t col_id : layout.Varlens()) {
        ArrowColumnInfo &col_info = bct.new_block_metadata_->GetColumnInfo(layout, col_id);
        if (col_info.type_ != ArrowColumnType::GATHERED_VARLEN) continue;
        TERRIER_ASSERT(bct.total_varlen_sizes_[col_id] == acc[col_id],
                       "Prefix sum result should match previous calculation");
        col_info.varlen_column_.offsets_[bct.new_block_metadata_->NumRecords()] = acc[col_id];
      }
    }
  }
  return true;
}

void BlockCompactor::Cleanup(CompactionGroup *cg, bool successful) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  // There is no cleanup to do if there is no varlen in the compaction group
  if (layout.Varlens().empty()) return;

  for (auto &entry : cg->blocks_to_compact_) {
    RawBlock *block = entry.first;
    BlockCompactionTask &bct = entry.second;
    for (col_id_t varlen_col_id : layout.Varlens()) {
      // Depending on whether the compaction was successful, we either need to swap in the new metadata and
      // deallocate the old, or throw away the new one if compaction failed.
      ArrowColumnInfo &col = (successful ? accessor.GetArrowBlockMetadata(block) : *bct.new_block_metadata_)
          .GetColumnInfo(layout, varlen_col_id);
      cg->txn_->loose_ptrs_.emplace(reinterpret_cast<byte *>(col.indices_));
      cg->txn_->loose_ptrs_.emplace(reinterpret_cast<byte *>(col.varlen_column_.offsets_));
      cg->txn_->loose_ptrs_.emplace(col.varlen_column_.values_);
    }
    if (successful)
      memcpy(&accessor.GetArrowBlockMetadata(block), bct.new_block_metadata_,
             ArrowBlockMetadata::Size(layout.NumColumns()));
    // Safe to delete
    delete[] reinterpret_cast<byte *>(bct.new_block_metadata_);
  }
}

}  // namespace terrier::storage
