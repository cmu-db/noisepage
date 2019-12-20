#include "storage/data_table.h"
#include <pthread.h>
#include <sys/stat.h>
#include <cstring>
#include <fstream>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/allocator.h"
#include "storage/block_access_controller.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

constexpr int32_t FLATBUF_CONTINUZATION = -1;
constexpr uint8_t ARROW_ALIGNMENT = 8;
constexpr char ALIGNMENT[8] = {0};
constexpr flatbuf::MetadataVersion METADATA_VERSION = flatbuf::MetadataVersion_V4;

DataTable::DataTable(BlockStore *const store, const BlockLayout &layout, const layout_version_t layout_version)
    : block_store_(store), layout_version_(layout_version), accessor_(layout) {
  TERRIER_ASSERT(layout.AttrSize(VERSION_POINTER_COLUMN_ID) == 8,
                 "First column must have size 8 for the version chain.");
  TERRIER_ASSERT(layout.NumColumns() > NUM_RESERVED_COLUMNS,
                 "First column is reserved for version info, second column is reserved for logical delete.");
  if (block_store_ != nullptr) {
    RawBlock *new_block = NewBlock();
    // insert block
    blocks_.push_back(new_block);
  }
  insertion_head_ = blocks_.begin();
}

DataTable::~DataTable() {
  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  for (RawBlock *block : blocks_) {
    StorageUtil::DeallocateVarlens(block, accessor_);
    for (col_id_t i : accessor_.GetBlockLayout().Varlens())
      accessor_.GetArrowBlockMetadata(block).GetColumnInfo(accessor_.GetBlockLayout(), i).Deallocate();
    block_store_->Release(block);
  }
}

bool DataTable::Select(terrier::transaction::TransactionContext *txn, terrier::storage::TupleSlot slot,
                       terrier::storage::ProjectedRow *out_buffer) const {
  data_table_counter_.IncrementNumSelect(1);
  return SelectIntoBuffer(txn, slot, out_buffer);
}

void DataTable::Scan(transaction::TransactionContext *const txn, SlotIterator *const start_pos,
                     ProjectedColumns *const out_buffer) const {
  // TODO(Tianyu): So far this is not that much better than tuple-at-a-time access,
  // but can be improved if block is read-only, or if we implement version synopsis, to just use std::memcpy when it's
  // safe
  uint32_t filled = 0;
  while (filled < out_buffer->MaxTuples() && *start_pos != end()) {
    ProjectedColumns::RowView row = out_buffer->InterpretAsRow(filled);
    const TupleSlot slot = **start_pos;
    // Only fill the buffer with valid, visible tuples
    if (SelectIntoBuffer(txn, slot, &row)) {
      out_buffer->TupleSlots()[filled] = slot;
      filled++;
    }
    ++(*start_pos);
  }
  out_buffer->SetNumTuples(filled);
}

DataTable::SlotIterator &DataTable::SlotIterator::operator++() {
  common::SpinLatch::ScopedSpinLatch guard(&table_->blocks_latch_);
  // Jump to the next block if already the last slot in the block.
  if (current_slot_.GetOffset() == table_->accessor_.GetBlockLayout().NumSlots() - 1) {
    ++block_;
    // Cannot dereference if the next block is end(), so just use nullptr to denote
    current_slot_ = {block_ == table_->blocks_.end() ? nullptr : *block_, 0};
  } else {
    current_slot_ = {*block_, current_slot_.GetOffset() + 1};
  }
  return *this;
}

DataTable::SlotIterator DataTable::end() const {  // NOLINT for STL name compability
  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  // TODO(Tianyu): Need to look in detail at how this interacts with compaction when that gets in.

  // The end iterator could either point to an unfilled slot in a block, or point to nothing if every block in the
  // table is full. In the case that it points to nothing, we will use the end-iterator of the blocks list and
  // 0 to denote that this is the case. This solution makes increment logic simple and natural.
  if (blocks_.empty()) return {this, blocks_.end(), 0};
  auto last_block = --blocks_.end();
  uint32_t insert_head = (*last_block)->GetInsertHead();
  // Last block is full, return the default end iterator that doesn't point to anything
  if (insert_head == accessor_.GetBlockLayout().NumSlots()) return {this, blocks_.end(), 0};
  // Otherwise, insert head points to the slot that will be inserted next, which would be exactly what we want.
  return {this, last_block, insert_head};
}

bool DataTable::Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) {
  TERRIER_ASSERT(redo.NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The input buffer cannot change the reserved columns, so it should have fewer attributes.");
  TERRIER_ASSERT(redo.NumColumns() > 0, "The input buffer should modify at least one attribute.");
  UndoRecord *const undo = txn->UndoRecordForUpdate(this, slot, redo);
  slot.GetBlock()->controller_.WaitUntilHot();
  UndoRecord *version_ptr;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);

    // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
    // write lock on the tuple.
    if (HasConflict(*txn, version_ptr) || !Visible(slot, accessor_)) {
      // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
      // TransactionManager's Rollback() and GC's Unlink logic
      undo->Table() = nullptr;
      return false;
    }

    // Store before-image before making any changes or grabbing lock
    for (uint16_t i = 0; i < undo->Delta()->NumColumns(); i++)
      StorageUtil::CopyAttrIntoProjection(accessor_, slot, undo->Delta(), i);

    // Update the next pointer of the new head of the version chain
    undo->Next() = version_ptr;
  } while (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo));

  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    TERRIER_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Input buffer should not change the version pointer column.");
    // TODO(Matt): It would be nice to check that a ProjectedRow that modifies the logical delete column only originated
    // from the DataTable calling Update() within Delete(), rather than an outside soure modifying this column, but
    // that's difficult with this implementation
    StorageUtil::CopyAttrFromProjection(accessor_, slot, redo, i);
  }
  data_table_counter_.IncrementNumUpdate(1);

  return true;
}

void DataTable::CheckMoveHead(std::list<RawBlock *>::iterator block) {
  // Assume block is full
  common::SpinLatch::ScopedSpinLatch guard_head(&header_latch_);
  if (block == insertion_head_) {
    // If the header block is full, move the header to point to the next block
    insertion_head_++;
  }

  // If there are no more free blocks, create a new empty block and  point the insertion_head to it
  if (insertion_head_ == blocks_.end()) {
    RawBlock *new_block = NewBlock();
    // take latch
    common::SpinLatch::ScopedSpinLatch guard_block(&blocks_latch_);
    // insert block
    blocks_.push_back(new_block);
    // set insertion header to --end()
    insertion_head_ = --blocks_.end();
  }
}

TupleSlot DataTable::Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo) {
  TERRIER_ASSERT(redo.NumColumns() == accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The input buffer never changes the version pointer column, so it should have  exactly 1 fewer "
                 "attribute than the DataTable's layout.");

  // Insertion header points to the first block that has free tuple slots
  // Once a txn arrives, it will start from the insertion header to find the first
  // idle (no other txn is trying to get tuple slots in that block) and non-full block.
  // If no such block is found, the txn will create a new block.
  // Before the txn writes to the block, it will set block status to busy.
  // The first bit of block insert_head_ is used to indicate if the block is busy
  // If the first bit is 1, it indicates one txn is writing to the block.

  TupleSlot result;
  auto block = insertion_head_;
  while (true) {
    // No free block left
    if (block == blocks_.end()) {
      RawBlock *new_block = NewBlock();
      TERRIER_ASSERT(accessor_.SetBlockBusyStatus(new_block), "Status of new block should not be busy");
      // No need to flip the busy status bit
      accessor_.Allocate(new_block, &result);
      // take latch
      common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
      // insert block
      blocks_.push_back(new_block);
      block = --blocks_.end();
      break;
    }

    if (accessor_.SetBlockBusyStatus(*block)) {
      // No one is inserting into this block
      if (accessor_.Allocate(*block, &result)) {
        // The block is not full, succeed
        break;
      }
      // Fail to insert into the block, flip back the status bit
      accessor_.ClearBlockBusyStatus(*block);
      // if the full block is the insertion_header, move the insertion_header
      // Next insert txn will search from the new insertion_header
      CheckMoveHead(block);
    }
    // The block is full or the block is being inserted by other txn, try next block
    ++block;
  }

  // Do not need to wait unit finish inserting,
  // can flip back the status bit once the thread gets the allocated tuple slot
  accessor_.ClearBlockBusyStatus(*block);
  InsertInto(txn, redo, result);

  data_table_counter_.IncrementNumInsert(1);
  return result;
}

void DataTable::InsertInto(transaction::TransactionContext *txn, const ProjectedRow &redo, TupleSlot dest) {
  TERRIER_ASSERT(accessor_.Allocated(dest), "destination slot must already be allocated");
  TERRIER_ASSERT(accessor_.IsNull(dest, VERSION_POINTER_COLUMN_ID),
                 "The slot needs to be logically deleted to every running transaction");
  // At this point, sequential scan down the block can still see this, except it thinks it is logically deleted if we 0
  // the primary key column
  UndoRecord *undo = txn->UndoRecordForInsert(this, dest);
  TERRIER_ASSERT(dest.GetBlock()->controller_.GetBlockState()->load() == BlockState::HOT,
                 "Should only be able to insert into hot blocks");
  AtomicallyWriteVersionPtr(dest, accessor_, undo);
  // Set the logically deleted bit to present as the undo record is ready
  accessor_.AccessForceNotNull(dest, VERSION_POINTER_COLUMN_ID);
  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    TERRIER_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Insert buffer should not change the version pointer column.");
    StorageUtil::CopyAttrFromProjection(accessor_, dest, redo, i);
  }
}

bool DataTable::Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
  data_table_counter_.IncrementNumDelete(1);
  UndoRecord *const undo = txn->UndoRecordForDelete(this, slot);
  slot.GetBlock()->controller_.WaitUntilHot();
  UndoRecord *version_ptr;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
    // write lock on the tuple.
    if (HasConflict(*txn, version_ptr) || !Visible(slot, accessor_)) {
      // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
      // TransactionManager's Rollback() and GC's Unlink logic
      undo->Table() = nullptr;
      return false;
    }

    // Update the next pointer of the new head of the version chain
    undo->Next() = version_ptr;
  } while (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo));

  // We have the write lock. Go ahead and flip the logically deleted bit to true
  accessor_.SetNull(slot, VERSION_POINTER_COLUMN_ID);
  return true;
}

void DataTable::WriteDataBlock(std::ofstream &outfile, const char *src, size_t len) {
  len = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, len);
  outfile.write(src, len);
}

void DataTable::AddBufferInfo(size_t *offset, size_t len, std::vector<flatbuf::Buffer> *buffers) {
  len = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, len);
  buffers->emplace_back(*offset, len);
  *offset += len;
}

void DataTable::AssembleMetadataBuffer(std::ofstream &outfile,
                                       flatbuf::MessageHeader header_type,
                                       flatbuffers::Offset<void> header,
                                       int64_t body_len,
                                       flatbuffers::FlatBufferBuilder *flatbuf_builder) {
  auto message = flatbuf::CreateMessage(*flatbuf_builder, METADATA_VERSION, header_type,
                                        header, body_len);
  flatbuf_builder->Finish(message);
  int32_t flatbuf_size = flatbuf_builder->GetSize();
  auto padded_flatbuf_size = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, flatbuf_size);
  outfile.write(reinterpret_cast<const char *>(&FLATBUF_CONTINUZATION), sizeof(int32_t));
  outfile.write(reinterpret_cast<const char *>(&padded_flatbuf_size), sizeof(int32_t));
  outfile.write(reinterpret_cast<const char *>(flatbuf_builder->GetBufferPointer()), flatbuf_size);
  if (padded_flatbuf_size != static_cast<uint32_t>(flatbuf_size)) {
    outfile.write(ALIGNMENT, padded_flatbuf_size - flatbuf_size);
  }
  outfile.flush();
}

void DataTable::WriteSchemaMessage(std::ofstream &outfile, std::unordered_map<col_id_t, int64_t> *dictionary_ids,
                                   flatbuffers::FlatBufferBuilder *flatbuf_builder) {
  RawBlock *block = blocks_.front();
  const BlockLayout &layout = accessor_.GetBlockLayout();
  ArrowBlockMetadata &metadata = accessor_.GetArrowBlockMetadata(block);
  std::vector<flatbuffers::Offset<flatbuf::Field>> fields;
  int64_t dictionary_id = 0;

  // For each column, write its metadata into the schema message that will be parsed at the very beginning when
  // reading a exported table file.
  //
  // Such metadata includes:
  // 1. Name of the column;
  // 2. Logical Type of the column (fixed length, varlen, or dictionary compressed);
  //    2.1. If it's fixed length, then its byte width will be included;
  //    2.2. If it's dictionary compressed, then we pre-assign an id for the dictionary. Its real dictionary will
  //         be built later with the id.
  for (col_id_t col_id : layout.AllColumns()) {
    ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
    // TODO(Yuze): Change column name when we have the information, which we may need from upper layers.
    auto name = flatbuf_builder->CreateString("Col" + std::to_string(!col_id));
    flatbuf::Type type;
    flatbuffers::Offset<void> type_offset;
    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary = 0;
    if (!layout.IsVarlen(col_id) || col_info.Type() == ArrowColumnType::FIXED_LENGTH) {
      uint8_t byte_width = accessor_.GetBlockLayout().AttrSize(col_id);
      type = flatbuf::Type_FixedSizeBinary;
      type_offset = flatbuf::CreateFixedSizeBinary(*flatbuf_builder, byte_width).Union();
    } else {
      switch (col_info.Type()) {
        case ArrowColumnType::DICTIONARY_COMPRESSED:
          dictionary = flatbuf::CreateDictionaryEncoding(
              *flatbuf_builder, dictionary_id, flatbuf::CreateInt(*flatbuf_builder, 8 * sizeof(uint64_t), true), false);
          dictionary_ids->emplace(col_id, dictionary_id++);
          TERRIER_FALLTHROUGH;
        case ArrowColumnType::GATHERED_VARLEN:
          type = flatbuf::Type_LargeBinary;
          type_offset = flatbuf::CreateLargeBinary(*flatbuf_builder).Union();
          break;
        default:
          throw std::runtime_error("unexpected control flow");
      }
    }

    // Apache Arrow supports nested logical types. For example, for type List<Int64>, the parent type is List,
    // and its children type is Int64. Another example, for type List<List<Int64>>, List<Int64> is the children
    // of the outer List, and Int64 is the children of the inner List. Since we don't have nested types, we use
    // an empty array as fake children.
    std::vector<flatbuffers::Offset<flatbuf::Field>> fake_children;
    fields.emplace_back(flatbuf::CreateField(*flatbuf_builder, name, true, type, type_offset, dictionary,
                                             flatbuf_builder->CreateVector(fake_children)));
  }


  auto schema =
      flatbuf::CreateSchema(*flatbuf_builder, flatbuf::Endianness_Little, flatbuf_builder->CreateVector(fields));
  AssembleMetadataBuffer(outfile, flatbuf::MessageHeader_Schema, schema.Union(), 0, flatbuf_builder);
}

void DataTable::WriteDictionaryMessage(std::ofstream &outfile, int64_t dictionary_id,
                                       const ArrowVarlenColumn &varlen_col,
                                       flatbuffers::FlatBufferBuilder *flatbuf_builder) {
  std::vector<flatbuf::FieldNode> field_nodes;
  std::vector<flatbuf::Buffer> buffers;
  uint32_t num_elements = varlen_col.OffsetsLength() - 1;
  size_t buffer_offset = 0;
  field_nodes.emplace_back(num_elements, 0);

  // Add a fake validity buffer. This is required by flatbuffer. The dictionary message is
  // actually a warpped recordbatch message, i.e., the dictionary entries are written
  // in one RecordBatch. RecordBatch is something requires a validity buffer
  buffers.emplace_back(buffer_offset, 0);

  AddBufferInfo(&buffer_offset, varlen_col.OffsetsLength() * sizeof(uint64_t), &buffers);

  AddBufferInfo(&buffer_offset, varlen_col.ValuesLength(), &buffers);

  auto record_batch =
      flatbuf::CreateRecordBatch(*flatbuf_builder, num_elements, flatbuf_builder->CreateVectorOfStructs(field_nodes),
                                 flatbuf_builder->CreateVectorOfStructs(buffers));
  auto dictionary_batch = flatbuf::CreateDictionaryBatch(*flatbuf_builder, dictionary_id, record_batch);
  auto aligned_offset = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, buffer_offset);
  AssembleMetadataBuffer(outfile,
                         flatbuf::MessageHeader_DictionaryBatch,
                         dictionary_batch.Union(),
                         aligned_offset,
                         flatbuf_builder);
  WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Offsets()),
                 varlen_col.OffsetsLength() * sizeof(uint64_t));

  WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Values()), varlen_col.ValuesLength());

  outfile.flush();
}

void DataTable::ExportTable(const std::string &file_name) {
  flatbuffers::FlatBufferBuilder flatbuf_builder;
  std::ofstream outfile(file_name, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
  std::unordered_map<col_id_t, int64_t> dictionary_ids;
  WriteSchemaMessage(outfile, &dictionary_ids, &flatbuf_builder);

  const BlockLayout &layout = accessor_.GetBlockLayout();
  auto column_ids = layout.AllColumns();
  blocks_latch_.Lock();
  std::list<RawBlock *> tmp_blocks = blocks_;
  blocks_latch_.Unlock();

  for (RawBlock *block : tmp_blocks) {
    std::vector<flatbuf::FieldNode> field_nodes;
    std::vector<flatbuf::Buffer> buffers;

    // Make sure varlen columns have correct data when reading
    while (!block->controller_.TryAcquireInPlaceRead()) {
    }
    ArrowBlockMetadata &metadata = accessor_.GetArrowBlockMetadata(block);
    uint32_t num_slots = metadata.NumRecords();

    size_t buffer_offset = 0;
    size_t column_id_size = column_ids.size();

    // First pass, write metadata_flatbuffer
    for (size_t i = 0; i < column_id_size; ++i) {
      auto col_id = column_ids[i];
      common::RawConcurrentBitmap *column_bitmap = accessor_.ColumnNullBitmap(block, col_id);
      std::byte *column_start = accessor_.ColumnStart(block, col_id);

      ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
      field_nodes.emplace_back(num_slots, metadata.NullCount(col_id));

      AddBufferInfo(&buffer_offset,
                    reinterpret_cast<uintptr_t>(column_start) - reinterpret_cast<uintptr_t>(column_bitmap), &buffers);
      if (layout.IsVarlen(col_id) && !(col_info.Type() == ArrowColumnType::FIXED_LENGTH)) {
        switch (col_info.Type()) {
          case ArrowColumnType::GATHERED_VARLEN: {
            ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
            AddBufferInfo(&buffer_offset, varlen_col.OffsetsLength() * sizeof(uint64_t), &buffers);
            AddBufferInfo(&buffer_offset, varlen_col.ValuesLength(), &buffers);
            break;
          }
          case ArrowColumnType::DICTIONARY_COMPRESSED: {
            ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
            WriteDictionaryMessage(outfile, dictionary_ids[col_id], varlen_col, &flatbuf_builder);
            AddBufferInfo(&buffer_offset, num_slots * sizeof(uint64_t), &buffers);
            break;
          }
          default:
            throw std::runtime_error("unexpected control flow");
        }
      } else {
        int32_t cur_buffer_len;
        // Calculate the length of the data region of current column. For the columns except the last one, we calculate
        // their length by using the start of next column's bit map - the start of current column's data. For the
        // last column, we calculate the length by using the beginning address of the next block - the start of current
        // column data.
        if (i == column_id_size - 1) {
          auto casted_column_start = reinterpret_cast<uintptr_t>(column_start);
          uintptr_t mask = common::Constants::BLOCK_SIZE - 1;
          cur_buffer_len = ((casted_column_start + mask) & (~mask)) - casted_column_start;
        } else {
          cur_buffer_len = reinterpret_cast<uintptr_t>(accessor_.ColumnNullBitmap(block, column_ids[i + 1])) -
                           reinterpret_cast<uintptr_t>(column_start);
        }
        AddBufferInfo(&buffer_offset, cur_buffer_len, &buffers);
      }
    }
    auto record_batch =
        flatbuf::CreateRecordBatch(flatbuf_builder, num_slots, flatbuf_builder.CreateVectorOfStructs(field_nodes),
                                   flatbuf_builder.CreateVectorOfStructs(buffers));
    auto aligned_offset = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, buffer_offset);
    AssembleMetadataBuffer(outfile,
                           flatbuf::MessageHeader_RecordBatch,
                           record_batch.Union(),
                           aligned_offset,
                           &flatbuf_builder);

    // Second pass, write data
    for (size_t i = 0; i < column_id_size; ++i) {
      auto col_id = column_ids[i];
      common::RawConcurrentBitmap *column_bitmap = accessor_.ColumnNullBitmap(block, col_id);
      std::byte *column_start = accessor_.ColumnStart(block, col_id);

      ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
      field_nodes.emplace_back(num_slots, metadata.NullCount(col_id));

      WriteDataBlock(outfile, reinterpret_cast<const char *>(column_bitmap),
                     reinterpret_cast<uintptr_t>(column_start) - reinterpret_cast<uintptr_t>(column_bitmap));

      if (layout.IsVarlen(col_id) && !(col_info.Type() == ArrowColumnType::FIXED_LENGTH)) {
        switch (col_info.Type()) {
          case ArrowColumnType::GATHERED_VARLEN: {
            ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
            WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Offsets()),
                           varlen_col.OffsetsLength() * sizeof(uint64_t));
            WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Values()), varlen_col.ValuesLength());
            break;
          }
          case ArrowColumnType::DICTIONARY_COMPRESSED: {
            auto indices = col_info.Indices();
            WriteDataBlock(outfile, reinterpret_cast<const char *>(indices), num_slots * sizeof(uint64_t));
            break;
          }
          default:
            throw std::runtime_error("unexpected control flow");
        }
      } else {
        int32_t cur_buffer_len;
        if (i == column_id_size - 1) {
          auto casted_column_start = reinterpret_cast<uintptr_t>(column_start);
          uintptr_t mask = common::Constants::BLOCK_SIZE - 1;
          cur_buffer_len = ((casted_column_start + mask) & (~mask)) - casted_column_start;
        } else {
          cur_buffer_len = reinterpret_cast<uintptr_t>(accessor_.ColumnNullBitmap(block, column_ids[i + 1])) -
                           reinterpret_cast<uintptr_t>(column_start);
        }
        WriteDataBlock(outfile, reinterpret_cast<const char *>(column_start), cur_buffer_len);
      }
    }
    outfile.flush();
    block->controller_.ReleaseInPlaceRead();
  }
  outfile.close();
}

template <class RowType>
bool DataTable::SelectIntoBuffer(transaction::TransactionContext *const txn, const TupleSlot slot,
                                 RowType *const out_buffer) const {
  TERRIER_ASSERT(out_buffer->NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");
  TERRIER_ASSERT(out_buffer->NumColumns() > 0, "The output buffer should return at least one attribute.");
  // This cannot be visible if it's already deallocated.
  if (!accessor_.Allocated(slot)) return false;

  UndoRecord *version_ptr;
  bool visible;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Copy the current (most recent) tuple into the output buffer. These operations don't need to be atomic,
    // because so long as we set the version ptr before updating in place, the reader will know if a conflict
    // can potentially happen, and chase the version chain before returning anyway,
    for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
      TERRIER_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                     "Output buffer should not read the version pointer column.");
      StorageUtil::CopyAttrIntoProjection(accessor_, slot, out_buffer, i);
    }

    // We still need to check the allocated bit because GC could have flipped it since last check
    visible = Visible(slot, accessor_);

    // Here we will need to check that the version pointer did not change during our read. If it did, the content
    // we have read might have been rolled back and an abort has already unlinked the associated undo-record,
    // we will have to loop around to avoid a dirty read.
    //
    // There is still an a-b-a problem if aborting transactions unlink themselves. Thus, in the system aborting
    // transactions still check out a timestamp and "commit" after rolling back their changes to guard against this,
    // The exact interleaving is this:
    //
    //      transaction 1         transaction 2
    //          begin
    //    read version_ptr
    //                                begin
    //                             write a -> a1
    //          read a1
    //                            rollback a1 -> a
    //    check version_ptr
    //         return a1
    //
    // For this to manifest, there has to be high contention on a given tuple slot, and insufficient CPU resources
    // (way more threads than there are cores, around 8x seems to work) such that threads are frequently swapped
    // out. compare-and-swap along with the pointer reduces the probability of this happening to be essentially
    // infinitesimal, but it's still a probabilistic fix. To 100% prevent this race, we have to wait until no
    // concurrent transaction with the abort that could have had a dirty read is alive to unlink this. The easiest
    // way to achieve that is to take a timestamp as well when all changes have been rolled back for an aborted
    // transaction, and let GC handle the unlinking.
  } while (version_ptr != AtomicallyReadVersionPtr(slot, accessor_));

  // Nullptr in version chain means no other versions visible to any transaction alive at this point.
  // Alternatively, if the current transaction holds the write lock, it should be able to read its own updates.
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn->FinishTime()) {
    return visible;
  }

  // Apply deltas until we reconstruct a version safe for us to read
  while (version_ptr != nullptr &&
         transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn->StartTime())) {
    switch (version_ptr->Type()) {
      case DeltaRecordType::UPDATE:
        // Normal delta to be applied. Does not modify the logical delete column.
        StorageUtil::ApplyDelta(accessor_.GetBlockLayout(), *(version_ptr->Delta()), out_buffer);
        break;
      case DeltaRecordType::INSERT:
        visible = false;
        break;
      case DeltaRecordType::DELETE:
        visible = true;
        break;
      default:
        throw std::runtime_error("unexpected delta record type");
    }
    version_ptr = version_ptr->Next();
  }

  return visible;
}

template bool DataTable::SelectIntoBuffer<ProjectedRow>(transaction::TransactionContext *txn, const TupleSlot slot,
                                                        ProjectedRow *const out_buffer) const;
template bool DataTable::SelectIntoBuffer<ProjectedColumns::RowView>(transaction::TransactionContext *txn,
                                                                     const TupleSlot slot,
                                                                     ProjectedColumns::RowView *const out_buffer) const;

UndoRecord *DataTable::AtomicallyReadVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  // Okay to ignore presence bit, because we use that for logical delete, not for validity of the version pointer value
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  return reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->load();
}

void DataTable::AtomicallyWriteVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor,
                                          UndoRecord *const desired) {
  // Okay to ignore presence bit, because we use that for logical delete, not for validity of the version pointer value
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->store(desired);
}

bool DataTable::Visible(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  const bool present = accessor.Allocated(slot);
  const bool not_deleted = !accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID);
  return present && not_deleted;
}

bool DataTable::HasConflict(const transaction::TransactionContext &txn, UndoRecord *const version_ptr) const {
  if (version_ptr == nullptr) return false;  // Nobody owns this tuple's write lock, no older version visible
  const transaction::timestamp_t version_timestamp = version_ptr->Timestamp().load();
  const transaction::timestamp_t txn_id = txn.FinishTime();
  const transaction::timestamp_t start_time = txn.StartTime();
  const bool owned_by_other_txn =
      (!transaction::TransactionUtil::Committed(version_timestamp) && version_timestamp != txn_id);
  const bool newer_committed_version = transaction::TransactionUtil::Committed(version_timestamp) &&
                                       transaction::TransactionUtil::NewerThan(version_timestamp, start_time);
  return owned_by_other_txn || newer_committed_version;
}

bool DataTable::CompareAndSwapVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor,
                                         UndoRecord *expected, UndoRecord *const desired) {
  // Okay to ignore presence bit, because we use that for logical delete, not for validity of the version pointer value
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  return reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->compare_exchange_strong(expected, desired);
}

RawBlock *DataTable::NewBlock() {
  RawBlock *new_block = block_store_->Get();
  accessor_.InitializeRawBlock(this, new_block, layout_version_);
  data_table_counter_.IncrementNumNewBlock(1);
  return new_block;
}

bool DataTable::HasConflict(const transaction::TransactionContext &txn, const TupleSlot slot) const {
  UndoRecord *const version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
  return HasConflict(txn, version_ptr);
}

bool DataTable::IsVisible(const transaction::TransactionContext &txn, const TupleSlot slot) const {
  UndoRecord *version_ptr;
  bool visible;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Here we will need to check that the version pointer did not change during our read. If it did, the visibility of
    // this tuple might have changed and we should check again.
    visible = Visible(slot, accessor_);
  } while (version_ptr != AtomicallyReadVersionPtr(slot, accessor_));

  // Nullptr in version chain means no other versions visible to any transaction alive at this point.
  // Alternatively, if the current transaction holds the write lock, it should be able to read its own updates.
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn.FinishTime()) {
    return visible;
  }

  // Apply deltas until we determine a version safe for us to read
  while (version_ptr != nullptr &&
         transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn.StartTime())) {
    switch (version_ptr->Type()) {
      case DeltaRecordType::UPDATE:
        // Normal delta to be applied. Does not modify the logical delete column.
        break;
      case DeltaRecordType::INSERT:
        visible = false;
        break;
      case DeltaRecordType::DELETE:
        visible = true;
    }
    version_ptr = version_ptr->Next();
  }

  return visible;
}

}  // namespace terrier::storage
