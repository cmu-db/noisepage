#pragma once
#include <vector>
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {
// TODO(Zhaozhe): Should fetch block_size. Use magic number first.
// The block size to output to disk every time
#define CHECKPOINT_BLOCK_SIZE 4096

/**
 * The header of a page in the checkpoint file.
 */
// TODO(Zhaozhe, Mengyang): More fields can be added to header
class CheckpointFilePage {
 public:
  /**
   * Initialize a given page
   * @param page to be initialized.
   */
  static void Initialize(CheckpointFilePage *page) {
    // TODO(mengyang): support non-trivial initialization
    page->checksum_ = 0;
    page->table_oid_ = catalog::table_oid_t(0);
  }

  /**
   * Get the checksum of the page.
   * @return checksum of the page.
   */
  uint32_t GetChecksum() { return checksum_; }

  /**
   * Set the checksum of the page.
   * @param checksum
   */
  void SetCheckSum(uint32_t checksum) { checksum_ = checksum; }

  /**
   * Get the oid of the table whose rows are stored in this page.
   * @return oid of the table.
   */
  catalog::table_oid_t GetTableOid() { return table_oid_; }

  /**
   * Set the oid of the table whose rows are stored in this page.
   * @param oid of the table.
   */
  void SetTableOid(catalog::table_oid_t oid) { table_oid_ = oid; }

  /**
   * Get the pointer to the first row in this page.
   * @return pointer to the first row.
   */
  byte *GetPayload() { return varlen_contents_; }

 private:
  uint32_t checksum_;
  catalog::table_oid_t table_oid_;
  byte varlen_contents_[0];
};

/**
 * A buffered tuple writer collects tuples into blocks, and output a block once a time.
 * The block size is fetched from database setting, and should be the page size.
 * layout:
 * ----------------------------------------------------------------------------------
 * | checksum | table_oid | tuple1 | varlen_entries(if exist) | tuple2 | tuple3 | ... |
 * ----------------------------------------------------------------------------------
 */
class BufferedTupleWriter {
  // TODO(Zhaozhe): checksum
 public:
  BufferedTupleWriter() = default;

  /**
   * Instantiate a new BufferedTupleWriter, open a new checkpoint file to write into.
   * @param log_file_path path to the checkpoint file.
   */
  explicit BufferedTupleWriter(const char *log_file_path)
      : out_(PosixIoWrappers::Open(log_file_path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR)),
        block_size_(CHECKPOINT_BLOCK_SIZE),
        buffer_(new byte[block_size_]()) {
    ResetBuffer();
  }

  /**
   * Open a file to write into
   * @param log_file_path path to the checkpoint file.
   */
  void Open(const char *log_file_path) {
    out_ = PosixIoWrappers::Open(log_file_path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    block_size_ = CHECKPOINT_BLOCK_SIZE;
    buffer_ = new byte[block_size_]();
    ResetBuffer();
  }

  /**
   * Write the content of the buffer into disk.
   */
  void Persist() { PersistBuffer(); }

  /**
   * Close current file and release the buffer.
   */
  void Close() {
    PosixIoWrappers::Close(out_);
    delete[] buffer_;
  }

  /**
   * Serialize the tuple into the buffer, and write to disk when the buffer is full.
   * @param row pointer to the RowView given by ProjectedColumn
   * @param row_buffer pointer to the ProjectedRow buffer.
   * @param layout of the Row.
   */
  void SerializeTuple(ProjectedColumns::RowView *row, ProjectedRow *row_buffer, const storage::BlockLayout &layout);

  /**
   * Serialize a ProjectedRow and its varlens into the file.
   * @param row_buffer pointer to the row to be serialized.
   * @param total_varlen total length of all varlens of this row.
   * @param varlen_entries all the varlen entries in this row.
   */
  void AppendTupleToBuffer(ProjectedRow *row_buffer, int32_t total_varlen,
                           const std::vector<const VarlenEntry *> &varlen_entries);

  /**
   * Get the current checkpoint page.
   * @return pointer to the checkpoint page header.
   */
  CheckpointFilePage *GetPage() { return reinterpret_cast<CheckpointFilePage *>(buffer_); }

 private:
  int out_;  // fd of the output files
  uint32_t block_size_;
  uint32_t cur_buffer_size_ = 0;
  byte *buffer_ = nullptr;

  void ResetBuffer() {
    memset(buffer_, 0, block_size_);
    CheckpointFilePage::Initialize(reinterpret_cast<CheckpointFilePage *>(buffer_));
    cur_buffer_size_ = sizeof(CheckpointFilePage);
  }

  void PersistBuffer() {
    // TODO(zhaozhe): calculate CHECKSUM. Currently using default 0 as checksum
    if (cur_buffer_size_ == sizeof(CheckpointFilePage)) {
      // If the buffer has no contents, just return
      return;
    }
    PosixIoWrappers::WriteFully(out_, buffer_, block_size_);
    ResetBuffer();
  }
};

/**
 * Reader class to read checkpoint file in pages (Checkpoint Page),
 * and provides basic interface similar to an iterator.
 */
class BufferedTupleReader {
 public:
  /**
   * Instantiate a new BufferedTupleReader to read from a checkpoint file.
   * @param log_file_path path to the checkpoint file.
   */
  explicit BufferedTupleReader(const char *log_file_path)
      : in_(PosixIoWrappers::Open(log_file_path, O_RDONLY)), buffer_(new byte[block_size_]()) {}

  /**
   * destruct the reader, close the file opened and release the buffer space.
   */
  ~BufferedTupleReader() {
    PosixIoWrappers::Close(in_);
    delete[] buffer_;
  }

  /**
   * Read the next block from the file into the buffer.
   * @return true if a page is read successfully. false, if EOF is encountered.
   */
  bool ReadNextBlock() {
    uint32_t size = PosixIoWrappers::ReadFully(in_, buffer_, block_size_);
    if (size == 0) {
      return false;
    }
    TERRIER_ASSERT(size == block_size_, "Incomplete Checkpoint Page");
    page_offset_ += static_cast<uint32_t>(sizeof(CheckpointFilePage));
    return true;
  }

  /**
   * Read the next row from the buffer.
   * @return pointer to the row if the next row is available.
   *         nullptr if an error happens or there is no other row in the page.
   */
  ProjectedRow *ReadNextRow() {
    if (block_size_ - page_offset_ < sizeof(uint32_t)) {
      // definitely not enough to store another row in the page.
      return nullptr;
    }
    uint32_t row_size = *reinterpret_cast<uint32_t *>(buffer_ + page_offset_);
    if (row_size == 0) {
      // no other row in this page.
      return nullptr;
    }

    auto *checkpoint_row = reinterpret_cast<ProjectedRow *>(buffer_ + page_offset_);
    // TODO(Zhaozhe): Ensure alignment directly when checkpointing
    // Allocate new memory because we have to ensure alignment, for project_row to work correctly
    auto *result = reinterpret_cast<ProjectedRow *>(common::AllocationUtil::AllocateAligned(row_size));
    memcpy(result, checkpoint_row, row_size);
    page_offset_ += row_size;
    return result;
  }

  /**
   * Read the size of the varlen pointed by the current offset. This does not check if currently the offset points to
   * a varlen.
   * @return
   */
  uint32_t ReadNextVarlenSize() {
    uint32_t size = *reinterpret_cast<uint32_t *>(buffer_ + page_offset_);
    page_offset_ += static_cast<uint32_t>(sizeof(uint32_t));
    return size;
  }

  /**
   * Read the next few bytes as a varlen content. The size field of this varlen should already been skipped.
   *
   * Warning: the returning pointer is not aligned, so make sure the calling function does not depend on alignment
   * @param size of the varlen
   * @return pointer to the varlen content
   */
  byte *ReadNextVarlen(uint32_t size) {
    byte *result = buffer_ + page_offset_;
    page_offset_ += size;
    return result;
  }

  /**
   * Get the current checkpoint page.
   * @return pointer to the checkpoint page header.
   */
  CheckpointFilePage *GetPage() { return reinterpret_cast<CheckpointFilePage *>(buffer_); }

 private:
  int in_;
  uint32_t block_size_ = CHECKPOINT_BLOCK_SIZE;
  uint32_t page_offset_ = 0;
  byte *buffer_;
};
}  // namespace terrier::storage
