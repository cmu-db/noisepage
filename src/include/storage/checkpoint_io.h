#pragma once
#include <vector>
#include "common/container/concurrent_map.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {
// TODO(Zhaozhe): Should fetch block_size. Use magic number first.
// The block size to output to disk every time
#define CHECKPOINT_BLOCK_SIZE 4096
// TODO(Yuning): Should allow setting number of pre-allocated buffers.
// Use magic number before we have a setting manager.
#define CHECKPOINT_BUF_NUM 10

/**
 * The header of a page in the checkpoint file.
 */
// TODO(Zhaozhe, Mengyang): More fields can be added to header
class PACKED CheckpointFilePage {
 public:
  /**
   * Initialize a given page
   * @param page to be initialized.
   */
  static void Initialize(CheckpointFilePage *page) {
    // TODO(mengyang): support non-trivial initialization
    page->checksum_ = 0;
    page->table_oid_ = 0;
    page->version_ = 0;
    page->flags_ = 0;
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
  catalog::table_oid_t GetTableOid() { return catalog::table_oid_t(table_oid_); }

  /**
   * Set the oid of the table whose rows are stored in this page.
   * @param oid of the table.
   */
  void SetTableOid(catalog::table_oid_t oid) { table_oid_ = !oid; }

  /**
   * Get the pointer to the first row in this page.
   * @return pointer to the first row.
   */
  byte *GetPayload() { return payload_; }

 private:
  uint32_t checksum_;
  // Note: use uint32_t instead of table_oid_t, because table_oid_t is not POD so that
  // we cannot use PACKED in this class.
  uint32_t table_oid_;
  uint32_t version_;  // version of the schema(useful with schema change)
  uint32_t flags_;
  byte payload_[0];

  common::RawBitmap &Bitmap() { return *reinterpret_cast<common::RawBitmap *>(&flags_); }

  const common::RawBitmap &Bitmap() const { return *reinterpret_cast<const common::RawBitmap *>(&flags_); }
};

/**
 * An async block writer will use a background thread to write blocks into
 * checkpoint file asynchronously. Its design is very similar with that of
 * the logger threads in SiloR. An async block writer will have some
 * pre-allocated block buffers. Workers must first ask for a free block
 * buffer. If there is no free block buffer now, it will be blocked until
 * some free block buffer is available. In this way, the async block writer
 * can give some back pressure on the worker to let it slow down if it is
 * generating blocks too quickly. The worker then fills the block buffer
 * and hand it back to the writer thread. The writer thread writes it into
 * the file and make the buffer available again.
 */
class AsyncBlockWriter {
 public:
  AsyncBlockWriter() = default;

  /**
   * Instantiate a new AsyncBlockWriter, open a new checkpoint file to write into.
   * @param log_file_path path to the checkpoint file.
   * @param buffer_num number of pre-allocated buffer
   */
  explicit AsyncBlockWriter(const char *log_file_path, int buffer_num) { Open(log_file_path, buffer_num); }

  /**
   * Open a file to write into
   * @param log_file_path path to the checkpoint file.
   * @param buffer_num number of pre-allocated buffer
   */
  void Open(const char *log_file_path, int buffer_num);

  /**
   * Ask for a free block buffer
   * @return pointer to a free block buffer
   */
  byte *GetBuffer() {
    byte *res;

    // TODO(Yuning): Maybe use blocking queue?
    while (!free_.Dequeue(&res)) {
      // spin
    }

    return res;
  }

  /**
   * Let the writer write a buffer (async)
   * @param buf the pointer to the buffer to be written
   */
  void WriteBuffer(byte *buf) {
    TERRIER_ASSERT(buf != nullptr, "Buffer pointer should not be nullptr");
    pending_.Enqueue(buf);
  }

  /**
   * Return a buffer to the writer, make it available again
   * without writing it
   * @param buf the pointer to the buffer to be returned
   */
  void ReturnBuffer(byte *buf) {
    TERRIER_ASSERT(buf != nullptr, "Buffer pointer should not be nullptr");
    free_.Enqueue(buf);
  }

  /**
   * Wait for all writes done, close current file and release all the buffers.
   */
  void Close();

 private:
  int out_;  // fd of the output files
  uint32_t block_size_;
  common::ConcurrentQueue<byte *> free_;     // free buffers
  common::ConcurrentQueue<byte *> pending_;  // buffers pending write
  std::thread *writer_thread_;               // writer thread

  /**
   * The writer thread
   */
  void RunWriter();
};

/**
 * A buffered tuple writer collects tuples into blocks, and output a block once a time.
 * The block size is fetched from database setting, and should be the page size.
 * TupleSlot is stored for log recovery (for the purpose of locating the records).
 * layout:
 * -----------------------------------------------------------------------------------------------------
 * | checksum | table_oid | tuple1 | TupleSlot1 | varlen_entries(if exist) | tuple2 | TupleSlot2 | ... |
 * -----------------------------------------------------------------------------------------------------
 */
class BufferedTupleWriter {
  // TODO(Zhaozhe): checksum
 public:
  BufferedTupleWriter() = default;

  /**
   * Instantiate a new BufferedTupleWriter, open a new checkpoint file to write into.
   * @param log_file_path path to the checkpoint file.
   */
  explicit BufferedTupleWriter(const char *log_file_path) { Open(log_file_path); }

  /**
   * Open a file to write into
   * @param log_file_path path to the checkpoint file.
   */
  void Open(const char *log_file_path) {
    async_writer_.Open(log_file_path, CHECKPOINT_BUF_NUM);
    block_size_ = CHECKPOINT_BLOCK_SIZE;
    buffer_ = async_writer_.GetBuffer();
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
    async_writer_.ReturnBuffer(buffer_);
    async_writer_.Close();
  }

  /**
   * Serialize a tuple into the checkpoint file (buffer). The buffer will be persisted to the disk, if the remaining
   * size of the buffer is  not enough for this row.
   * @param row to be serialized
   * @param schema schema of the row
   * @param proj_map projection map of the schema.
   */
  void SerializeTuple(ProjectedRow *row, const TupleSlot *slot, const catalog::Schema &schema,
                      const ProjectionMap &proj_map);

  /**
   * Get the current checkpoint page.
   * @return pointer to the checkpoint page header.
   */
  CheckpointFilePage *GetPage() { return reinterpret_cast<CheckpointFilePage *>(buffer_); }

 private:
  AsyncBlockWriter async_writer_;  // background writer
  uint32_t block_size_;
  uint32_t page_offset_ = 0;
  byte *buffer_ = nullptr;

  void ResetBuffer() {
    CheckpointFilePage::Initialize(reinterpret_cast<CheckpointFilePage *>(buffer_));
    page_offset_ = sizeof(CheckpointFilePage);
    AlignBufferOffset<uint64_t>();  // align for ProjectedRow
  }

  void PersistBuffer() {
    // TODO(zhaozhe): calculate CHECKSUM. Currently using default 0 as checksum
    if (page_offset_ == sizeof(CheckpointFilePage)) {
      // If the buffer has no contents, just return
      return;
    }
    AlignBufferOffset<uint64_t>();
    if (block_size_ - page_offset_ > sizeof(uint32_t)) {
      // append a zero to the last record, so that during recovery it can be recognized as the end
      memset(buffer_ + page_offset_, 0, sizeof(uint32_t));
    }
    async_writer_.WriteBuffer(buffer_);
    buffer_ = async_writer_.GetBuffer();
    ResetBuffer();
  }

  template <class T>
  void AlignBufferOffset() {
    page_offset_ = StorageUtil::PadUpToSize(alignof(T), page_offset_);
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
    page_count_++;
    page_offset_ = static_cast<uint32_t>(sizeof(CheckpointFilePage));
    AlignBufferOffset<uint64_t>();
    return true;
  }

  /**
   * Read the next row from the buffer.
   * @return pointer to the row if the next row is available.
   *         nullptr if an error happens or there is no other row in the page.
   */
  ProjectedRow *ReadNextRow() {
    // TODO(mengyang): why sometimes uint64 aligned, sometimes uint32 aligned?
    AlignBufferOffset<uint64_t>();
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
    page_offset_ += row_size;
    return checkpoint_row;
  }

  TupleSlot *ReadNextTupleSlot() {
    AlignBufferOffset<uint32_t>();
    auto slot = reinterpret_cast<TupleSlot *>(buffer_ + page_offset_);
    page_offset_ += static_cast<uint32_t>(sizeof(TupleSlot));
    return slot;
  }

  /**
   * Read the size of the varlen pointed by the current offset. This does not check if currently the offset points to
   * a varlen.
   * @return
   */
  uint32_t ReadNextVarlenSize() {
    AlignBufferOffset<uint32_t>();
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
  uint32_t page_count_ = 0;
  byte *buffer_;

  template <class T>
  void AlignBufferOffset() {
    page_offset_ = StorageUtil::PadUpToSize(alignof(T), page_offset_);
  }
};

}  // namespace terrier::storage
