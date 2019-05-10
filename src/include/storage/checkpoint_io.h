#pragma once
#include <vector>
#include "common/container/concurrent_blocking_queue.h"
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

// checkpoint file header flags
#define P_IS_CATALOG 0

/**
 * The header of a page in the checkpoint file.
 */
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

  /**
   * Set all the flags at the same time.
   * @param flags to be set.
   */
  void SetFlags(uint32_t flags) { flags_ = flags; }

  /**
   * Set the bit in the flags to some value.
   * @param pos position of the flag to set, from the most significant bit to the least significant bit.
   * @param value bool value to set
   */
  void SetFlag(uint32_t pos, bool value) { Bitmap().Set(pos, value); }

  /**
   * Get the status of the flag at some position
   * @param pos position of the flag to test, from the most significant bit the the least significant bit.
   * @return the value of the flag.
   */
  bool GetFlag(uint32_t pos) { return Bitmap().Test(pos); }

  void SetVersion(uint32_t version) { version_ = version; }

  uint32_t GetVersion() { return version_; }

 private:
  uint32_t checksum_;
  // Note: use uint32_t instead of table_oid_t, because table_oid_t is not POD so that
  // we cannot use PACKED in this class.
  uint32_t table_oid_;
  uint32_t version_;  // version of the schema(useful with schema change), or, for catalog tables,
                      // serves as the type of the catalog table.
  /* 32 bit flag. The meaning of each bit is:
   *  0. catalog table
   */
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
    byte *res = nullptr;
    free_.Dequeue(&res);
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
  common::ConcurrentBlockingQueue<byte *> free_;     // free buffers
  common::ConcurrentBlockingQueue<byte *> pending_;  // buffers pending write
  std::thread *writer_thread_;                       // writer thread

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
 * +-----------------------------------------------+
 * | checksum | table_oid | schema_version | flags |
 * +-----------------------------------------------+
 * | tuple1 | TupleSlot1 | varlen_entries(if exist)|
 * +-----------------------------------------------+
 * | tuple2 | TupleSlot2 | varlen_entries(if exist)|
 * +-----------------------------------------------+
 * | ... |
 * +-----+
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
  void Persist() {
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

  /**
   * Set the cached table oid. This value will be written to every page, unless otherwise changed.
   * @param oid of the table
   */
  void SetTableOid(catalog::table_oid_t oid) {
    table_oid_ = !oid;
    GetPage()->SetTableOid(oid);
  }

  /**
   * Set the cached table schema version. This value will be written to every page, unless otherwise changed.
   * Currently the table only have one version, so this should be 0 all the time.
   * @param version of the table schema
   */
  void SetVersion(uint32_t version) {
    version_ = version;
    GetPage()->SetVersion(version);
  }

 private:
  AsyncBlockWriter async_writer_;  // background writer
  uint32_t block_size_;
  uint32_t page_offset_ = 0;
  byte *buffer_ = nullptr;

  // cache of the header so that it can be automatically filled into new pages.
  uint32_t table_oid_;
  uint32_t version_;
  uint32_t flags_;

  /**
   * Write a piece of data into the buffer. If the buffer is not enough, persist current buffer and allocate a new one.
   * Note that alignment is not dealt in this function. It has to be done manually.
   * @param data to be written.
   * @param size of the data.
   */
  void WriteDataToBuffer(const byte *data, uint32_t size) {
    uint32_t left_to_write = size;
    while (left_to_write > 0) {
      uint32_t remaining_buffer = block_size_ - page_offset_;
      if (left_to_write >= remaining_buffer) {
        memcpy(buffer_ + page_offset_, data, remaining_buffer);
        page_offset_ += remaining_buffer;
        left_to_write -= remaining_buffer;
        data = data + remaining_buffer;
        Persist();
      } else {
        memcpy(buffer_ + page_offset_, data, left_to_write);
        page_offset_ += left_to_write;
        return;
      }
    }
  }

  void ResetBuffer() {
    CheckpointFilePage::Initialize(reinterpret_cast<CheckpointFilePage *>(buffer_));
    page_offset_ = sizeof(CheckpointFilePage);
    AlignBufferOffset<uint64_t>();  // align for ProjectedRow
    GetPage()->SetVersion(version_);
    GetPage()->SetTableOid(catalog::table_oid_t(table_oid_));
    GetPage()->SetFlags(flags_);
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
    ClearLoosePointers();
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

    ClearLoosePointers();
    auto *checkpoint_row = reinterpret_cast<ProjectedRow *>(ReadDataAcrossPages(row_size));
    return checkpoint_row;
  }

  TupleSlot *ReadNextTupleSlot() {
    AlignBufferOffset<uintptr_t>();
    auto slot = reinterpret_cast<TupleSlot *>(ReadDataAcrossPages(sizeof(uintptr_t)));
    return slot;
  }

  /**
   * Read the size of the varlen pointed by the current offset. This does not check if currently the offset points to
   * a varlen.
   * @return
   */
  uint32_t ReadNextVarlenSize() {
    AlignBufferOffset<uint32_t>();
    uint32_t size = *reinterpret_cast<uint32_t *>(ReadDataAcrossPages(sizeof(uint32_t)));
    return size;
  }

  /**
   * Read the next few bytes as a varlen content. The size field of this varlen should already been skipped.
   *
   * Warning: the returning pointer is not aligned, so make sure the calling function does not depend on alignment
   * @param size of the varlen
   * @return pointer to the varlen content
   */
  byte *ReadNextVarlen(uint32_t size) { return ReadDataAcrossPages(size); }

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

  std::vector<byte *> loose_ptrs_;

  /**
   * Align the current buffer offset to 4 bytes or 8 bytes, depending on what kind of value will be read next.
   */
  template <class T>
  void AlignBufferOffset() {
    page_offset_ = StorageUtil::PadUpToSize(alignof(T), page_offset_);
  }

  /**
   * Read a piece of data from the checkpoint file. If the size is larger than the remaining size of the page, a new
   * block is read in. Data from different pages will be concatenate together.
   *
   * Note: if data is read from multiple pages, a new piece of memory is allocated for this data, and thus it is a
   *       loose pointer. This pointer will be cleaned when reading the next row (i.e. next call to ReadNextRow), so
   *       whoever is using this piece of memory should stop using it after reading the next row.
   *       This is a technical debt because we don't know when the user will end doing things on it. This also make us
   *       unable to have concurrent readers. Maybe in the future we will have a dedicated GC thread for doing this.
   * @param size of the data to be read from the file.
   * @return pointer to the data.
   */
  byte *ReadDataAcrossPages(uint32_t size) {
    uint32_t remaining_buffer = block_size_ - page_offset_;
    if (size <= remaining_buffer) {  // current buffer is enough
      byte *result = buffer_ + page_offset_;
      page_offset_ += size;
      return result;
    }
    // current buffer is not enough
    byte *tmp_buffer = common::AllocationUtil::AllocateAligned(size);
    uint32_t left_to_read = size;
    uint32_t already_read = 0;
    while (left_to_read > 0) {
      remaining_buffer = block_size_ - page_offset_;
      if (left_to_read <= remaining_buffer) {  // remaining buffer is enough
        memcpy(tmp_buffer + already_read, buffer_ + page_offset_, left_to_read);
        page_offset_ += left_to_read;
        loose_ptrs_.emplace_back(tmp_buffer);
        return tmp_buffer;
      }
      // remaining buffer is not enough
      memcpy(tmp_buffer + already_read, buffer_ + page_offset_, remaining_buffer);
      bool hasNextBlock UNUSED_ATTRIBUTE = ReadNextBlock();
      TERRIER_ASSERT(hasNextBlock, "Incomplete Checkpoint file");
      left_to_read -= remaining_buffer;
      already_read += remaining_buffer;
    }
    return tmp_buffer;
  }

  void ClearLoosePointers() {
    for (auto ptr : loose_ptrs_) {
      delete[] ptr;
    }
    loose_ptrs_.clear();
  }
};

}  // namespace terrier::storage
