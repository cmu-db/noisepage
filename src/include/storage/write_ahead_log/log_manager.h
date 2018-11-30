#pragma once

#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent.
 */
class LogManager {
 public:
  /**
   * Constructs a new LogManager, writing its logs out to the given file.
   *
   * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
   *                      otherwise, changes are appended to the end of the file.
   * @param buffer_pool the object pool to draw log buffers from. This must be the same pool transactions draw their
   *                    buffers from
   */
  LogManager(const char *log_file_path, RecordBufferSegmentPool *buffer_pool)
      : out_(log_file_path), buffer_pool_(buffer_pool) {}

  /**
   * Must be called when no other threads are doing work
   */
  void Shutdown() {
    Process();
    Flush();
    out_.Close();
  }

  /**
   * Returns a (perhaps partially) filled log buffer to the log manager to be consumed. Caller should drop its
   * reference to the buffer after the method returns immediately, as it would no longer be safe to read from or
   * write to the buffer. This method can be called safely from concurrent execution threads.
   *
   * @param buffer the (perhaps partially) filled log buffer ready to be consumed
   */
  void AddBufferToFlushQueue(RecordBufferSegment *buffer) {
    common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
    flush_queue_.push(buffer);
  }

  /**
   * Process all the accumulated log records and serialize them out to disk. A flush will always happen at the end.
   * (Beware the performance consequences of calling flush too frequently) This method should only be called from a
   * dedicated
   * logging thread.
   */
  void Process();

  /**
   * Flush the logs to make sure all serialized records before this invocation are persistent. Callbacks from committed
   * transactions are also invoked when possible. This method should only be called from a dedicated logging thread.
   *
   * Usually this method is called from Process(), but can also be called by itself if need be.
   */
  void Flush();

 private:
  // TODO(Tianyu): This can be changed later to be include things that are not necessarily backed by a disk
  //  (e.g. logs can be streamed out to the network for remote replication)
  BufferedLogWriter out_;
  RecordBufferSegmentPool *buffer_pool_;

  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  common::SpinLatch flush_queue_latch_;
  // TODO(Tianyu): benchmark for if these should be concurrent data structures, and if we should apply the same
  //  optimization we applied to the GC queue.
  std::queue<RecordBufferSegment *> flush_queue_;

  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<std::pair<transaction::callback_fn, void *>> commits_in_buffer_;

  void SerializeRecord(const LogRecord &record);

  template <class T>
  void WriteValue(const T &val) {
    out_.BufferWrite(&val, sizeof(T));
  }

};
}  // namespace terrier::storage
