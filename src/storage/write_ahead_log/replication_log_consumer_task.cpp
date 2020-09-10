#include "storage/write_ahead_log/replication_log_consumer_task.h"

#include "network/itp/itp_packet_writer.h"

namespace terrier::storage {

void ReplicationLogConsumerTask::RunTask() {
  run_task_ = true;
  ReplicationLogConsumerTaskLoop();
}

void ReplicationLogConsumerTask::Terminate() {
  // If the task hasn't run yet, yield the thread until it's started
  while (!run_task_) std::this_thread::yield();
  TERRIER_ASSERT(run_task_, "Cant terminate a task that isn't running");
  // Signal to terminate and force a flush so task persists before LogManager closes buffers
  run_task_ = false;
  replication_log_sender_cv_.notify_one();
}

void ReplicationLogConsumerTask::SendLogsOverNetwork() {
  // Grab all buffers availible, and compute total size of data
  std::deque<BufferedLogWriter *> temp_buffer_queue;
  uint64_t data_size = 0;
  SerializedLogs logs;
  while (!filled_buffer_queue_->Empty()) {
    filled_buffer_queue_->Dequeue(&logs);
    data_size += logs.first->buffer_size_;
    temp_buffer_queue.push_back(logs.first);
  }
  TERRIER_ASSERT(data_size > 0, "Amount of data to send must be greater than 0");

  // Build the packet
  // TODO(Gus): Consider stashing the packet writer in the class to avoid constant construction/destruction
  network::ITPPacketWriter packet_writer(io_wrapper_->GetWriteQueue());
  packet_writer.BeginReplicationCommand(message_id_++, data_size);
  for (auto *buffer : temp_buffer_queue) {
    packet_writer.AppendRaw(&buffer->buffer_, buffer->buffer_size_);
    // Return buffer to log manager
    empty_buffer_queue_->Enqueue(buffer);
  }
  packet_writer.EndReplicationCommand();

  // Send packet over network
  io_wrapper_->FlushAllWrites();
}

void ReplicationLogConsumerTask::SendStopReplicationMessage() {
  network::ITPPacketWriter packet_writer(io_wrapper_->GetWriteQueue());
  packet_writer.WriteStopReplicationCommand();
  io_wrapper_->FlushAllWrites();
}

void ReplicationLogConsumerTask::ReplicationLogConsumerTaskLoop() {
  // TODO(Gus): Add some sort of handshake messaging between master and replica before we begin streaming logs
  // TODO(Gus): Add metric exporting when task is finalized
  do {
    {
      std::unique_lock<std::mutex> lock(replication_lock_);
      // We use a CV because we need fast response to when a new buffer appears. We need fast response time because we
      // prioritize a low replication delay
      replication_log_sender_cv_.wait(lock, [&] { return !run_task_ || !filled_buffer_queue_->Empty(); });
    }

    // TODO(gus): perf if taking lock is expensive above. We can modify SendLogsOverNetwork to spin in a loop and grab
    // new buffers that may have arrived during last packet construction
    if (!filled_buffer_queue_->Empty()) SendLogsOverNetwork();
  } while (run_task_ || !filled_buffer_queue_->Empty());

  SendStopReplicationMessage();
}

}  // namespace terrier::storage
