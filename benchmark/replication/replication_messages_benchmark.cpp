#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/dedicated_thread_registry.h"
#include "common/scoped_timer.h"
#include "replication/replication_messages.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage {

class ReplicationMessagesBenchmark : public benchmark::Fixture {
 public:
  void TearDown(const benchmark::State &state) final { unlink(noisepage::BenchmarkConfig::logfile_path.data()); }
  void FillBuffer(storage::BufferedLogWriter *buffer) {
    for (size_t i = 0; i < common::Constants::LOG_BUFFER_SIZE; i++) {
      char c = RandomChar();
      buffer->BufferWrite(&c, 1);
    }
  }
  char RandomChar() { return static_cast<char>(std::rand() % (CHAR_MAX - CHAR_MIN + 1) + CHAR_MIN); }
};

// Serialize

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ReplicationMessagesBenchmark, NotifyOATMsgSerialization)(benchmark::State &state) {
  replication::NotifyOATMsg msg(replication::ReplicationMessageMetadata(replication::msg_id_t(666)),
                                replication::record_batch_id_t(42), transaction::timestamp_t(999));

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      msg.Serialize();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ReplicationMessagesBenchmark, RecordsBatchMsgSerialization)(benchmark::State &state) {
  unlink(noisepage::BenchmarkConfig::logfile_path.data());
  storage::BufferedLogWriter buffer(noisepage::BenchmarkConfig::logfile_path.data());
  FillBuffer(&buffer);
  replication::RecordsBatchMsg msg(replication::ReplicationMessageMetadata(replication::msg_id_t(666)),
                                   replication::record_batch_id_t(42), &buffer);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      msg.Serialize();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations());
  unlink(noisepage::BenchmarkConfig::logfile_path.data());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ReplicationMessagesBenchmark, TxnAppliedMsgSerialization)(benchmark::State &state) {
  replication::TxnAppliedMsg msg(replication::ReplicationMessageMetadata(replication::msg_id_t(666)),
                                 transaction::timestamp_t(42));

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      msg.Serialize();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations());
}

// Deserialize

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ReplicationMessagesBenchmark, NotifyOATMsgDeserialization)(benchmark::State &state) {
  replication::NotifyOATMsg msg(replication::ReplicationMessageMetadata(replication::msg_id_t(666)),
                                replication::record_batch_id_t(42), transaction::timestamp_t(999));
  std::string serialized_msg = msg.Serialize();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      replication::BaseReplicationMessage::ParseFromString(serialized_msg);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ReplicationMessagesBenchmark, RecordsBatchMsgDeserialization)(benchmark::State &state) {
  unlink(noisepage::BenchmarkConfig::logfile_path.data());
  storage::BufferedLogWriter buffer(noisepage::BenchmarkConfig::logfile_path.data());
  FillBuffer(&buffer);
  replication::RecordsBatchMsg msg(replication::ReplicationMessageMetadata(replication::msg_id_t(666)),
                                   replication::record_batch_id_t(42), &buffer);
  std::string serialized_msg = msg.Serialize();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      replication::BaseReplicationMessage::ParseFromString(serialized_msg);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations());
  unlink(noisepage::BenchmarkConfig::logfile_path.data());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ReplicationMessagesBenchmark, TxnAppliedMsgDeserialization)(benchmark::State &state) {
  replication::TxnAppliedMsg msg(replication::ReplicationMessageMetadata(replication::msg_id_t(666)),
                                 transaction::timestamp_t(42));
  std::string serialized_msg = msg.Serialize();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      replication::BaseReplicationMessage::ParseFromString(serialized_msg);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations());
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(ReplicationMessagesBenchmark, NotifyOATMsgSerialization)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ReplicationMessagesBenchmark, RecordsBatchMsgSerialization)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ReplicationMessagesBenchmark, TxnAppliedMsgSerialization)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ReplicationMessagesBenchmark, NotifyOATMsgDeserialization)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ReplicationMessagesBenchmark, RecordsBatchMsgDeserialization)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ReplicationMessagesBenchmark, TxnAppliedMsgDeserialization)->Unit(benchmark::kNanosecond);
// clang-format on

}  // namespace noisepage
