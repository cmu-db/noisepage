#include "execution/exec/output.h"

#include "execution/sql/value.h"
#include "loggers/execution_logger.h"
#include "network/postgres/postgres_packet_writer.h"

namespace terrier::execution::exec {

OutputBuffer::~OutputBuffer() {
  memory_pool_->Deallocate(num_tuples_, MAX_THREAD_SIZE * sizeof(uint32_t));
  for (auto &it : buffer_map_) {
    memory_pool_->Deallocate(it.second.second, BATCH_SIZE * tuple_size_);
  }
}

void OutputBuffer::Finalize() {
  std::thread::id this_id = std::this_thread::get_id();
  InsertIfAbsent(this_id);
  auto it = buffer_map_.find(this_id);
  size_t index = it->second.first;
  byte *tuples = it->second.second;
  if (num_tuples_[index] > 0) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    callback_(tuples, num_tuples_[index], tuple_size_);
    num_tuples_[index] = 0;
  }
}

void OutputPrinter::operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
  // Limit the number of tuples printed
  std::stringstream ss{};
  for (uint32_t row = 0; row < num_tuples; row++) {
    uint32_t curr_offset = 0;
    for (uint16_t col = 0; col < schema_->GetColumns().size(); col++) {
      // TODO(Amadou): Figure out to print other types.
      switch (schema_->GetColumns()[col].GetType()) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::BIGINT:
        case type::TypeId::INTEGER: {
          auto *val = reinterpret_cast<sql::Integer *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null_)
            ss << "NULL";
          else
            ss << val->val_;
          break;
        }
        case type::TypeId::BOOLEAN: {
          auto *val = reinterpret_cast<sql::BoolVal *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null_) {
            ss << "NULL";
          } else {
            if (val->val_) {
              ss << "true";
            } else {
              ss << "false";
            }
          }
          break;
        }
        case type::TypeId::DECIMAL: {
          auto *val = reinterpret_cast<sql::Real *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null_)
            ss << "NULL";
          else
            ss << val->val_;
          break;
        }
        case type::TypeId::DATE: {
          auto *val = reinterpret_cast<sql::DateVal *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null_) {
            ss << "NULL";
          } else {
            ss << val->val_.ToString();
          }
          break;
        }
        case type::TypeId::VARCHAR: {
          auto *val = reinterpret_cast<sql::StringVal *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null_) {
            ss << "NULL";
          } else {
            ss.write(val->Content(), val->len_);
            ss.put('\0');
          }
          break;
        }
        default:
          UNREACHABLE("Cannot output unsupported type!!!");
      }
      curr_offset += sql::ValUtil::GetSqlSize(schema_->GetColumns()[col].GetType());
      if (col != schema_->GetColumns().size() - 1) ss << ", ";
    }
    ss << std::endl;
  }
  EXECUTION_LOG_INFO("Ouptut batch {}: \n{}", printed_, ss.str());
  printed_++;
}

void OutputWriter::operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
  // Write out the rows for this batch
  for (uint32_t row = 0; row < num_tuples; row++) {
    const byte *const tuple = tuples + row * tuple_size;
    out_->WriteDataRow(tuple, schema_->GetColumns());
    num_rows_++;
  }
}
}  // namespace terrier::execution::exec
