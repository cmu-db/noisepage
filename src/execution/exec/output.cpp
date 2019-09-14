#include "execution/exec/output.h"
#include "execution/sql/value.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::exec {

OutputBuffer::~OutputBuffer() { memory_pool_->Deallocate(tuples_, BATCH_SIZE * tuple_size_); }

void OutputBuffer::Finalize() {
  if (num_tuples_ > 0) {
    callback_(tuples_, num_tuples_, tuple_size_);
    // Reset to zero.
    num_tuples_ = 0;
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
          auto *val = reinterpret_cast<sql::Date *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null_) {
            ss << "NULL";
          } else {
            ss << sql::ValUtil::DateToString(*val);
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
}  // namespace terrier::execution::exec
