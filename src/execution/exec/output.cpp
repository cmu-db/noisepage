#include "execution/exec/output.h"
#include "execution/sql/value.h"

namespace terrier::execution::exec {

OutputBuffer::~OutputBuffer() { delete[] tuples_; }

void OutputBuffer::Advance() {
  num_tuples_++;
  if (num_tuples_ == batch_size_) {
    callback_(tuples_, num_tuples_, tuple_size_);
    num_tuples_ = 0;
    return;
  }
}

void OutputBuffer::Finalize() {
  if (num_tuples_ > 0) {
    callback_(tuples_, num_tuples_, tuple_size_);
    // Reset to zero.
    num_tuples_ = 0;
  }
}

void OutputPrinter::operator()(byte *tuples, u32 num_tuples, u32 tuple_size) {
  // Limit the number of tuples printed
  std::stringstream ss{};
  for (u32 row = 0; row < num_tuples; row++) {
    uint32_t curr_offset = 0;
    for (u16 col = 0; col < schema_->GetColumns().size(); col++) {
      // TODO(Amadou): Figure out to print other types.
      switch (schema_->GetColumns()[col].GetType()) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::BIGINT:
        case type::TypeId::INTEGER: {
          auto *val = reinterpret_cast<sql::Integer *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null)
            ss << "NULL";
          else
            ss << val->val;
          break;
        }
        case type::TypeId::BOOLEAN: {
          auto *val = reinterpret_cast<sql::BoolVal *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null) {
            ss << "NULL";
          } else {
            if (val->val) {
              ss << "true";
            } else {
              ss << "false";
            }
          }
          break;
        }
        case type::TypeId::DECIMAL: {
          auto *val = reinterpret_cast<sql::Real *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null)
            ss << "NULL";
          else
            ss << val->val;
          break;
        }
        case type::TypeId::DATE: {
          auto *val = reinterpret_cast<sql::Date *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null) {
            ss << "NULL";
          } else {
            ss << sql::ValUtil::DateToString(*val);
          }
          break;
        }
        case type::TypeId::VARCHAR: {
          auto *val = reinterpret_cast<sql::StringVal *>(tuples + row * tuple_size + curr_offset);
          if (val->is_null) {
            ss << "NULL";
          } else {
            ss.write(val->Content(), val->len);
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
  printed_++;
  std::cout << ss.str() << std::endl;
}
}  // namespace terrier::execution::exec
