#include "execution/exec/output.h"

namespace tpl::exec {

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
    for (u16 col = 0; col < schema_.GetCols().size(); col++) {
      // TODO(Amadou): Figure out to print other types.
      switch (schema_.GetCols()[col].GetType()) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::BIGINT:
        case type::TypeId::INTEGER: {
          auto *val = reinterpret_cast<sql::Integer *>(
              tuples + row * tuple_size + schema_.GetOffset(col));
          if (val->is_null)
            ss << "NULL";
          else
            ss << val->val;
          break;
        }
        case type::TypeId::BOOLEAN: {
          auto *val = reinterpret_cast<sql::Integer *>(
              tuples + row * tuple_size + schema_.GetOffset(col));
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
        default:
          break;
      }
      if (col != schema_.GetCols().size() - 1) ss << ", ";
    }
    ss << std::endl;
  }
  printed_++;
  std::cout << ss.str() << std::endl;
}
}  // namespace tpl::exec