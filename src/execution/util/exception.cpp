#include "execution/util/exception.h"

#include <iostream>
#include <string>

#include "spdlog/fmt/fmt.h"

namespace terrier::execution {

Exception::Exception(ExceptionType exception_type, const std::string &message) : type_(exception_type) {
  exception_message_ = ExceptionTypeToString(type_) + ": " + message;
}

const char *Exception::what() const noexcept { return exception_message_.c_str(); }

template <typename... Args>
void Exception::Format(const Args &... args) {
  exception_message_ = fmt::format(exception_message_, args...);
}

// static
std::string Exception::ExceptionTypeToString(ExceptionType type) {
  switch (type) {
    case ExceptionType::OutOfRange:
      return "Out of Range";
    case ExceptionType::Conversion:
      return "Conversion";
    case ExceptionType::UnknownType:
      return "Unknown Type";
    case ExceptionType::Decimal:
      return "Decimal";
    case ExceptionType::DivideByZero:
      return "Divide by Zero";
    case ExceptionType::InvalidType:
      return "Invalid type";
    case ExceptionType::Execution:
      return "Executor";
    case ExceptionType::Index:
      return "Index";
    default:
      return "Unknown";
  }
}
std::ostream &operator<<(std::ostream &os, const Exception &e) { return os << e.what(); }

CastException::CastException(sql::TypeId src_type, sql::TypeId dest_type)
    : Exception(ExceptionType::Conversion, "Type {} cannot be cast as {}") {
  Format(TypeIdToString(src_type), TypeIdToString(dest_type));
}

InvalidTypeException::InvalidTypeException(sql::TypeId type, const std::string &msg)
    : Exception(ExceptionType::InvalidType, "Invalid type ['{}']: " + msg) {
  Format(TypeIdToString(type));
}

TypeMismatchException::TypeMismatchException(sql::TypeId src_type, sql::TypeId dest_type, const std::string &msg)
    : Exception(ExceptionType::TypeMismatch, "Type '{}' does not match type '{}'. " + msg) {
  Format(TypeIdToString(src_type), TypeIdToString(dest_type));
}

ValueOutOfRangeException::ValueOutOfRangeException(sql::TypeId src_type, sql::TypeId dest_type)
    : ValueOutOfRangeException("Type {} cannot be cast because the value is out of range for the target type {}") {
  Format(TypeIdToString(src_type), TypeIdToString(dest_type));
}

}  // namespace terrier::execution
