#include "execution/util/exception.h"

#include <iostream>
#include <string>

#include "spdlog/fmt/fmt.h"

namespace terrier::execution {

// Exception::Exception(ExceptionType exception_type, const std::string &message) : type_(exception_type) {
//  exception_message_ = ExceptionTypeToString(type_) + ": " + message;
//}
//
// const char *Exception::what() const noexcept { return exception_message_.c_str(); }
//
// template <typename... Args>
// void Exception::Format(const Args &... args) {
//  exception_message_ = fmt::format(exception_message_, args...);
//}
//
//// static
// std::string Exception::ExceptionTypeToString(ExceptionType type) {
//  switch (type) {
//    case ExecutionExceptionType::OutOfRange:
//      return "Out of Range";
//    case ExceptionType::Conversion:
//      return "Conversion";
//    case ExceptionType::UnknownType:
//      return "Unknown Type";
//    case ExceptionType::Decimal:
//      return "Decimal";
//    case ExceptionType::DivideByZero:
//      return "Divide by Zero";
//    case ExceptionType::InvalidType:
//      return "Invalid type";
//    case ExceptionType::Execution:
//      return "Executor";
//    case ExceptionType::Index:
//      return "Index";
//    default:
//      return "Unknown";
//  }
//}
std::ostream &operator<<(std::ostream &os, const Exception &e) { return os << e.what(); }

CastException::CastException(sql::TypeId src_type, sql::TypeId dest_type, const char *file, int line)
    : ExecutionException(
          fmt::format("Cast Error: Type {} cannot be cast as {}", TypeIdToString(src_type), TypeIdToString(dest_type))
              .data(),
          file, line) {}

InvalidTypeException::InvalidTypeException(sql::TypeId type, const char *msg, const char *file, int line)
    : ExecutionException(fmt::format("Invalid type ['{}']: {}", TypeIdToString(type), msg).data(), file, line) {}

TypeMismatchException::TypeMismatchException(sql::TypeId src_type, sql::TypeId dest_type, const char *msg,
                                             const char *file, int line)
    : ExecutionException(fmt::format("Type '{}' does not match type '{}'. {}", TypeIdToString(src_type),
                                     TypeIdToString(dest_type), msg)
                             .data(),
                         file, line) {}

ValueOutOfRangeException::ValueOutOfRangeException(sql::TypeId src_type, sql::TypeId dest_type, const char *file,
                                                   int line)
    : ExecutionException(fmt::format("Type {} cannot be cast because the value is out of range for the target type {}",
                                     TypeIdToString(src_type), TypeIdToString(dest_type))
                             .data(),
                         file, line) {}

}  // namespace terrier::execution
