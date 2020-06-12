#pragma once

#include <cstdarg>
#include <stdexcept>
#include <string>

#include "common/exception.h"
#include "common/macros.h"
#include "execution/sql/sql.h"

namespace terrier::execution {

///**
// * All exception types.
// *
// * NOTE: Please keep these sorted.
// */
// enum class ExecutionExceptionType {
//  Cardinality,   // vectors have different cardinalities
//  CodeGen,       // code generation
//  Conversion,    // conversion/casting error
//  Decimal,       // decimal related
//  DivideByZero,  // divide by 0
//  Execution,     // executor related
//  File,          // file related
//  Index,         // index related
//  InvalidType,   // incompatible for operation
//  OutOfRange,    // value out of range error
//  TypeMismatch,  // mismatched types
//  UnknownType,   // unknown type
//};
//
/////**
//// * Base exception class.
//// */
////class Exception : public std::exception {
//// public:
////  /**
////   * Construct an exception with the given type and message.
////   * @param exception_type The type of the exception.
////   * @param message The message to print.
////   */
////  Exception(ExceptionType exception_type, const std::string &message);
////
////  /**
////   * Return the error message.
////   * @return
////   */
////  const char *what() const noexcept override;
////
////  /**
////   * Convert an exception type into its string representation.
////   * @param type The type of exception.
////   * @return The string name of the input exception type.
////   */
////  static std::string ExceptionTypeToString(ExceptionType type);
////
//// protected:
////  template <typename... Args>
////  void Format(const Args &... args);
////
//// private:
////  // The type of the exception
////  ExceptionType type_;
////
////  // The message
////  std::string exception_message_;
////};

std::ostream &operator<<(std::ostream &os, const Exception &e);

// ---------------------------------------------------------
// Concrete exceptions
// ---------------------------------------------------------

/**
 * An exception thrown due to an invalid cast.
 */
class CastException : public ExecutionException {
 public:
  CastException(sql::TypeId src_type, sql::TypeId dest_type, const char *file, int line);
};

/**
 * An exception thrown when a given type cannot be converted into another type.
 */
class ConversionException : public ExecutionException {
 public:
  explicit ConversionException(const char *msg, const char *file, int line) : ExecutionException(msg, file, line) {}
};

/**
 * The given type is invalid in its context of use.
 */
class InvalidTypeException : public ExecutionException {
 public:
  InvalidTypeException(sql::TypeId type, const char *msg, const char *file, int line);
};

/**
 * An unexpected type enters.
 */
class TypeMismatchException : public ExecutionException {
 public:
  TypeMismatchException(sql::TypeId src_type, sql::TypeId dest_type, const char *msg, const char *file, int line);
};

/**
 * An exception thrown when a value falls outside a given type's valid value range.
 */
class ValueOutOfRangeException : public ExecutionException {
 public:
  ValueOutOfRangeException(sql::TypeId src_type, sql::TypeId dest_type, const char *file, int line);
};

#define CARDINALITY_EXCEPTION(msg) \
  ExecutionException(fmt::format("Vector cardinality mismatch: {}", msg).data(), __FILE__, __LINE__)
#define CODEGEN_EXCEPTION(msg) ExecutionException(fmt::format("Codegen error: {}", msg).data(), __FILE__, __LINE__)
#define EXEC_CONVERSION_EXCEPTION(msg) \
  ExecutionException(fmt::format("Conversion error: {}", msg).data(), __FILE__, __LINE__)
#define DECIMAL_EXCEPTION(msg) ExecutionException(fmt::format("Decimal error: {}", msg).data(), __FILE__, __LINE__)
#define DIVIDE_BY_ZERO_EXCEPTION(msg) \
  ExecutionException(fmt::format("Divide by zero error: {}", msg).data(), __FILE__, __LINE__)
#define EXECUTOR_EXCEPTION(msg) ExecutionException(fmt::format("Executor error: {}", msg).data(), __FILE__, __LINE__)
#define FILE_EXCEPTION(msg) ExecutionException(fmt::format("File error: {}", msg).data(), __FILE__, __LINE__)
#define INDEX_EXCEPTION(msg) ExecutionException(fmt::format("Index error: {}", msg).data(), __FILE__, __LINE__)
#define INVALID_TYPE_EXCEPTION(type, msg) \
  InvalidTypeException(type, fmt::format("Decimal error: {}", msg).data(), __FILE__, __LINE__)
#define OUT_OF_RANGE_EXCEPTION(src_type, dest_type) ValueOutOfRangeException(src_type, dest_type, __FILE__, __LINE__)
#define TYPE_MISMATCH_EXCEPTION(src_type, dest_type, msg) \
  TypeMismatchException(src_type, dest_type, msg, __FILE__, __LINE__)

}  // namespace terrier::execution
