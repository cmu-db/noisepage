#pragma once

#include <cstdarg>
#include <stdexcept>
#include <string>

#include "common/macros.h"
#include "execution/sql/sql.h"

namespace terrier::execution {

/**
 * All exception types.
 *
 * NOTE: Please keep these sorted.
 */
enum class ExceptionType {
  Cardinality,     // vectors have different cardinalities
  CodeGen,         // code generation
  Conversion,      // conversion/casting error
  Decimal,         // decimal related
  DivideByZero,    // divide by 0
  Execution,       // executor related
  File,            // file related
  Index,           // index related
  InvalidType,     // incompatible for operation
  NotImplemented,  // method not implemented
  OutOfRange,      // value out of range error
  TypeMismatch,    // mismatched types
  UnknownType,     // unknown type
};

/**
 * Base exception class.
 */
class Exception : public std::exception {
 public:
  /**
   * Construct an exception with the given type and message.
   * @param exception_type The type of the exception.
   * @param message The message to print.
   */
  Exception(ExceptionType exception_type, const std::string &message);

  /**
   * Return the error message.
   * @return
   */
  const char *what() const noexcept override;

  /**
   * Convert an exception type into its string representation.
   * @param type The type of exception.
   * @return The string name of the input exception type.
   */
  static std::string ExceptionTypeToString(ExceptionType type);

 protected:
  template <typename... Args>
  void Format(const Args &... args);

 private:
  // The type of the exception
  ExceptionType type_;

  // The message
  std::string exception_message_;
};

std::ostream &operator<<(std::ostream &os, const Exception &e);

// ---------------------------------------------------------
// Concrete exceptions
// ---------------------------------------------------------

/**
 * An exception thrown due to an invalid cast.
 */
class CastException : public Exception {
 public:
  CastException(sql::TypeId src_type, sql::TypeId dest_type);
};

/**
 * An exception thrown when a given type cannot be converted into another type.
 */
class ConversionException : public Exception {
 public:
  explicit ConversionException(const std::string &msg)
      : Exception(ExceptionType::Conversion, msg) {}
};

/**
 * The given type is invalid in its context of use.
 */
class InvalidTypeException : public Exception {
 public:
  InvalidTypeException(sql::TypeId type, const std::string &msg);
};

/**
 * An exception thrown to indicate some functionality isn't implemented.
 */
class NotImplementedException : public Exception {
 public:
  explicit NotImplementedException(const std::string &msg)
      : Exception(ExceptionType::NotImplemented, msg) {}
};

/**
 * An unexpected type enters.
 */
class TypeMismatchException : public Exception {
 public:
  TypeMismatchException(sql::TypeId src_type, sql::TypeId dest_type, const std::string &msg);
};

/**
 * An exception thrown when a value falls outside a given type's valid value range.
 */
class ValueOutOfRangeException : public Exception {
 public:
  ValueOutOfRangeException(sql::TypeId src_type, sql::TypeId dest_type);

 private:
  explicit ValueOutOfRangeException(const std::string &message)
      : Exception(ExceptionType::OutOfRange, message) {}
};

}  // namespace terrier::execution
