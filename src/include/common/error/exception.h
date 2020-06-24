#pragma once

#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <string>

#include "common/error/error_code.h"

namespace terrier {

/**
 * Use the macros below for generating exceptions.
 * They record where the exception was generated.
 */

#define NOT_IMPLEMENTED_EXCEPTION(msg) NotImplementedException(msg, __FILE__, __LINE__)
#define CATALOG_EXCEPTION(msg) CatalogException(msg, __FILE__, __LINE__)
#define CONVERSION_EXCEPTION(msg) ConversionException(msg, __FILE__, __LINE__)
#define PARSER_EXCEPTION(msg) ParserException(msg, __FILE__, __LINE__)
#define NETWORK_PROCESS_EXCEPTION(msg) NetworkProcessException(msg, __FILE__, __LINE__)
#define SETTINGS_EXCEPTION(msg) SettingsException(msg, __FILE__, __LINE__)
#define OPTIMIZER_EXCEPTION(msg) OptimizerException(msg, __FILE__, __LINE__)
#define SYNTAX_EXCEPTION(msg) SyntaxException(msg, __FILE__, __LINE__)
#define BINDER_EXCEPTION(msg, code) BinderException(msg, __FILE__, __LINE__, (code))

/**
 * Exception types
 */
enum class ExceptionType : uint8_t {
  RESERVED,
  NOT_IMPLEMENTED,
  BINDER,
  CATALOG,
  CONVERSION,
  NETWORK,
  PARSER,
  SETTINGS,
  OPTIMIZER,
  SYNTAX
};

/**
 * Exception base class.
 */
class Exception : public std::runtime_error {
 public:
  /**
   * Creates a new Exception with the given parameters.
   * @param type exception type
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   */
  Exception(const ExceptionType type, const char *msg, const char *file, int line)
      : std::runtime_error(msg), type_(type), file_(file), line_(line) {}

  /**
   * Allows type and source location of the exception to be recorded in the log
   * at the catch point.
   */
  friend std::ostream &operator<<(std::ostream &out, const Exception &ex) {
    out << ex.GetType() << " exception:";
    out << ex.GetFile() << ":";
    out << ex.GetLine() << ":";
    out << ex.what();
    return out;
  }

  /**
   * @return the exception type
   */
  const char *GetType() const {
    switch (type_) {
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not Implemented";
      case ExceptionType::CATALOG:
        return "Catalog";
      case ExceptionType::PARSER:
        return "Parser";
      case ExceptionType::NETWORK:
        return "Network";
      case ExceptionType::SETTINGS:
        return "Settings";
      case ExceptionType::BINDER:
        return "Binder";
      case ExceptionType::OPTIMIZER:
        return "Optimizer";
      default:
        return "Unknown exception type";
    }
  }

  /**
   * @return the file that threw the exception
   */
  const char *GetFile() const { return file_; }

  /**
   * @return the line number that threw the exception
   */
  int GetLine() const { return line_; }

 protected:
  /**
   * The type of exception.
   */
  const ExceptionType type_;
  /**
   * The name of the file in which the exception was raised.
   */
  const char *file_;
  /**
   * The line number at which the exception was raised.
   */
  const int line_;
};

// -----------------------
// Derived exception types
// -----------------------

#define DEFINE_EXCEPTION(e_name, e_type)                                                                       \
  class e_name : public Exception {                                                                            \
    e_name() = delete;                                                                                         \
                                                                                                               \
   public:                                                                                                     \
    e_name(const char *msg, const char *file, int line) : Exception(e_type, msg, file, line) {}                \
    e_name(const std::string &msg, const char *file, int line) : Exception(e_type, msg.c_str(), file, line) {} \
  }

DEFINE_EXCEPTION(NotImplementedException, ExceptionType::NOT_IMPLEMENTED);
DEFINE_EXCEPTION(CatalogException, ExceptionType::CATALOG);
DEFINE_EXCEPTION(NetworkProcessException, ExceptionType::NETWORK);
DEFINE_EXCEPTION(SettingsException, ExceptionType::SETTINGS);
DEFINE_EXCEPTION(OptimizerException, ExceptionType::OPTIMIZER);
DEFINE_EXCEPTION(ConversionException, ExceptionType::CONVERSION);
DEFINE_EXCEPTION(SyntaxException, ExceptionType::SYNTAX);

/**
 * Specialized Parser exception since we want a cursor position to get more verbose output
 */
class ParserException : public Exception {
  const uint32_t cursorpos_ = 0;

 public:
  ParserException() = delete;
  /**
   * Creates a new ParserException with the given parameters.
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   */
  ParserException(const char *msg, const char *file, int line) : Exception(ExceptionType::PARSER, msg, file, line) {}

  /**
   * Creates a new ParserException with the given parameters.
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   */
  ParserException(const std::string &msg, const char *file, int line)
      : Exception(ExceptionType::PARSER, msg.c_str(), file, line) {}

  /**
   * Creates a new ParserException with the given parameters.
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   * @param cursorpos from libpgquery
   */
  ParserException(const char *msg, const char *file, int line, uint32_t cursorpos)
      : Exception(ExceptionType::PARSER, msg, file, line), cursorpos_(cursorpos) {}

  /**
   * Creates a new ParserException with the given parameters.
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   * @param cursorpos from libpgquery
   */
  ParserException(const std::string &msg, const char *file, int line, uint32_t cursorpos)
      : Exception(ExceptionType::PARSER, msg.c_str(), file, line), cursorpos_(cursorpos) {}

  /**
   * @return from libpgquery
   */
  uint32_t GetCursorPos() const { return cursorpos_; }
};

/**
 * Specialized Binder exception since we want an error code to get more verbose output
 */
class BinderException : public Exception {
 public:
  BinderException() = delete;

  /**
   * Creates a new ParserException with the given parameters.
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   * @param code sql error code
   */
  BinderException(const char *msg, const char *file, int line, common::ErrorCode code)
      : Exception(ExceptionType::BINDER, msg, file, line), code_(code) {}

  /**
   * Creates a new ParserException with the given parameters.
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   * @param code sql error code
   */
  BinderException(const std::string &msg, const char *file, int line, common::ErrorCode code)
      : Exception(ExceptionType::BINDER, msg.c_str(), file, line), code_(code) {}

  /**
   * sql error code
   */
  common::ErrorCode code_;
};

}  // namespace terrier
