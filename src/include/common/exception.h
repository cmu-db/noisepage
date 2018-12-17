#pragma once

#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>

namespace terrier {

/**
 * Use the macros below for generating exceptions.
 * They record where the exception was generated.
 */

#define NOT_IMPLEMENTED_EXCEPTION(msg) NotImplementedException(msg, __FILE__, __LINE__)
#define PARSER_EXCEPTION(msg) ParserException(msg, __FILE__, __LINE__)

/**
 * Exception types
 */
enum class ExceptionType { RESERVED = 0, NOT_IMPLEMENTED = 1, PARSER = 2 };

class Exception : public std::runtime_error {
 public:
  Exception(const ExceptionType type, const char *msg, const char *file, int line)
      : std::runtime_error(msg), type_(type), file_(file), line_(line) {}

  /**
   * Allows type and source location of the exception to be recorded in the
   * log, at the catch point.
   */
  friend std::ostream &operator<<(std::ostream &out, const Exception &ex) {
    out << ex.get_type() << " exception:";
    out << ex.get_file() << ":";
    out << ex.get_line() << ":";
    out << ex.what();
    return out;
  }

  /**
   * Return the exception type
   */
  const char *get_type() const {
    switch (type_) {
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not Implemented";
      case ExceptionType::PARSER:
        return "Parser";
      default:
        return "Unknown exception type";
    }
  }
  const char *get_file() const { return file_; }
  const int get_line() const { return line_; }

 protected:
  const ExceptionType type_;
  const char *file_;
  const int line_;
};

// -----------------------
// Derived exception types
// -----------------------

#define DEFINE_EXCEPTION(e_name, e_type)                                                        \
  class e_name : public Exception {                                                             \
    e_name() = delete;                                                                          \
                                                                                                \
   public:                                                                                      \
    e_name(const char *msg, const char *file, int line) : Exception(e_type, msg, file, line) {} \
  }

DEFINE_EXCEPTION(NotImplementedException, ExceptionType::NOT_IMPLEMENTED);
DEFINE_EXCEPTION(ParserException, ExceptionType::PARSER);

}  // namespace terrier
