#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/error/error_code.h"
#include "common/error/error_defs.h"
#include "common/macros.h"

namespace noisepage::common {

/**
 * Class to maintain error state when they occur in the system. These can be a variety of levels (@see ErrorSeverity).
 * They contain a number of fields (@see ErrorField), but only two are required: message and code (@see ErrorCode).
 * These are meant to provide verbose error output to the client (@see PostgresPacketWriter::WriteError), and are not
 * intended to be relied on for control flow.
 */
class ErrorData {
 public:
  /**
   * Don't need a default constructor, we want to force some fields to be populated by the user.
   */
  ErrorData() = delete;

  /**
   * copy assignment operator
   */
  ErrorData &operator=(const ErrorData &other) = default;

  /**
   * move assignment operator
   */
  ErrorData &operator=(ErrorData &&other) = default;

  /**
   * copy constructor
   */
  ErrorData(const ErrorData &other) = default;

  /**
   * move constructor
   */
  ErrorData(ErrorData &&other) = default;

  /**
   * @param severity how serious is this error. Severity determines if it will be written back to the client as an Error
   * or a Notice message. @see PostgresPacketWriter::WriteError
   * @param message human readable error message
   * @param code sql error code
   */
  ErrorData(const common::ErrorSeverity severity, std::string_view message, const ErrorCode code)
      : severity_(severity),
        message_(std::string(message)),
        code_(code),
        fields_({{ErrorField::SEVERITY, std::string(ErrorSeverityToString(severity))},
                 {ErrorField::SEVERITY_LOCALIZED, std::string(ErrorSeverityToString(severity))}}) {}

  /**
   * @param field field type. There should not be duplicates
   * @param message value of this field to be sent back to the client
   */
  void AddField(ErrorField field, const std::string_view message) {
    NOISEPAGE_ASSERT(field != ErrorField::HUMAN_READABLE_ERROR, "ErrorData already contains a required message.");
    NOISEPAGE_ASSERT(field != ErrorField::CODE, "ErrorData already contains a required code.");
    fields_.emplace_back(field, std::string(message));
  }

  /**
   * @return severity level
   */
  ErrorSeverity GetSeverity() const { return severity_; }

  /**
   * @return human readable error message
   */
  const std::string &GetMessage() const { return message_; }

  /**
   * @return sql error code
   */
  ErrorCode GetCode() const { return code_; }

  /**
   * @return all of the error message fields to write to the client
   */
  const std::vector<std::pair<ErrorField, std::string>> &Fields() const { return fields_; }

 private:
  ErrorSeverity severity_;
  std::string message_;
  ErrorCode code_;
  std::vector<std::pair<ErrorField, std::string>> fields_;
};  // namespace noisepage::common
}  // namespace noisepage::common
