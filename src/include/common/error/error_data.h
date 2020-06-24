#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/error/error_code.h"
#include "common/error/error_defs.h"
#include "common/macros.h"

namespace terrier::common {
class ErrorData {
 public:
  ErrorData() = delete;
  ErrorData &operator=(const ErrorData &other) = default;
  ErrorData &operator=(ErrorData &&other) = default;
  ErrorData(ErrorData &&other) = default;
  ErrorData(const ErrorData &other) = default;

  ErrorData(const common::ErrorSeverity severity, std::string_view message, const ErrorCode code)
      : severity_(severity),
        message_(std::string(message)),
        code_(code),
        fields_({{ErrorField::SEVERITY, std::string(ErrorSeverityToString(severity))},
                 {ErrorField::SEVERITY_LOCALIZED, std::string(ErrorSeverityToString(severity))}}) {}

  void AddField(ErrorField field, const std::string_view message) {
    TERRIER_ASSERT(field != ErrorField::HUMAN_READABLE_ERROR, "ErrorData already contains a required message.");
    TERRIER_ASSERT(field != ErrorField::CODE, "ErrorData already contains a required code.");
    fields_.emplace_back(field, std::string(message));
  }

  ErrorSeverity GetSeverity() const { return severity_; }
  const std::string &GetMessage() const { return message_; }
  ErrorCode GetCode() const { return code_; }
  const std::vector<std::pair<ErrorField, std::string>> &Fields() const { return fields_; }

 private:
  ErrorSeverity severity_ = ErrorSeverity::ERROR;
  std::string message_;
  ErrorCode code_;
  std::vector<std::pair<ErrorField, std::string>> fields_;
};  // namespace terrier::common
}  // namespace terrier::common
