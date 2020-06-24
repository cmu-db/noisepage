#pragma once

#include <string>
#include <string_view>

#include "common/error/error_code.h"
#include "common/error/error_defs.h"

namespace terrier::common {
class ErrorData {
 private:
  ErrorData(const common::ErrorSeverity severity, std::string_view message,
            std::vector<std::pair<ErrorField, std::string>> &&fields)
      : severity_(severity), message_(std::string(message)), fields_(std::move(fields)) {}

  ErrorData(const common::ErrorSeverity severity, std::string &&message,
            std::vector<std::pair<ErrorField, std::string>> &&fields)
      : severity_(severity), message_(std::move(message)), fields_(std::move(fields)) {}

 public:
  ErrorData() = delete;
  ErrorData &operator=(const ErrorData &other) = default;
  ErrorData &operator=(ErrorData &&other) = default;
  ErrorData(ErrorData &&other) = default;
  ErrorData(const ErrorData &other) = default;

  template <ErrorSeverity severity = ErrorSeverity::ERROR>
  static ErrorData Message(const std::string_view message) {
    return {severity,
            message,
            {{ErrorField::SEVERITY, std::string(ErrorSeverityToString<severity>())},
             {ErrorField::SEVERITY_LOCALIZED, std::string(ErrorSeverityToString<severity>())}}};
  }

  void AddField(ErrorField field, const std::string_view message) { fields_.emplace_back(field, std::string(message)); }

  void AddField(ErrorField field, std::string &&message) { fields_.emplace_back(field, std::move(message)); }

  const std::string &GetMessage() const { return message_; }

  template <ErrorCode code>
  void AddCode() {
    AddField(ErrorField::CODE, ErrorCodeToString<code>());
  }

  ErrorSeverity GetSeverity() const { return severity_; }
  const std::vector<std::pair<ErrorField, std::string>> &Fields() const { return fields_; }

 private:
  ErrorSeverity severity_ = ErrorSeverity::ERROR;
  std::string message_;
  std::vector<std::pair<ErrorField, std::string>> fields_;
};
}  // namespace terrier::common
