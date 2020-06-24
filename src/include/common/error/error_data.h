#pragma once

#include "common/error/error_defs.h"

namespace terrier::common {
class ErrorData {
 public:
  ErrorData(const common::ErrorSeverity severity, std::vector<std::pair<ErrorField, std::string>> &&fields)
      : severity_(severity), fields_(std::move(fields)) {}

  ErrorData() = default;
  ErrorData &operator=(const ErrorData &other) = default;
  ErrorData &operator=(ErrorData &&other) noexcept = default;
  ErrorData(ErrorData &&other) noexcept = default;
  ErrorData(const ErrorData &other) = default;

  template <ErrorSeverity severity = ErrorSeverity::ERROR>
  static ErrorData Message(const std::string_view message) {
    return {severity,
            {{ErrorField::SEVERITY, std::string(ErrorSeverityToString<severity>())},
             {ErrorField::SEVERITY_LOCALIZED, std::string(ErrorSeverityToString<severity>())},
             {ErrorField::HUMAN_READABLE_ERROR, std::string(message)}}};
  }

  // TODO(Matt): maybe needed eventually?
  //  template <ErrorSeverity severity = ErrorSeverity::ERROR>
  //  static ErrorData Message(std::string &&message) {
  //    return {severity,
  //            {{ErrorField::SEVERITY, std::string(ErrorSeverityToString<severity>())},
  //             {ErrorField::SEVERITY_OLD, std::string(ErrorSeverityToString<severity>())},
  //             {ErrorField::HUMAN_READABLE_ERROR, std::move(message)}}};
  //  }

  void AddField(ErrorField field, const std::string_view message) { fields_.emplace_back(field, std::string(message)); }

  void AddField(ErrorField field, std::string &&message) { fields_.emplace_back(field, std::move(message)); }

  ErrorSeverity GetSeverity() const { return severity_; }
  const std::vector<std::pair<ErrorField, std::string>> &Fields() const { return fields_; }

 private:
  ErrorSeverity severity_ = ErrorSeverity::ERROR;
  std::vector<std::pair<ErrorField, std::string>> fields_;
};
}  // namespace terrier::common
