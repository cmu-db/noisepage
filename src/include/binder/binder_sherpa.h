#pragma once

namespace terrier {

namespace parser {
class ParseResult;
};

namespace binder {
/**
 * Sherps things.
 */
class BinderSherpa {
 public:
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result) : parse_result_(parse_result) {}

  common::ManagedPointer<parser::ParseResult> GetParseResult() { return parse_result_; }

 private:
  const common::ManagedPointer<parser::ParseResult> parse_result_;
};
}  // namespace binder

}  // namespace terrier