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
  common::ManagedPointer<parser::ParseResult> GetParseResult();
};
}

}