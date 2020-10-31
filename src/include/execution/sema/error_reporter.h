#pragma once

#include <iosfwd>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "execution/ast/identifier.h"
#include "execution/parsing/token.h"
#include "execution/sema/error_message.h"
#include "execution/util/execution_common.h"
#include "execution/util/region_containers.h"

namespace noisepage::execution {

namespace ast {
class Type;
}  // namespace ast

namespace sema {

namespace detail {

/**
 * Argument that's passed in
 * @tparam T type of the argument
 */
template <typename T>
struct PassArgument {
  /**
   * type of the argument
   */
  using type = T;
};

}  // namespace detail

/**
 * Used to report errors found during compilation.
 */
class ErrorReporter {
 public:
  /**
   * Constructor
   * @param region region to use for allocation
   */
  explicit ErrorReporter(util::Region *region) : region_(region), errors_(region) {}

  /**
   * Record an error
   * @tparam ArgTypes types of the arguments
   * @param pos source position
   * @param message error message
   * @param args arguments to pass
   */
  template <typename... ArgTypes>
  void Report(const SourcePosition &pos, const ErrorMessage<ArgTypes...> &message,
              typename detail::PassArgument<ArgTypes>::type... args) {
    errors_.emplace_back(region_, pos, message, std::forward<ArgTypes>(args)...);
  }

  /**
   * Have any errors been reported?
   */
  bool HasErrors() const { return !errors_.empty(); }

  /**
   * Clears the list of errors
   */
  void Reset() { errors_.clear(); }

  /**
   * Serializes the list of errors
   */
  std::string SerializeErrors();

 private:
  /*
   * A single argument in the error message
   */
  class MessageArgument {
   public:
    explicit MessageArgument(const char *str) : arg_(str) {}

    explicit MessageArgument(ast::Identifier str) : MessageArgument(str.GetData()) {}

    explicit MessageArgument(SourcePosition pos) : arg_(pos) {}

    explicit MessageArgument(int32_t integer) : arg_(integer) {}

    explicit MessageArgument(ast::Type *type) : arg_(type) {}

    explicit MessageArgument(parsing::Token::Type type) : arg_(type) {}

   private:
    friend class ErrorReporter;
    void FormatMessageArgument(std::string *str) const;

    std::variant<const char *, int32_t, SourcePosition, parsing::Token::Type, ast::Type *> arg_;
  };

  /*
   * An encapsulated error message with proper argument types that can be
   * formatted and printed.
   */
  class MessageWithArgs {
   public:
    template <typename... ArgTypes>
    MessageWithArgs(util::Region *region, const SourcePosition &pos, const ErrorMessage<ArgTypes...> &message,
                    typename detail::PassArgument<ArgTypes>::type... args)
        : pos_(pos), id_(message.id_), args_(region) {
      args_.insert(args_.end(), {MessageArgument(std::move(args))...});
    }

    const SourcePosition &Position() const { return pos_; }

    ErrorMessageId GetErrorMessageId() const { return id_; }

   private:
    friend class ErrorReporter;
    std::string FormatMessage() const;

   private:
    const SourcePosition pos_;
    ErrorMessageId id_;
    util::RegionVector<MessageArgument> args_;
  };

 private:
  // Memory region
  util::Region *region_;

  // List of all errors
  util::RegionVector<MessageWithArgs> errors_;
};

}  // namespace sema
}  // namespace noisepage::execution
