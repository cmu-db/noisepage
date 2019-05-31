#pragma once

#include <string>
#include <utility>
#include <vector>

#include "execution/ast/identifier.h"
#include "execution/parsing/token.h"
#include "execution/sema/error_message.h"
#include "execution/util/common.h"
#include "execution/util/region_containers.h"

namespace tpl {

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
   * Outputs the list of errors
   */
  void PrintErrors();

 private:
  /*
   * A single argument in the error message
   */
  class MessageArgument {
   public:
    enum Kind { CString, Int, Position, Token, Type };

    explicit MessageArgument(const char *str) : kind_(Kind::CString), raw_str_(str) {}

    explicit MessageArgument(i32 integer) : kind_(Kind::Int), integer_(integer) {}

    explicit MessageArgument(ast::Identifier str) : MessageArgument(str.data()) {}

    explicit MessageArgument(ast::Type *type) : kind_(Kind::Type), type_(type) {}

    explicit MessageArgument(const parsing::Token::Type type)
        : MessageArgument(static_cast<std::underlying_type_t<parsing::Token::Type>>(type)) {
      kind_ = Kind::Token;
    }

    explicit MessageArgument(const SourcePosition &pos) : kind_(Kind::Position), pos_(pos) {}

    Kind kind() const { return kind_; }

   private:
    friend class ErrorReporter;
    void FormatMessageArgument(std::string *str) const;

   private:
    Kind kind_;
    union {
      const char *raw_str_;
      i32 integer_;
      SourcePosition pos_;
      ast::Type *type_;
    };
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
        : pos_(pos), id_(message.id), args_(region) {
      args_.insert(args_.end(), {MessageArgument(std::move(args))...});
    }

    const SourcePosition &position() const { return pos_; }

    ErrorMessageId error_message_id() const { return id_; }

   private:
    friend class ErrorReporter;
    std::string FormatMessage() const;

   private:
    const SourcePosition pos_;
    ErrorMessageId id_;
    util::RegionVector<MessageArgument> args_;
  };

 private:
  util::Region *region_;
  util::RegionVector<MessageWithArgs> errors_;
};

}  // namespace sema
}  // namespace tpl
