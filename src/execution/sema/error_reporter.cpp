#include "execution/sema/error_reporter.h"

#include <cstring>
#include <string>

#include "execution/ast/type.h"
#include "execution/ast/type_visitor.h"

#include "loggers/execution_logger.h"

namespace terrier::execution::sema {

namespace {
#define F(id, str, arg_types) str,
// NOLINTNEXTLINE
constexpr const char *kErrorStrings[] = {MESSAGE_LIST(F)};
#undef F


// Helper template class for MessageArgument::FormatMessageArgument().
template <class T>
struct always_false : std::false_type {};

}  // namespace

void ErrorReporter::MessageArgument::FormatMessageArgument(std::string &str) const {
  std::visit(
      [&](auto &&arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, const char *>) {
          str.append(arg);
        } else if constexpr (std::is_same_v<T, int32_t>) {
          str.append(std::to_string(arg));
        } else if constexpr (std::is_same_v<T, SourcePosition>) {
          str.append("[line/col: ")
              .append(std::to_string(arg.line_))
              .append("/")
              .append(std::to_string(arg.column_))
              .append("]");
        } else if constexpr (std::is_same_v<T, parsing::Token::Type>) {
          str.append(parsing::Token::GetString(static_cast<parsing::Token::Type>(arg)));
        } else if constexpr (std::is_same_v<T, ast::Type *>) {
          str.append(ast::Type::ToString(arg));
        } else {
          static_assert(always_false<T>::value, "non-exhaustive visitor");
        }
      },
      arg_);
}

std::string ErrorReporter::MessageWithArgs::FormatMessage() const {
  std::string msg;

  auto msg_idx = static_cast<std::underlying_type_t<ErrorMessageId>>(GetErrorMessageId());

  msg.append("Line: ").append(std::to_string(Position().line_)).append(", ");
  msg.append("Col: ").append(std::to_string(Position().column_)).append(" => ");

  const char *fmt = kErrorStrings[msg_idx];
  if (args_.empty()) {
    msg.append(fmt);
    return msg;
  }

  // Need to format
  uint32_t arg_idx = 0;
  while (fmt != nullptr) {
    const char *pos = strchr(fmt, '%');
    if (pos == nullptr) {
      msg.append(fmt);
      break;
    }

    msg.append(fmt, pos - fmt);

    args_[arg_idx++].FormatMessageArgument(msg);

    fmt = pos + 1;
    while (std::isalnum(*fmt) != 0) {
      fmt++;
    }
  }

  return msg;
}

void ErrorReporter::PrintErrors(std::ostream &os) {
  for (const auto &error : errors_) {
    os << error.FormatMessage() << "\n";
  }
}

}  // namespace terrier::execution::sema
