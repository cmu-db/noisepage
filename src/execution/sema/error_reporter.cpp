#include "execution/sema/error_reporter.h"

#include <cstring>
#include <string>

#include "execution/ast/type.h"
#include "execution/ast/type_visitor.h"
#include "loggers/execution_logger.h"

namespace noisepage::execution::sema {

namespace {
#define F(id, str, arg_types) str,
// NOLINTNEXTLINE
constexpr const char *error_strings[] = {MESSAGE_LIST(F)};
#undef F

// Helper template class for MessageArgument::FormatMessageArgument().
template <class T>
struct AlwaysFalse : std::false_type {};

}  // namespace

void ErrorReporter::MessageArgument::FormatMessageArgument(std::string *str) const {
  std::visit(
      [&](auto &&arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, const char *>) {  // NOLINT
          str->append(arg);
        } else if constexpr (std::is_same_v<T, int32_t>) {  // NOLINT
          str->append(std::to_string(arg));
        } else if constexpr (std::is_same_v<T, SourcePosition>) {  // NOLINT
          str->append("[line/col: ")
              .append(std::to_string(arg.line_))
              .append("/")
              .append(std::to_string(arg.column_))
              .append("]");
        } else if constexpr (std::is_same_v<T, parsing::Token::Type>) {  // NOLINT
          str->append(parsing::Token::GetString(static_cast<parsing::Token::Type>(arg)));
        } else if constexpr (std::is_same_v<T, ast::Type *>) {  // NOLINT
          str->append(ast::Type::ToString(arg));
        } else {
          static_assert(AlwaysFalse<T>::value, "non-exhaustive visitor");
        }
      },
      arg_);
}

std::string ErrorReporter::MessageWithArgs::FormatMessage() const {
  std::string msg;

  auto msg_idx = static_cast<std::underlying_type_t<ErrorMessageId>>(GetErrorMessageId());

  msg.append("Line: ").append(std::to_string(Position().line_)).append(", ");
  msg.append("Col: ").append(std::to_string(Position().column_)).append(" => ");

  const char *fmt = error_strings[msg_idx];
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

    args_[arg_idx++].FormatMessageArgument(&msg);

    fmt = pos + 1;
    while (std::isalnum(*fmt) != 0) {
      fmt++;
    }
  }

  return msg;
}

std::string ErrorReporter::SerializeErrors() {
  std::string error_str;

  for (const auto &error : errors_) {
    error_str.append(error.FormatMessage()).append("\n");
  }

  return error_str;
}

}  // namespace noisepage::execution::sema
