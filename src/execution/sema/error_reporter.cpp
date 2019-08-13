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
constexpr const char *error_strings[] = {MESSAGE_LIST(F)};
#undef F

}  // namespace

void ErrorReporter::MessageArgument::FormatMessageArgument(std::string *str) const {
  switch (kind()) {
    case Kind::CString: {
      str->append(raw_str_);
      break;
    }
    case MessageArgument::Kind::Int: {
      str->append(std::to_string(integer_));
      break;
    }
    case Kind::Position: {
      str->append("[line/col: ")
          .append(std::to_string(pos_.line))
          .append("/")
          .append(std::to_string(pos_.column))
          .append("]");
      break;
    }
    case Kind::Token: {
      str->append(parsing::Token::GetString(static_cast<parsing::Token::Type>(integer_)));
      break;
    }
    case Kind::Type: {
      str->append(ast::Type::ToString(type_));
      break;
    }
  }
}

std::string ErrorReporter::MessageWithArgs::FormatMessage() const {
  std::string msg;

  auto msg_idx = static_cast<std::underlying_type_t<ErrorMessageId>>(error_message_id());

  msg.append("Line: ").append(std::to_string(position().line)).append(", ");
  msg.append("Col: ").append(std::to_string(position().column)).append(" => ");

  const char *fmt = error_strings[msg_idx];
  if (args_.empty()) {
    msg.append(fmt);
    return msg;
  }

  // Need to format
  u32 arg_idx = 0;
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

void ErrorReporter::PrintErrors() {
  std::string error_str;

  for (const auto &error : errors_) {
    error_str.append(error.FormatMessage()).append("\n");
  }

  EXECUTION_LOG_ERROR("{}", error_str.c_str());
}

}  // namespace terrier::execution::sema
