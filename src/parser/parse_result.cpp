#include "parser/parse_result.h"

#include "parser/sql_statement.h"

namespace terrier::parser {
void ParseResult::AddStatement(std::unique_ptr<SQLStatement> statement) {
  statements_.emplace_back(std::move(statement));
}
}  // namespace terrier::parser
