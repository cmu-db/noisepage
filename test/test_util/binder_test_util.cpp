#include "test_util/binder_test_util.h"

#include <stdexcept>

#include "parser/postgresparser.h"

namespace noisepage {

std::pair<std::unique_ptr<parser::ParseResult>, common::ManagedPointer<parser::SelectStatement>>
BinderTestUtil::ParseToSelectStatement(const std::string &sql) {
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);
  if (parse_tree->GetStatement(0)->GetType() != parser::StatementType::SELECT) {
    // Just die, don't really care how
    throw std::runtime_error{""};
  }
  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();
  return std::make_pair(std::move(parse_tree), select);
}

}  // namespace noisepage
