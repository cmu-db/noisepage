#include "parser/postgresparser.h"
#include <string>
#include <vector>

#include "util/test_harness.h"

namespace terrier::parser {

/**
 * A basic test for select statements.
 */
// NOLINTNEXTLINE
TEST(PGParserTests, SelectTest) {
  auto pgparser = PostgresParser::GetInstance();
  auto stmts = pgparser.BuildParseTree("SELECT * FROM foo;");

  EXPECT_EQ(stmts.size(), 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::SELECT);

  auto stmt = std::move(stmts[0]);
  auto select_stmt = reinterpret_cast<SelectStatement *>(stmt.get());
  EXPECT_EQ(select_stmt->from_->table_info_->table_name_, "foo");
  EXPECT_EQ(select_stmt->select_[0]->GetExpressionType(), ExpressionType::STAR);
}

}  // namespace terrier::parser
