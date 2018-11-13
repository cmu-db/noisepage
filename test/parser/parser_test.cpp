#include "parser/postgresparser.h"
#include <string>
#include <vector>

#include "util/test_harness.h"

namespace terrier::parser {

class ParserTestBase : public TerrierTest {
 protected:
  /**
   * Initialization
   */
  void SetUp() override {
    pgparser = PostgresParser::GetInstance();
  }

  /*
  void InitBasicQueries() {
    // Aggregate
    queries.emplace_back("SELECT * FROM foo;");
    queries.emplace_back("SELECT COUNT(*) FROM foo;");
    queries.emplace_back("SELECT COUNT(DISTINCT id) FROM foo;");
    queries.emplace_back("SELECT MAX(*) FROM foo;");
    queries.emplace_back("SELECT MIN(*) FROM foo;");

    // GROUP BY
    queries.emplace_back("SELECT * FROM foo GROUP BY id,"
			 "name HAVING id > 10;");
    
    // queries.emplace_back("");
  }
  */

  void TearDown() override {
    // cleanup calls here
  }

  void CheckTable(const std::unique_ptr<TableInfo> &table_info, std::string table_name) {
    EXPECT_EQ(table_info->table_name_, table_name);
  }

  PostgresParser pgparser;
};

// NOLINTNEXTLINE  
TEST_F(ParserTestBase, DropDBTest) {
  auto stmts = pgparser.BuildParseTree("DROP DATABASE test_db;");
  EXPECT_EQ(stmts.size(), 1);
  
  auto drop_stmt = reinterpret_cast<DropStatement *>(stmts[0].get());
  EXPECT_EQ(drop_stmt->type_, DropStatement::DropType::kDatabase);
  EXPECT_EQ(drop_stmt->GetDatabaseName(), "test_db");  
}
  
// NOLINTNEXTLINE  
TEST_F(ParserTestBase, DropTableTest) {
  auto stmts = pgparser.BuildParseTree("DROP TABLE test_db;");
  EXPECT_EQ(stmts.size(), 1);
  
  auto drop_stmt = reinterpret_cast<DropStatement *>(stmts[0].get());
  EXPECT_EQ(drop_stmt->type_, DropStatement::DropType::kTable);  
  EXPECT_EQ(drop_stmt->GetTableName(), "test_db");
}  

/**
 * A basic test for select statements.
 */
// NOLINTNEXTLINE
TEST_F(ParserTestBase, SelectTest) {  
  auto stmts = pgparser.BuildParseTree("SELECT * FROM foo;");

  EXPECT_EQ(stmts.size(), 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::SELECT);

  auto stmt = std::move(stmts[0]);
  auto select_stmt = reinterpret_cast<SelectStatement *>(stmt.get());
  EXPECT_EQ(select_stmt->from_->table_info_->table_name_, "foo");
  // CheckTable(select_stmt->from_->table_info_, std::string("foo"));
  EXPECT_EQ(select_stmt->select_[0]->GetExpressionType(), ExpressionType::STAR);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, UpdateTest) {    
  auto stmts = pgparser.BuildParseTree("UPDATE students SET grade = 1.0;");

  EXPECT_EQ(stmts.size(), 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::UPDATE);

  auto stmt = std::move(stmts[0]);
  auto update_stmt = reinterpret_cast<UpdateStatement *>(stmt.get());
  EXPECT_EQ(update_stmt->table_->table_info_->table_name_, "students");
  // check expression here
  EXPECT_EQ(update_stmt->where_, nullptr);
}

// NOLINTNEXTLINE


}  // namespace terrier::parser
