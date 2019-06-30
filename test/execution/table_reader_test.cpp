#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/sql/table_generator/schema_reader.h"
#include "execution/sql/table_generator/table_reader.h"
#include "execution/sql_test.h"  // NOLINT

namespace tpl::compiler::test {

class TableReaderTest : public SqlBasedTest {
 public:
  void SetUp() {
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
  }

 protected:
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(TableReaderTest, DISABLED_SimpleSchemaTest) {
  sql::SchemaReader schema_reader{exec_ctx_->GetAccessor()};
  auto table_info = schema_reader.ReadTableInfo("../sample_tpl/tables/test_1.schema");

  EXPECT_EQ(table_info->schema->GetColumns().size(), 4);
  EXPECT_EQ(table_info->indexes.size(), 1);
  // TODO(Amadou): test with columns types

}

// NOLINTNEXTLINE
TEST_F(TableReaderTest, DISABLED_SimpleTableReaderTest) {
  // Get Catalog and txn manager
  sql::TableReader table_reader(exec_ctx_.get());
  uint32_t num_written =
      table_reader.ReadTable("../sample_tpl/tables/test_1.schema", "../sample_tpl/tables/test_1.data");
  ASSERT_EQ(num_written, 6);
  // TODO(Amadou): Iterate through table to see if it's correct.
}

}  // namespace tpl::compiler::test
