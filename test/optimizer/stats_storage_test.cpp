#include "optimizer/statistics/stats_storage.h"

#include "gtest/gtest.h"
#include "storage/sql_table.h"
#include "test_util/end_to_end_test.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class StatsStorageTests : public test::EndToEndTest {
 public:
  void SetUp() override { EndToEndTest::SetUp(); }
};

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, EmptyTableTest) {
  std::string table_name = "emptyTable";
  std::string col1_name = "a";
  std::string col2_name = "b";
  RunQuery("CREATE TABLE " + table_name + "(" + col1_name + " int, " + col2_name + " int);");
  //  auto schema = accessor_->GetSchema(accessor_->GetTableOid(table_name)).
  //  stats_storage_.MarkStatsStale()
}

}  // namespace noisepage::optimizer
