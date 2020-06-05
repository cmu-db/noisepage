#include <array>
#include <memory>
#include <vector>

#include "execution/sql/iter_cte_scan_iterator.h"
#include "execution/sql/storage_interface.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

class IterCTEScanTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

TEST_F(IterCTEScanTest, IterCTEInitTest) {
  // Check the mapping of col_oids to the col_ids in the constructed table

}

TEST_F(IterCTEScanTest, IterCTESingleInsertTest) {
  // Simple single-iteration insert into cte_table
  // INSERT INTO cte_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 and 505.

}

TEST_F(IterCTEScanTest, IterCTEMultipleInsertTest) {
  // Multiple iterations of accumulations and insertions into cte_table
}

}  // namespace terrier::execution::sql::test
