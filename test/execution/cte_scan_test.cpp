#include "execution/sql/storage_interface.h"
#include "execution/sql/cte_scan_iterator.h"

#include <array>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"

namespace terrier::execution::sql::test {
  class CTEScanTest : public SqlBasedTest {
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

  TEST_F(CTEScanTest, CTEInitTest) {
    // INSERT INTO cte_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 and 505.

    // initialize the test_1 and the index on the table
    auto table_oid1 = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
    auto index_oid1 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

    // Select colA only
    std::array<uint32_t, 1> col_oids{1};

    uint32_t cte_table_col_type[1] = {4};

    // The index iterator gives us the slots to update.
    IndexIterator index_iter1{
        exec_ctx_.get(), 1, !table_oid1, !index_oid1, col_oids.data(), static_cast<uint32_t>(col_oids.size())};
    index_iter1.Init();

    // TODO (Gautam): StorageInterface for the CTE's
    std::vector<catalog::col_oid_t> cte_table_col_oids(col_oids.data(),
                                                       col_oids.data() + static_cast<uint32_t>(col_oids.size()));

    // TODO (Gautam) : Check if the materialized tuple can be used to make the schema
    // Using the store that was used in the set up
    auto cte_scan = new terrier::execution::sql::CteScanIterator(exec_ctx_.get(), cte_table_col_type, 1);

    auto cte_table = cte_scan->GetTable();

  }
}