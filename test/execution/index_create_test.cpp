#include "execution/compiler/compilation_context.h"
#include "execution/sql/ddl_executors.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "planner/plannodes/create_index_plan_node.h"
namespace noisepage::execution::sql::test {

class IndexCreateTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 protected:
  void CreateIndex(catalog::table_oid_t table_oid, const std::string &index_name,
                   std::unique_ptr<catalog::IndexSchema> schema) {
    planner::CreateIndexPlanNode::Builder builder;
    auto plan_node = builder.SetNamespaceOid(NSOid())
                         .SetTableOid(table_oid)
                         .SetIndexName(index_name)
                         .SetSchema(std::move(schema))
                         .SetOutputSchema(std::make_unique<planner::OutputSchema>(planner::OutputSchema()))
                         .Build();

    EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
        common::ManagedPointer<planner::CreateIndexPlanNode>(plan_node),
        common::ManagedPointer<catalog::CatalogAccessor>(exec_ctx_->GetAccessor())));
    auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), index_name);
    EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
    auto index_ptr = exec_ctx_->GetAccessor()->GetIndex(index_oid);
    EXPECT_NE(index_ptr, nullptr);

    auto query = execution::compiler::CompilationContext::Compile(*plan_node, exec_ctx_->GetExecutionSettings(),
                                                                  exec_ctx_->GetAccessor());

    query->Run(common::ManagedPointer<execution::exec::ExecutionContext>(exec_ctx_.get()),
               execution::vm::ExecutionMode::Interpret);
  }

  void VerifyIndexResult(catalog::table_oid_t table_oid, const std::string &index_name, std::vector<uint32_t> col_oids,
                         uint32_t key_num) {
    auto sql_table = exec_ctx_->GetAccessor()->GetTable(table_oid);
    auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), index_name);
    TableVectorIterator table_iter(exec_ctx_.get(), uint32_t(table_oid), col_oids.data(),
                                   static_cast<uint32_t>(col_oids.size()));
    IndexIterator index_iter{exec_ctx_.get(),     1,
                             uint32_t(table_oid), uint32_t(index_oid),
                             col_oids.data(),     static_cast<uint32_t>(col_oids.size())};
    table_iter.Init();
    index_iter.Init();
    VectorProjectionIterator *vpi = table_iter.GetVectorProjectionIterator();
    auto result_num = col_oids.size();

    // Iterate through the table.
    while (table_iter.Advance()) {
      for (; vpi->HasNext(); vpi->Advance()) {
        auto *const index_pr(index_iter.PR());
        for (uint32_t i = 0; i < key_num; ++i) {
          auto *key = vpi->GetValue<int32_t, false>(i, nullptr);
          index_pr->Set<int32_t, false>(i, *key, false);
        }
        // Check that the key can be recovered through the index
        index_iter.ScanKey();
        // One entry should be found
        ASSERT_TRUE(index_iter.Advance());
        // Get directly from iterator
        auto *const table_pr(index_iter.TablePR());
        for (uint32_t i = 0; i < result_num; ++i) {
          auto table_val UNUSED_ATTRIBUTE = vpi->GetValue<int32_t, false>(i, nullptr);
          auto indexed_val UNUSED_ATTRIBUTE = table_pr->Get<int32_t, false>(i, nullptr);
          ASSERT_EQ(*table_val, *indexed_val);
        }
        // Get indirectly from tuple slot
        storage::TupleSlot slot(index_iter.CurrentSlot());
        ASSERT_TRUE(sql_table->Select(exec_ctx_->GetTxn(), slot, index_iter.TablePR()));
        for (uint32_t i = 0; i < result_num; ++i) {
          auto table_val UNUSED_ATTRIBUTE = vpi->GetValue<int32_t, false>(i, nullptr);
          auto tuple_val UNUSED_ATTRIBUTE = table_pr->Get<int32_t, false>(i, nullptr);
          ASSERT_EQ(*table_val, *tuple_val);
        }

        // Check that there are no more entries.
        ASSERT_FALSE(index_iter.Advance());
      }
      vpi->Reset();
    }
  }
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(IndexCreateTest, SimpleIndexCreate) {
  // CREATE INDEX test_index on index_test_table(colA)
  // Select colA from table and index to verify
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "index_test_table");
  auto &table_schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  // Create Index Schema
  std::vector<catalog::IndexSchema::Column> index_cols;
  const auto &table_col = table_schema.GetColumn("colA");
  parser::ColumnValueExpression col_expr(table_oid, table_col.Oid(), table_col.Type());
  index_cols.emplace_back("index_colA", type::TypeId::INTEGER, false, col_expr);

  catalog::IndexSchema tmp_index_schema{index_cols, storage::index::IndexType::BWTREE, false, false, false, false};

  CreateIndex(table_oid, "indexA", std::make_unique<catalog::IndexSchema>(tmp_index_schema));

  // verify index population
  VerifyIndexResult(table_oid, "indexA", {1}, 1);
}

// NOLINTNEXTLINE
TEST_F(IndexCreateTest, SimpleIndexCreate2) {
  // CREATE INDEX test_index on index_test_table(colE)
  // Select * from table and index to verify
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "index_test_table");
  auto &table_schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  // Create Index Schema
  std::vector<catalog::IndexSchema::Column> index_cols;
  const auto &table_col = table_schema.GetColumn("colE");
  parser::ColumnValueExpression col_expr(table_oid, table_col.Oid(), table_col.Type());
  index_cols.emplace_back("index_colE", type::TypeId::INTEGER, false, col_expr);

  catalog::IndexSchema tmp_index_schema{index_cols, storage::index::IndexType::BWTREE, false, false, false, false};

  CreateIndex(table_oid, "indexE", std::make_unique<catalog::IndexSchema>(tmp_index_schema));

  // verify index population
  VerifyIndexResult(table_oid, "indexE", {1, 2, 3, 4, 5}, 1);
}

// NOLINTNEXTLINE
TEST_F(IndexCreateTest, MultiColumnIndexCreate) {
  // CREATE INDEX test_index on index_test_table(colA, colB)
  // Select colA, colB, colC, colD, from table and index to verify
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "index_test_table");
  auto &table_schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  // Create Index Schema
  std::vector<catalog::IndexSchema::Column> index_cols;
  const auto &table_col_a = table_schema.GetColumn("colA");
  parser::ColumnValueExpression col_expr_a(table_oid, table_col_a.Oid(), table_col_a.Type());
  index_cols.emplace_back("index_colA", type::TypeId::INTEGER, false, col_expr_a);

  const auto &table_col_b = table_schema.GetColumn("colB");
  parser::ColumnValueExpression col_expr_b(table_oid, table_col_b.Oid(), table_col_b.Type());
  index_cols.emplace_back("index_colB", type::TypeId::INTEGER, false, col_expr_b);

  catalog::IndexSchema tmp_index_schema{index_cols, storage::index::IndexType::BWTREE, false, false, false, false};

  CreateIndex(table_oid, "indexAB", std::make_unique<catalog::IndexSchema>(tmp_index_schema));

  // verify index population
  VerifyIndexResult(table_oid, "indexAB", {1, 2, 3, 4}, 2);
}

}  // namespace noisepage::execution::sql::test
