#include <memory>

#include "execution/sql_test.h"  // NOLINT

#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 public:
  terrier::parser::ConstantValueExpression DummyExpr() {
    return terrier::parser::ConstantValueExpression(terrier::type::TransientValueFactory::GetInteger(0));
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};
/*
// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "empty_table");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  iter.Init();
  ASSERT_FALSE(iter.Advance());
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  i32 prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto* val = pci->Get<i32, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test1_size, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, NullableTypesIteratorTest) {
  //
  // Ensure we iterate over the whole table even the types of the columns are
  // different
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  i16 prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // The serial column is the smallest one (SmallInt type), so it ends up at the last index in the storage layer.
      auto* val = pci->Get<i16, false>(3, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test2_size, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, IteratorAddColTest) {
  //
  // Ensure we only iterate over specified columns
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  const auto & schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  iter.AddCol(!schema.GetColumn("col1").Oid());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  i16 prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Because we only specified one column, its index is 0 instead of three
      auto* val = pci->Get<i16, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test2_size, num_tuples);
}
*/
TEST_F(TableVectorIteratorTest, DummyTest) {
  // Make schema
  std::vector<terrier::catalog::Schema::Column> cols;
  cols.emplace_back("colA", terrier::type::TypeId::INTEGER, false, DummyExpr());
  cols.emplace_back("colB", terrier::type::TypeId::INTEGER, false, DummyExpr());
  terrier::catalog::Schema tmp_schema{cols};

  // Make table
  auto table_oid = exec_ctx_->GetAccessor()->CreateTable(NSOid(), "dummy_table", tmp_schema);
  ASSERT_NE(table_oid, terrier::catalog::INVALID_TABLE_OID);
  auto & schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  auto * table = new terrier::storage::SqlTable(BlockStore(), schema);
  exec_ctx_->GetAccessor()->SetTablePointer(table_oid, table);

  // FIRST ISSUE: the schema object itself has been reordered
  // Prints: colB, colA instead of colA, colB
  for (const auto & col : schema.GetColumns()) {
    std::cout << "Col Name: " << col.Name() << std::endl;
  }

  // Now get the column mapping
  std::vector<terrier::catalog::col_oid_t> col_oids;
  for (const auto & col :schema.GetColumns()) {
    col_oids.emplace_back(col.Oid());
  }
  auto pr_map = table->InitializerForProjectedRow(col_oids);
  auto offset_map = pr_map.second;

  // SECOND ISSUE: the sorting order is not stable (with respect to the initial order colA, colB)
  // Prints 1, 0 instead of 0, 1
  auto colA_oid = schema.GetColumn("colA").Oid();
  auto colB_oid = schema.GetColumn("colB").Oid();

  std::cout << "(ColOID, Offset): (" << !colA_oid  << ", " << offset_map[colA_oid] << ")" << std::endl;
  std::cout << "(ColOID, Offset): (" << !colB_oid  << ", " << offset_map[colB_oid] << ")" << std::endl;
}

TEST_F(TableVectorIteratorTest, DummyIndexTest) {
  // Make schema
  std::vector<terrier::catalog::Schema::Column> cols;
  cols.emplace_back("colA", terrier::type::TypeId::INTEGER, false, DummyExpr());
  cols.emplace_back("colB", terrier::type::TypeId::INTEGER, false, DummyExpr());
  terrier::catalog::Schema tmp_schema{cols};

  // Make table
  auto table_oid = exec_ctx_->GetAccessor()->CreateTable(NSOid(), "dummy_table", tmp_schema);
  ASSERT_NE(table_oid, terrier::catalog::INVALID_TABLE_OID);
  auto & schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  auto * table = new terrier::storage::SqlTable(BlockStore(), schema);
  exec_ctx_->GetAccessor()->SetTablePointer(table_oid, table);

  // FIRST ISSUE: the schema object itself has been reordered
  // Prints: colB, colA instead of colA, colB
  for (const auto & col : schema.GetColumns()) {
    std::cout << "Col Name: " << col.Name() << std::endl;
  }

  // Now get the column mapping
  auto colA_oid = schema.GetColumn("colA").Oid();
  auto colB_oid = schema.GetColumn("colB").Oid();
  std::vector<terrier::catalog::col_oid_t> col_oids{colA_oid, colB_oid};
  auto pr_map = table->InitializerForProjectedRow(col_oids);
  auto offset_map = pr_map.second;

  // SECOND ISSUE: the sorting order is not stable (with respect to the initial order colA, colB)
  // Prints (1, 1), (2, 0) instead of (2, 0), (1, 1)
  std::cout << "(ColOID, Offset): (" << !colA_oid  << ", " << offset_map[colA_oid] << ")" << std::endl;
  std::cout << "(ColOID, Offset): (" << !colB_oid  << ", " << offset_map[colB_oid] << ")" << std::endl;
}

}  // namespace tpl::sql::test
