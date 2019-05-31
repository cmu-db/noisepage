#include "execution/sql_test.h"  // NOLINT

#include "catalog/catalog_defs.h"
#include "execution/sql/execution_structures.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {};

TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //
  auto *exec = sql::ExecutionStructures::Instance();
  auto test_db_ns = exec->GetTestDBAndNS();
  auto txn = exec->GetTxnManager()->BeginTransaction();
  auto catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "empty_table");
  TableVectorIterator iter(!test_db_ns.first, !test_db_ns.second,
                           !catalog_table->Oid(), txn);
  iter.Init();
  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
  exec->GetTxnManager()->Commit(txn, [](void*){}, nullptr);
}

TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto *exec = sql::ExecutionStructures::Instance();
  auto test_db_ns = exec->GetTestDBAndNS();
  auto txn = exec->GetTxnManager()->BeginTransaction();
  auto catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");
  TableVectorIterator iter(!test_db_ns.first, !test_db_ns.second,
                           !catalog_table->Oid(), txn);
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test1_size, num_tuples);
  exec->GetTxnManager()->Commit(txn, [](void*){}, nullptr);
}

TEST_F(TableVectorIteratorTest, NullableTypesIteratorTest) {
  //
  // Ensure we iterate over the whole table even the types of the columns are
  // different
  //

  auto *exec = sql::ExecutionStructures::Instance();
  auto test_db_ns = exec->GetTestDBAndNS();
  auto txn = exec->GetTxnManager()->BeginTransaction();
  auto catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_2");
  TableVectorIterator iter(!test_db_ns.first, !test_db_ns.second,
                           !catalog_table->Oid(), txn);
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test2_size, num_tuples);
  exec->GetTxnManager()->Commit(txn, [](void*){}, nullptr);
}

}  // namespace tpl::sql::test