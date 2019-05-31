#include "execution/sql_test.h"  // NOLINT

#include "execution/catalog/catalog_defs.h"
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
  auto catalog_table = exec->GetCatalog()->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "empty_table");

  TableVectorIterator iter(static_cast<uint32_t>(catalog::DEFAULT_DATABASE_OID),
                           static_cast<uint32_t>(catalog_table->Oid()));
  iter.Init();
  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
}

TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto *exec = sql::ExecutionStructures::Instance();
  auto catalog_table = exec->GetCatalog()->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "test_1");

  TableVectorIterator iter(static_cast<uint32_t>(catalog::DEFAULT_DATABASE_OID),
                           static_cast<uint32_t>(catalog_table->Oid()));
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
}

TEST_F(TableVectorIteratorTest, NullableTypesIteratorTest) {
  //
  // Ensure we iterate over the whole table even the types of the columns are
  // different
  //

  auto *exec = sql::ExecutionStructures::Instance();
  auto catalog_table = exec->GetCatalog()->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "test_2");

  TableVectorIterator iter(static_cast<uint32_t>(catalog::DEFAULT_DATABASE_OID),
                           static_cast<uint32_t>(catalog_table->Oid()));
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
}

}  // namespace tpl::sql::test