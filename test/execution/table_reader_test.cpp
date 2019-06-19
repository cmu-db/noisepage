#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/tpl_test/schema_reader.h"
#include "execution/tpl_test/table_reader.h"
#include "execution/sql/execution_structures.h"
#include "catalog/catalog.h"
#include "execution/tpl_test.h"  // NOLINT


namespace tpl::compiler::test {

class TableReaderTest : public TplTest {
 public:
  void SetUp() {
    TplTest::SetUp();
    catalog_ = sql::ExecutionStructures::Instance()->GetCatalog();
    txn_mgr_ = sql::ExecutionStructures::Instance()->GetTxnManager();
    txn_ = txn_mgr_->BeginTransaction();
    db_oid_ = sql::ExecutionStructures::Instance()->GetTestDBAndNS().first;
    ns_oid_ = sql::ExecutionStructures::Instance()->GetTestDBAndNS().second;
  }

  ~TableReaderTest() {
    txn_mgr_->Commit(txn_, nullptr, nullptr);
    delete txn_;
  }

 protected:
  terrier::catalog::Catalog* catalog_{nullptr};
  terrier::transaction::TransactionManager* txn_mgr_{nullptr};
  terrier::transaction::TransactionContext* txn_{nullptr};
  terrier::catalog::db_oid_t db_oid_{0};
  terrier::catalog::namespace_oid_t ns_oid_{0};
};

// NOLINTNEXTLINE
TEST_F(TableReaderTest, SimpleSchemaTest) {
  reader::SchemaReader schema_reader{catalog_};
  auto table_info = schema_reader.ReadTableInfo("../sample_tpl/tables/test_1.schema");
  ASSERT_EQ(table_info->schema->GetColumns().size(), 4);
  ASSERT_EQ(table_info->indexes.size(), 1);
}

// NOLINTNEXTLINE
TEST_F(TableReaderTest, SimpleTableReaderTest) {
  // Get Catalog and txn manager
  reader::TableReader table_reader(catalog_, txn_, db_oid_, ns_oid_);
  uint32_t num_written = table_reader.ReadTable("../sample_tpl/tables/test_1.schema", "../sample_tpl/tables/test_1.data");
  ASSERT_EQ(num_written, 5);

}

}