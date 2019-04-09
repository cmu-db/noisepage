
#include "storage/index/index_populator.h"
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_sql_table.h"
#include "storage/index/index_builder.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::storage::index {
struct IndexPopulatorTest : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    catalog_ = new catalog::Catalog(txn_manager_);
    txn_ = txn_manager_->BeginTransaction();
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    TerrierTest::TearDown();
    delete catalog_;  // delete catalog first
    delete txn_manager_;
    delete txn_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
};

// Check the basic correctness of index populator
// NOLINTNEXTLINE
TEST_F(IndexPopulatorTest, BasicCorrectnessTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("sex", type::TypeId::BOOLEAN, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("address", type::TypeId::VARCHAR, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table = table_handle.CreateTable(txn_, schema, "test_table");
  auto table_entry = table_handle.GetTableEntry(txn_, "test_table");
  EXPECT_NE(table_entry, nullptr);
  std::string_view str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(0));
  EXPECT_EQ(str, "public");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(1));
  EXPECT_EQ(str, "test_table");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(2));
  EXPECT_EQ(str, "pg_default");

  // Insert a few rows into the table
  auto ptr = table_handle.GetTable(txn_, "test_table");
  EXPECT_EQ(ptr, table);

  for (int i = 0; i < 100; ++i) {
    std::vector<type::TransientValue> row;
    std::ostringstream stringStream;
    stringStream << "name_" << i;
    std::string name_string = stringStream.str();
    stringStream << "address_" << i;
    std::string address_string = stringStream.str();
    row.emplace_back(type::TransientValueFactory::GetBoolean(i % 2 == 0));
    row.emplace_back(type::TransientValueFactory::GetInteger(i));
    row.emplace_back(type::TransientValueFactory::GetVarChar(name_string));
    row.emplace_back(type::TransientValueFactory::GetVarChar(address_string));
    ptr->InsertRow(txn_, row);
  }

  // Build an index on the first column of the table
  IndexBuilder index_builder;
  catalog::index_oid_t index_oid_0(catalog_->GetNextOid());

  // Set up the IndexKeySchema
  std::vector<IndexKeyColumn> key_cols_0;

  {
    auto col = table->GetSqlTable()->GetSchema().GetColumn("name");
    auto col_oid = catalog::indexkeycol_oid_t(!col.GetOid());

    // FIXME(xueyuan): Seems the Schema::GetAttrSize for varlen attribute is the internal representation, thus
    // incompatible with return value of the BlockLayout::AttrSize
    key_cols_0.emplace_back(IndexKeyColumn(col_oid, col.GetType(), false, INT8_MAX & col.GetAttrSize()));
  }

  {
    auto col = table->GetSqlTable()->GetSchema().GetColumn("id");
    auto col_oid = catalog::indexkeycol_oid_t(!col.GetOid());
    key_cols_0.emplace_back(IndexKeyColumn(col_oid, col.GetType(), false));
  }

  {
    auto col = table->GetSqlTable()->GetSchema().GetColumn("address");
    auto col_oid = catalog::indexkeycol_oid_t(!col.GetOid());

    // FIXME(xueyuan): Seems the Schema::GetAttrSize for varlen attribute is the internal representation, thus
    // incompatible with return value of the BlockLayout::AttrSize
    key_cols_0.emplace_back(IndexKeyColumn(col_oid, col.GetType(), false, INT8_MAX & col.GetAttrSize()));
  }

  {
    auto col = table->GetSqlTable()->GetSchema().GetColumn("sex");
    auto col_oid = catalog::indexkeycol_oid_t(!col.GetOid());
    key_cols_0.emplace_back(IndexKeyColumn(col_oid, col.GetType(), false));
  }

  auto index_key_schema = IndexKeySchema(key_cols_0);

  // Build the index
  index_builder.SetOid(index_oid_0);
  index_builder.SetConstraintType(ConstraintType::DEFAULT);
  index_builder.SetKeySchema(index_key_schema);
  auto index_0 = index_builder.Build();

  // Populate the index
  IndexPopulator::PopulateIndex(txn_, *table->GetSqlTable(), IndexKeySchema(key_cols_0), *index_0);

  // Create the projected row for index
  const IndexMetadata &metadata_0 = index_0->GetIndexMetadata();
  const auto &pr_initializer = metadata_0.GetProjectedRowInitializer();
  auto *key_buf_index = common::AllocationUtil::AllocateAligned(pr_initializer.ProjectedRowSize());
  ProjectedRow *index_key = pr_initializer.InitializeRow(key_buf_index);

  // Create the projected row for the sql table
  std::vector<catalog::col_oid_t> col_oids;
  col_oids.reserve(index_key_schema.size());
  for (const auto &it : index_key_schema) {
    col_oids.emplace_back(catalog::col_oid_t(!it.GetOid()));
  }
  auto init_and_map = table->GetSqlTable()->InitializerForProjectedRow(col_oids);
  auto *key_buf = common::AllocationUtil::AllocateAligned(init_and_map.first.ProjectedRowSize());
  ProjectedRow *key = init_and_map.first.InitializeRow(key_buf);

  // Record the col_id of each column
  std::vector<col_id_t> sql_table_cols;
  sql_table_cols.reserve(key->NumColumns());
  for (uint16_t i = 0; i < key->NumColumns(); ++i) {
    sql_table_cols.emplace_back(key->ColumnIds()[i]);
  }
  for (const auto &it : *table->GetSqlTable()) {
    if (table->GetSqlTable()->Select(txn_, it, key)) {
      for (uint16_t i = 0; i < key->NumColumns(); ++i) {
        key->ColumnIds()[i] = index_key->ColumnIds()[i];
      }
      std::vector<TupleSlot> tup_slots;
      index_0->ScanKey(*key, &tup_slots);
      bool flag = false;
      for (const auto &tup_slot : tup_slots) {
        if (it == tup_slot) {
          flag = true;
        }
      }
      EXPECT_EQ(flag, true);
      for (uint16_t i = 0; i < key->NumColumns(); ++i) {
        key->ColumnIds()[i] = sql_table_cols[i];
      }
    }
  }
  delete[] key_buf_index;
  delete[] key_buf;
  delete index_0;
  delete table;
}
}  // namespace terrier::storage::index
