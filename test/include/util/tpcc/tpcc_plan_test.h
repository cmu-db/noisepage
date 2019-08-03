#include <string>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "storage/garbage_collector.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/tpcc/builder.h"

namespace terrier {

struct TpccPlanTest : public TerrierTest {
  catalog::table_oid_t CreateTable(catalog::CatalogAccessor *accessor, std::string tbl_name, catalog::Schema schema) {
    auto tbl_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), tbl_name, schema);
    EXPECT_NE(tbl_oid, catalog::INVALID_TABLE_OID);

    tbl_oid = accessor->GetTableOid(accessor->GetDefaultNamespace(), tbl_name);
    EXPECT_NE(tbl_oid, catalog::INVALID_TABLE_OID);

    auto table = new storage::SqlTable(&block_store_, schema);
    EXPECT_TRUE(accessor->SetTablePointer(tbl_oid, table));
    return tbl_oid;
  }

  void CreateIndex(catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid, std::string idx_name,
                   catalog::IndexSchema schema, bool is_primary) {
    auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), tbl_oid, idx_name, schema);
    EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);

    idx_oid = accessor->GetIndexOid(idx_name);
    EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);

    storage::index::IndexBuilder index_builder;
    index_builder.SetOid(idx_oid).SetKeySchema(schema);
    if (is_primary)
      index_builder.SetConstraintType(storage::index::ConstraintType::UNIQUE);
    else
      index_builder.SetConstraintType(storage::index::ConstraintType::DEFAULT);

    auto index = index_builder.Build();
    EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  }

  void SetUpTpccSchemas() {
    // Registers everything we need with the catalog
    // We don't actually need SqlTables/Indexes/Data
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(txn, db_);

    // Following logic taken from util/tpcc/builder.cpp
    uint32_t oid_counter = 0;
    auto item_schema = tpcc::Schemas::BuildItemTableSchema(&oid_counter);
    auto warehouse_schema = tpcc::Schemas::BuildWarehouseTableSchema(&oid_counter);
    auto stock_schema = tpcc::Schemas::BuildStockTableSchema(&oid_counter);
    auto district_schema = tpcc::Schemas::BuildDistrictTableSchema(&oid_counter);
    auto customer_schema = tpcc::Schemas::BuildCustomerTableSchema(&oid_counter);
    auto history_schema = tpcc::Schemas::BuildHistoryTableSchema(&oid_counter);
    auto new_order_schema = tpcc::Schemas::BuildNewOrderTableSchema(&oid_counter);
    auto order_schema = tpcc::Schemas::BuildOrderTableSchema(&oid_counter);
    auto order_line_schema = tpcc::Schemas::BuildOrderLineTableSchema(&oid_counter);

    tbl_item_ = CreateTable(accessor, "ITEM", item_schema);
    tbl_warehouse_ = CreateTable(accessor, "WAREHOUSE", warehouse_schema);
    tbl_stock_ = CreateTable(accessor, "STOCK", stock_schema);
    tbl_district_ = CreateTable(accessor, "DISTRICT", district_schema);
    tbl_customer_ = CreateTable(accessor, "CUSTOMER", customer_schema);
    tbl_history_ = CreateTable(accessor, "HISTORY", history_schema);
    tbl_new_order_ = CreateTable(accessor, "NEW-ORDER", new_order_schema);
    tbl_order_ = CreateTable(accessor, "ORDER", order_schema);
    tbl_order_line_ = CreateTable(accessor, "ORDER-LINE", order_line_schema);

    CreateIndex(accessor, tbl_warehouse_, "PK_WAREHOUSE",
                tpcc::Schemas::BuildWarehousePrimaryIndexSchema(warehouse_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_district_, "PK_DISTRICT",
                tpcc::Schemas::BuildDistrictPrimaryIndexSchema(district_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_customer_, "PK_CUSTOMER",
                tpcc::Schemas::BuildCustomerPrimaryIndexSchema(customer_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_customer_, "SK_CUSTOMER",
                tpcc::Schemas::BuildCustomerSecondaryIndexSchema(customer_schema, &oid_counter), false);
    CreateIndex(accessor, tbl_new_order_, "PK_NEW-ORDER",
                tpcc::Schemas::BuildNewOrderPrimaryIndexSchema(new_order_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_order_, "PK_ORDER",
                tpcc::Schemas::BuildOrderPrimaryIndexSchema(order_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_order_, "SK_ORDER",
                tpcc::Schemas::BuildOrderSecondaryIndexSchema(order_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_order_line_, "PK_ORDER-LINE",
                tpcc::Schemas::BuildOrderLinePrimaryIndexSchema(order_line_schema, &oid_counter), true);
    CreateIndex(accessor, tbl_item_, "PK_ITEM", tpcc::Schemas::BuildItemPrimaryIndexSchema(item_schema, &oid_counter),
                true);
    CreateIndex(accessor, tbl_stock_, "PK_STOCK",
                tpcc::Schemas::BuildStockPrimaryIndexSchema(stock_schema, &oid_counter), true);

    delete accessor;
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  void SetUp() override {
    TerrierTest::SetUp();

    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
    gc_ = new storage::GarbageCollector(txn_manager_, nullptr);

    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);

    auto txn = txn_manager_->BeginTransaction();
    db_ = catalog_->CreateDatabase(txn, "terrier", true);
    EXPECT_NE(db_, catalog::INVALID_DATABASE_OID);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    SetUpTpccSchemas();

    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

  void TearDown() override {
    // Cleanup the catalog
    catalog_->TearDown();

    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    delete catalog_;
    delete gc_;
    delete txn_manager_;

    TerrierTest::TearDown();
  }

  catalog::Catalog *catalog_;
  transaction::TransactionManager *txn_manager_;

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};
  storage::GarbageCollector *gc_;

  catalog::db_oid_t db_;

  // OIDs
  catalog::table_oid_t tbl_item_;
  catalog::table_oid_t tbl_warehouse_;
  catalog::table_oid_t tbl_stock_;
  catalog::table_oid_t tbl_district_;
  catalog::table_oid_t tbl_customer_;
  catalog::table_oid_t tbl_history_;
  catalog::table_oid_t tbl_new_order_;
  catalog::table_oid_t tbl_order_;
  catalog::table_oid_t tbl_order_line_;
};

}  // namespace terrier
