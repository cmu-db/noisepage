#include "test_util/tpcc/builder.h"

namespace noisepage::tpcc {

Database *Builder::Build(const storage::index::IndexType index_type) {
  // create the database in the catalog
  auto *txn = txn_manager_->BeginTransaction();
  const catalog::db_oid_t db_oid = catalog_->CreateDatabase(common::ManagedPointer(txn), "tpcc", true);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // generate all of the table schemas
  auto item_schema = Schemas::BuildItemTableSchema();
  auto warehouse_schema = Schemas::BuildWarehouseTableSchema();
  auto stock_schema = Schemas::BuildStockTableSchema();
  auto district_schema = Schemas::BuildDistrictTableSchema();
  auto customer_schema = Schemas::BuildCustomerTableSchema();
  auto history_schema = Schemas::BuildHistoryTableSchema();
  auto new_order_schema = Schemas::BuildNewOrderTableSchema();
  auto order_schema = Schemas::BuildOrderTableSchema();
  auto order_line_schema = Schemas::BuildOrderLineTableSchema();

  // create the tables in the catalog
  txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  const catalog::table_oid_t item_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "ITEM", item_schema);
  const catalog::table_oid_t warehouse_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "WAREHOUSE", warehouse_schema);
  const catalog::table_oid_t stock_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "STOCK", stock_schema);
  const catalog::table_oid_t district_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "DISTRICT", district_schema);
  const catalog::table_oid_t customer_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "CUSTOMER", customer_schema);
  const catalog::table_oid_t history_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "HISTORY", history_schema);
  const catalog::table_oid_t new_order_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "NEW ORDER", new_order_schema);
  const catalog::table_oid_t order_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "ORDER", order_schema);
  const catalog::table_oid_t order_line_table_oid =
      accessor->CreateTable(accessor->GetDefaultNamespace(), "ORDER LINE", order_line_schema);
  NOISEPAGE_ASSERT(item_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(warehouse_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(stock_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(district_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(customer_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(history_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(new_order_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(order_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");
  NOISEPAGE_ASSERT(order_line_table_oid != catalog::INVALID_TABLE_OID, "Failed to create table.");

  // get the schemas from the catalog

  item_schema = accessor->GetSchema(item_table_oid);
  warehouse_schema = accessor->GetSchema(warehouse_table_oid);
  stock_schema = accessor->GetSchema(stock_table_oid);
  district_schema = accessor->GetSchema(district_table_oid);
  customer_schema = accessor->GetSchema(customer_table_oid);
  history_schema = accessor->GetSchema(history_table_oid);
  new_order_schema = accessor->GetSchema(new_order_table_oid);
  order_schema = accessor->GetSchema(order_table_oid);
  order_line_schema = accessor->GetSchema(order_line_table_oid);

  // instantiate and set the table pointers in the catalog

  auto result UNUSED_ATTRIBUTE = accessor->SetTablePointer(item_table_oid, new storage::SqlTable(store_, item_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(warehouse_table_oid, new storage::SqlTable(store_, warehouse_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(stock_table_oid, new storage::SqlTable(store_, stock_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(district_table_oid, new storage::SqlTable(store_, district_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(customer_table_oid, new storage::SqlTable(store_, customer_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(history_table_oid, new storage::SqlTable(store_, history_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(new_order_table_oid, new storage::SqlTable(store_, new_order_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(order_table_oid, new storage::SqlTable(store_, order_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");
  result = accessor->SetTablePointer(order_line_table_oid, new storage::SqlTable(store_, order_line_schema));
  NOISEPAGE_ASSERT(result, "Failed to set table pointer.");

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // get the table pointers back from the catalog
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  const auto item_table = accessor->GetTable(item_table_oid);
  const auto warehouse_table = accessor->GetTable(warehouse_table_oid);
  const auto stock_table = accessor->GetTable(stock_table_oid);
  const auto district_table = accessor->GetTable(district_table_oid);
  const auto customer_table = accessor->GetTable(customer_table_oid);
  const auto history_table = accessor->GetTable(history_table_oid);
  const auto new_order_table = accessor->GetTable(new_order_table_oid);
  const auto order_table = accessor->GetTable(order_table_oid);
  const auto order_line_table = accessor->GetTable(order_line_table_oid);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // The following assertions verify that all of the primary key and their respective foreign key dependencies have
  // the same types across schemas.

  NOISEPAGE_ASSERT(
      warehouse_schema.GetColumn(0).Name() == "w_id" && district_schema.GetColumn(1).Name() == "d_w_id" &&
          customer_schema.GetColumn(2).Name() == "c_w_id" && history_schema.GetColumn(2).Name() == "h_c_w_id" &&
          history_schema.GetColumn(4).Name() == "h_w_id" && new_order_schema.GetColumn(2).Name() == "no_w_id" &&
          order_schema.GetColumn(2).Name() == "o_w_id" && order_line_schema.GetColumn(2).Name() == "ol_w_id" &&
          stock_schema.GetColumn(1).Name() == "s_w_id" &&
          (warehouse_schema.GetColumn(0).Type() == district_schema.GetColumn(1).Type() &&
           warehouse_schema.GetColumn(0).Type() == customer_schema.GetColumn(2).Type() &&
           warehouse_schema.GetColumn(0).Type() == history_schema.GetColumn(2).Type() &&
           warehouse_schema.GetColumn(0).Type() == history_schema.GetColumn(4).Type() &&
           warehouse_schema.GetColumn(0).Type() == new_order_schema.GetColumn(2).Type() &&
           warehouse_schema.GetColumn(0).Type() == order_schema.GetColumn(2).Type() &&
           warehouse_schema.GetColumn(0).Type() == order_line_schema.GetColumn(2).Type() &&
           warehouse_schema.GetColumn(0).Type() == stock_schema.GetColumn(1).Type()),
      "Invalid schema configurations for W_ID.");

  NOISEPAGE_ASSERT(
      district_schema.GetColumn(0).Name() == "d_id" && customer_schema.GetColumn(1).Name() == "c_d_id" &&
          history_schema.GetColumn(1).Name() == "h_c_d_id" && history_schema.GetColumn(3).Name() == "h_d_id" &&
          new_order_schema.GetColumn(1).Name() == "no_d_id" && order_schema.GetColumn(1).Name() == "o_d_id" &&
          order_line_schema.GetColumn(1).Name() == "ol_d_id" &&
          (district_schema.GetColumn(0).Type() == customer_schema.GetColumn(1).Type() &&
           district_schema.GetColumn(0).Type() == history_schema.GetColumn(1).Type() &&
           district_schema.GetColumn(0).Type() == history_schema.GetColumn(3).Type() &&
           district_schema.GetColumn(0).Type() == new_order_schema.GetColumn(1).Type() &&
           district_schema.GetColumn(0).Type() == order_schema.GetColumn(1).Type() &&
           district_schema.GetColumn(0).Type() == order_line_schema.GetColumn(1).Type()),
      "Invalid schema configurations for D_ID.");

  NOISEPAGE_ASSERT(customer_schema.GetColumn(0).Name() == "c_id" && history_schema.GetColumn(0).Name() == "h_c_id" &&
                       order_schema.GetColumn(3).Name() == "o_c_id" &&
                       (customer_schema.GetColumn(0).Type() == history_schema.GetColumn(0).Type() &&
                        customer_schema.GetColumn(0).Type() == order_schema.GetColumn(3).Type()),
                   "Invalid schema configurations for C_ID.");

  NOISEPAGE_ASSERT(new_order_schema.GetColumn(0).Name() == "no_o_id" && order_schema.GetColumn(0).Name() == "o_id" &&
                       order_line_schema.GetColumn(0).Name() == "ol_o_id" &&
                       (new_order_schema.GetColumn(0).Type() == order_schema.GetColumn(0).Type() &&
                        new_order_schema.GetColumn(0).Type() == order_line_schema.GetColumn(0).Type()),
                   "Invalid schema configurations for O_ID.");

  NOISEPAGE_ASSERT(order_line_schema.GetColumn(4).Name() == "ol_i_id" && item_schema.GetColumn(0).Name() == "i_id" &&
                       stock_schema.GetColumn(0).Name() == "s_i_id" &&
                       (order_line_schema.GetColumn(4).Type() == item_schema.GetColumn(0).Type() &&
                        order_line_schema.GetColumn(4).Type() == stock_schema.GetColumn(0).Type()),
                   "Invalid schema configurations for I_ID.");

  // generate all of the index schemas
  auto warehouse_primary_index_schema =
      Schemas::BuildWarehousePrimaryIndexSchema(warehouse_schema, index_type, db_oid, warehouse_table_oid);
  auto district_primary_index_schema =
      Schemas::BuildDistrictPrimaryIndexSchema(district_schema, index_type, db_oid, district_table_oid);
  auto customer_primary_index_schema =
      Schemas::BuildCustomerPrimaryIndexSchema(customer_schema, index_type, db_oid, customer_table_oid);
  auto customer_secondary_index_schema =
      Schemas::BuildCustomerSecondaryIndexSchema(customer_schema, index_type, db_oid, customer_table_oid);
  auto new_order_primary_index_schema = Schemas::BuildNewOrderPrimaryIndexSchema(
      new_order_schema, storage::index::IndexType::BWTREE, db_oid, new_order_table_oid);
  auto order_primary_index_schema =
      Schemas::BuildOrderPrimaryIndexSchema(order_schema, index_type, db_oid, order_table_oid);
  auto order_secondary_index_schema =
      Schemas::BuildOrderSecondaryIndexSchema(order_schema, storage::index::IndexType::BWTREE, db_oid, order_table_oid);
  auto order_line_primary_index_schema = Schemas::BuildOrderLinePrimaryIndexSchema(
      order_line_schema, storage::index::IndexType::BWTREE, db_oid, order_line_table_oid);
  auto item_primary_index_schema =
      Schemas::BuildItemPrimaryIndexSchema(item_schema, index_type, db_oid, item_table_oid);
  auto stock_primary_index_schema =
      Schemas::BuildStockPrimaryIndexSchema(stock_schema, index_type, db_oid, stock_table_oid);

  // create the indexes in the catalog
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  const catalog::index_oid_t warehouse_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), warehouse_table_oid, "WAREHOUSE PRIMARY", warehouse_primary_index_schema);
  const catalog::index_oid_t district_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), district_table_oid, "DISTRICT PRIMARY", district_primary_index_schema);
  const catalog::index_oid_t customer_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), customer_table_oid, "CUSTOMER PRIMARY", customer_primary_index_schema);
  const catalog::index_oid_t customer_secondary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), customer_table_oid, "WAREHOUSE SECONDARY", customer_secondary_index_schema);
  const catalog::index_oid_t new_order_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), new_order_table_oid, "NEW ORDER PRIMARY", new_order_primary_index_schema);
  const catalog::index_oid_t order_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), order_table_oid, "ORDER PRIMARY", order_primary_index_schema);
  const catalog::index_oid_t order_secondary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), order_table_oid, "ORDER SECONDARY", order_secondary_index_schema);
  const catalog::index_oid_t order_line_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), order_line_table_oid, "ORDER LINE PRIMARY", order_line_primary_index_schema);
  const catalog::index_oid_t item_primary_index_oid =
      accessor->CreateIndex(accessor->GetDefaultNamespace(), item_table_oid, "ITEM PRIMARY", item_primary_index_schema);
  const catalog::index_oid_t stock_primary_index_oid = accessor->CreateIndex(
      accessor->GetDefaultNamespace(), stock_table_oid, "STOCK PRIMARY", stock_primary_index_schema);
  NOISEPAGE_ASSERT(warehouse_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(district_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(customer_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(customer_secondary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(new_order_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(order_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(order_secondary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(order_line_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(item_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");
  NOISEPAGE_ASSERT(stock_primary_index_oid != catalog::INVALID_INDEX_OID, "Failed to create index.");

  // get the schemas from the catalog

  warehouse_primary_index_schema = accessor->GetIndexSchema(warehouse_primary_index_oid);
  district_primary_index_schema = accessor->GetIndexSchema(district_primary_index_oid);
  customer_primary_index_schema = accessor->GetIndexSchema(customer_primary_index_oid);
  customer_secondary_index_schema = accessor->GetIndexSchema(customer_secondary_index_oid);
  new_order_primary_index_schema = accessor->GetIndexSchema(new_order_primary_index_oid);
  order_primary_index_schema = accessor->GetIndexSchema(order_primary_index_oid);
  order_secondary_index_schema = accessor->GetIndexSchema(order_secondary_index_oid);
  order_line_primary_index_schema = accessor->GetIndexSchema(order_line_primary_index_oid);
  item_primary_index_schema = accessor->GetIndexSchema(item_primary_index_oid);
  stock_primary_index_schema = accessor->GetIndexSchema(stock_primary_index_oid);

  // instantiate and set the index pointers in the catalog

  result = accessor->SetIndexPointer(warehouse_primary_index_oid, BuildIndex(warehouse_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(district_primary_index_oid, BuildIndex(district_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(customer_primary_index_oid, BuildIndex(customer_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(customer_secondary_index_oid, BuildIndex(customer_secondary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(new_order_primary_index_oid, BuildIndex(new_order_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(order_primary_index_oid, BuildIndex(order_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(order_secondary_index_oid, BuildIndex(order_secondary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(order_line_primary_index_oid, BuildIndex(order_line_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(item_primary_index_oid, BuildIndex(item_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");
  result = accessor->SetIndexPointer(stock_primary_index_oid, BuildIndex(stock_primary_index_schema));
  NOISEPAGE_ASSERT(result, "Failed to set index pointer.");

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // get the table pointers back from the catalog
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  const auto warehouse_index = accessor->GetIndex(warehouse_primary_index_oid);
  const auto district_index = accessor->GetIndex(district_primary_index_oid);
  const auto customer_index = accessor->GetIndex(customer_primary_index_oid);
  const auto customer_secondary_index = accessor->GetIndex(customer_secondary_index_oid);
  const auto new_order_index = accessor->GetIndex(new_order_primary_index_oid);
  const auto order_index = accessor->GetIndex(order_primary_index_oid);
  const auto order_secondary_index = accessor->GetIndex(order_secondary_index_oid);
  const auto order_line_index = accessor->GetIndex(order_line_primary_index_oid);
  const auto item_index = accessor->GetIndex(item_primary_index_oid);
  const auto stock_index = accessor->GetIndex(stock_primary_index_oid);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Verify that we got the indexes and key types that we expect out of the builder
  if (index_type == storage::index::IndexType::HASHMAP) {
    NOISEPAGE_ASSERT(warehouse_index->Type() == storage::index::IndexType::HASHMAP,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(district_index->Type() == storage::index::IndexType::HASHMAP, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(customer_index->Type() == storage::index::IndexType::HASHMAP, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(customer_secondary_index->Type() == storage::index::IndexType::HASHMAP,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(new_order_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_index->Type() == storage::index::IndexType::HASHMAP, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_secondary_index->Type() == storage::index::IndexType::BWTREE,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_line_index->Type() == storage::index::IndexType::BWTREE,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(item_index->Type() == storage::index::IndexType::HASHMAP, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(stock_index->Type() == storage::index::IndexType::HASHMAP, "Constructed the wrong index type.");

    NOISEPAGE_ASSERT(warehouse_index->KeyKind() == storage::index::IndexKeyKind::HASHKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(district_index->KeyKind() == storage::index::IndexKeyKind::HASHKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(customer_index->KeyKind() == storage::index::IndexKeyKind::HASHKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(customer_secondary_index->KeyKind() == storage::index::IndexKeyKind::GENERICKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(new_order_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(order_index->KeyKind() == storage::index::IndexKeyKind::HASHKEY,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_secondary_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(order_line_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(item_index->KeyKind() == storage::index::IndexKeyKind::HASHKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(stock_index->KeyKind() == storage::index::IndexKeyKind::HASHKEY,
                     "Constructed the wrong index key type.");
  } else {
    NOISEPAGE_ASSERT(index_type == storage::index::IndexType::BWTREE,
                     "This branch expects the BwTree. Did you add another IndexType to the system?");
    NOISEPAGE_ASSERT(warehouse_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(district_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(customer_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(customer_secondary_index->Type() == storage::index::IndexType::BWTREE,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(new_order_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_secondary_index->Type() == storage::index::IndexType::BWTREE,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_line_index->Type() == storage::index::IndexType::BWTREE,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(item_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(stock_index->Type() == storage::index::IndexType::BWTREE, "Constructed the wrong index type.");

    NOISEPAGE_ASSERT(warehouse_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(district_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(customer_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(customer_secondary_index->KeyKind() == storage::index::IndexKeyKind::GENERICKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(new_order_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(order_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index type.");
    NOISEPAGE_ASSERT(order_secondary_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(order_line_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(item_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
    NOISEPAGE_ASSERT(stock_index->KeyKind() == storage::index::IndexKeyKind::COMPACTINTSKEY,
                     "Constructed the wrong index key type.");
  }

  return new Database(item_schema, warehouse_schema, stock_schema, district_schema, customer_schema, history_schema,
                      new_order_schema, order_schema, order_line_schema,

                      item_table, warehouse_table, stock_table, district_table, customer_table, history_table,
                      new_order_table, order_table, order_line_table,

                      item_primary_index_schema, warehouse_primary_index_schema, stock_primary_index_schema,
                      district_primary_index_schema, customer_primary_index_schema, customer_secondary_index_schema,
                      new_order_primary_index_schema, order_primary_index_schema, order_secondary_index_schema,
                      order_line_primary_index_schema,

                      item_index, warehouse_index, stock_index, district_index, customer_index,
                      customer_secondary_index, new_order_index, order_index, order_secondary_index, order_line_index,

                      db_oid,

                      item_table_oid, warehouse_table_oid, stock_table_oid, district_table_oid, customer_table_oid,
                      history_table_oid, new_order_table_oid, order_table_oid, order_line_table_oid,

                      warehouse_primary_index_oid, district_primary_index_oid, customer_primary_index_oid,
                      customer_secondary_index_oid, new_order_primary_index_oid, order_primary_index_oid,
                      order_secondary_index_oid, order_line_primary_index_oid, item_primary_index_oid,
                      stock_primary_index_oid);
}

}  // namespace noisepage::tpcc
