#include "util/tpcc/builder.h"

namespace terrier::tpcc {

Database *Builder::Build() {
  // generate all of the table schemas
  auto item_schema = Schemas::BuildItemTableSchema(&oid_counter_);
  auto warehouse_schema = Schemas::BuildWarehouseTableSchema(&oid_counter_);
  auto stock_schema = Schemas::BuildStockTableSchema(&oid_counter_);
  auto district_schema = Schemas::BuildDistrictTableSchema(&oid_counter_);
  auto customer_schema = Schemas::BuildCustomerTableSchema(&oid_counter_);
  auto history_schema = Schemas::BuildHistoryTableSchema(&oid_counter_);
  auto new_order_schema = Schemas::BuildNewOrderTableSchema(&oid_counter_);
  auto order_schema = Schemas::BuildOrderTableSchema(&oid_counter_);
  auto order_line_schema = Schemas::BuildOrderLineTableSchema(&oid_counter_);

  // instantiate all of the tables
  auto *const item_table = new storage::SqlTable(store_, item_schema);
  auto *const warehouse_table = new storage::SqlTable(store_, warehouse_schema);
  auto *const stock_table = new storage::SqlTable(store_, stock_schema);
  auto *const district_table = new storage::SqlTable(store_, district_schema);
  auto *const customer_table = new storage::SqlTable(store_, customer_schema);
  auto *const history_table = new storage::SqlTable(store_, history_schema);
  auto *const new_order_table = new storage::SqlTable(store_, new_order_schema);
  auto *const order_table = new storage::SqlTable(store_, order_schema);
  auto *const order_line_table = new storage::SqlTable(store_, order_line_schema);

  // The following assertions verify that all of the primary key and their respective foreign key dependencies have
  // the same types across schemas.

  TERRIER_ASSERT(
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

  TERRIER_ASSERT(
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

  TERRIER_ASSERT(customer_schema.GetColumn(0).Name() == "c_id" && history_schema.GetColumn(0).Name() == "h_c_id" &&
                     order_schema.GetColumn(3).Name() == "o_c_id" &&
                     (customer_schema.GetColumn(0).Type() == history_schema.GetColumn(0).Type() &&
                      customer_schema.GetColumn(0).Type() == order_schema.GetColumn(3).Type()),
                 "Invalid schema configurations for C_ID.");

  TERRIER_ASSERT(new_order_schema.GetColumn(0).Name() == "no_o_id" && order_schema.GetColumn(0).Name() == "o_id" &&
                     order_line_schema.GetColumn(0).Name() == "ol_o_id" &&
                     (new_order_schema.GetColumn(0).Type() == order_schema.GetColumn(0).Type() &&
                      new_order_schema.GetColumn(0).Type() == order_line_schema.GetColumn(0).Type()),
                 "Invalid schema configurations for O_ID.");

  TERRIER_ASSERT(order_line_schema.GetColumn(4).Name() == "ol_i_id" && item_schema.GetColumn(0).Name() == "i_id" &&
                     stock_schema.GetColumn(0).Name() == "s_i_id" &&
                     (order_line_schema.GetColumn(4).Type() == item_schema.GetColumn(0).Type() &&
                      order_line_schema.GetColumn(4).Type() == stock_schema.GetColumn(0).Type()),
                 "Invalid schema configurations for I_ID.");

  // generate all of the index schemas
  auto warehouse_primary_index_schema = Schemas::BuildWarehousePrimaryIndexSchema(warehouse_schema, &oid_counter_);
  auto district_primary_index_schema = Schemas::BuildDistrictPrimaryIndexSchema(district_schema, &oid_counter_);
  auto customer_primary_index_schema = Schemas::BuildCustomerPrimaryIndexSchema(customer_schema, &oid_counter_);
  auto customer_secondary_index_schema = Schemas::BuildCustomerSecondaryIndexSchema(customer_schema, &oid_counter_);
  auto new_order_primary_index_schema = Schemas::BuildNewOrderPrimaryIndexSchema(new_order_schema, &oid_counter_);
  auto order_primary_index_schema = Schemas::BuildOrderPrimaryIndexSchema(order_schema, &oid_counter_);
  auto order_secondary_index_schema = Schemas::BuildOrderSecondaryIndexSchema(order_schema, &oid_counter_);
  auto order_line_primary_index_schema = Schemas::BuildOrderLinePrimaryIndexSchema(order_line_schema, &oid_counter_);
  auto item_primary_index_schema = Schemas::BuildItemPrimaryIndexSchema(item_schema, &oid_counter_);
  auto stock_primary_index_schema = Schemas::BuildStockPrimaryIndexSchema(stock_schema, &oid_counter_);

  // instantiate all of the indexes
  auto *const warehouse_index = BuildPrimaryIndex(warehouse_primary_index_schema);
  auto *const district_index = BuildPrimaryIndex(district_primary_index_schema);
  auto *const customer_index = BuildPrimaryIndex(customer_primary_index_schema);
  auto *const customer_secondary_index = BuildSecondaryIndex(customer_secondary_index_schema);
  auto *const new_order_index = BuildPrimaryIndex(new_order_primary_index_schema);
  auto *const order_index = BuildPrimaryIndex(order_primary_index_schema);
  auto *const order_secondary_index = BuildPrimaryIndex(order_secondary_index_schema);
  auto *const order_line_index = BuildPrimaryIndex(order_line_primary_index_schema);
  auto *const item_index = BuildPrimaryIndex(item_primary_index_schema);
  auto *const stock_index = BuildPrimaryIndex(stock_primary_index_schema);

  const catalog::db_oid_t db_oid(++oid_counter_);

  const catalog::table_oid_t item_table_oid(++oid_counter_);
  const catalog::table_oid_t warehouse_table_oid(++oid_counter_);
  const catalog::table_oid_t stock_table_oid(++oid_counter_);
  const catalog::table_oid_t district_table_oid(++oid_counter_);
  const catalog::table_oid_t customer_table_oid(++oid_counter_);
  const catalog::table_oid_t history_table_oid(++oid_counter_);
  const catalog::table_oid_t new_order_table_oid(++oid_counter_);
  const catalog::table_oid_t order_table_oid(++oid_counter_);
  const catalog::table_oid_t order_line_table_oid(++oid_counter_);

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
                      history_table_oid, new_order_table_oid, order_table_oid, order_line_table_oid);
}

}  // namespace terrier::tpcc
