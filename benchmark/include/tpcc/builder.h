#pragma once

#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "tpcc/database.h"
#include "tpcc/schemas.h"

namespace terrier::tpcc {

/**
 * Builds all of the tables and indexes for TPCC, and returns them in a Database object
 */
class Builder {
 public:
  explicit Builder(storage::BlockStore *const store) : store_(store), oid_counter_(0) {}
  Database *Build() {
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
    auto *const item_table = new storage::SqlTable(
        store_, item_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const warehouse_table = new storage::SqlTable(
        store_, warehouse_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const stock_table = new storage::SqlTable(
        store_, stock_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const district_table = new storage::SqlTable(
        store_, district_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const customer_table = new storage::SqlTable(
        store_, customer_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const history_table = new storage::SqlTable(
        store_, history_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const new_order_table = new storage::SqlTable(
        store_, new_order_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const order_table = new storage::SqlTable(
        store_, order_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));
    auto *const order_line_table = new storage::SqlTable(
        store_, order_line_schema, static_cast<catalog::table_oid_t>(static_cast<uint32_t>(++oid_counter_)));

    // The following assertions verify that all of the primary key and their respective foreign key dependencies have
    // the same types across schemas.

    TERRIER_ASSERT(
        warehouse_schema.GetColumn(0).GetName() == "W_ID" && district_schema.GetColumn(1).GetName() == "D_W_ID" &&
            customer_schema.GetColumn(2).GetName() == "C_W_ID" && history_schema.GetColumn(2).GetName() == "H_C_W_ID" &&
            history_schema.GetColumn(4).GetName() == "H_W_ID" && new_order_schema.GetColumn(2).GetName() == "NO_W_ID" &&
            order_schema.GetColumn(2).GetName() == "O_W_ID" && order_line_schema.GetColumn(2).GetName() == "OL_W_ID" &&
            stock_schema.GetColumn(1).GetName() == "S_W_ID" &&
            (warehouse_schema.GetColumn(0).GetType() == district_schema.GetColumn(1).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == customer_schema.GetColumn(2).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == history_schema.GetColumn(2).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == history_schema.GetColumn(4).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == new_order_schema.GetColumn(2).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == order_schema.GetColumn(2).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == order_line_schema.GetColumn(2).GetType() &&
             warehouse_schema.GetColumn(0).GetType() == stock_schema.GetColumn(1).GetType()),
        "Invalid schema configurations for W_ID.");

    TERRIER_ASSERT(
        district_schema.GetColumn(0).GetName() == "D_ID" && customer_schema.GetColumn(1).GetName() == "C_D_ID" &&
            history_schema.GetColumn(1).GetName() == "H_C_D_ID" && history_schema.GetColumn(3).GetName() == "H_D_ID" &&
            new_order_schema.GetColumn(1).GetName() == "NO_D_ID" && order_schema.GetColumn(1).GetName() == "O_D_ID" &&
            order_line_schema.GetColumn(1).GetName() == "OL_D_ID" &&
            (district_schema.GetColumn(0).GetType() == customer_schema.GetColumn(1).GetType() &&
             district_schema.GetColumn(0).GetType() == history_schema.GetColumn(1).GetType() &&
             district_schema.GetColumn(0).GetType() == history_schema.GetColumn(3).GetType() &&
             district_schema.GetColumn(0).GetType() == new_order_schema.GetColumn(1).GetType() &&
             district_schema.GetColumn(0).GetType() == order_schema.GetColumn(1).GetType() &&
             district_schema.GetColumn(0).GetType() == order_line_schema.GetColumn(1).GetType()),
        "Invalid schema configurations for D_ID.");

    TERRIER_ASSERT(customer_schema.GetColumn(0).GetName() == "C_ID" &&
                       history_schema.GetColumn(0).GetName() == "H_C_ID" &&
                       order_schema.GetColumn(3).GetName() == "O_C_ID" &&
                       (customer_schema.GetColumn(0).GetType() == history_schema.GetColumn(0).GetType() &&
                        customer_schema.GetColumn(0).GetType() == order_schema.GetColumn(3).GetType()),
                   "Invalid schema configurations for C_ID.");

    TERRIER_ASSERT(new_order_schema.GetColumn(0).GetName() == "NO_O_ID" &&
                       order_schema.GetColumn(0).GetName() == "O_ID" &&
                       order_line_schema.GetColumn(0).GetName() == "OL_O_ID" &&
                       (new_order_schema.GetColumn(0).GetType() == order_schema.GetColumn(0).GetType() &&
                        new_order_schema.GetColumn(0).GetType() == order_line_schema.GetColumn(0).GetType()),
                   "Invalid schema configurations for O_ID.");

    TERRIER_ASSERT(order_line_schema.GetColumn(4).GetName() == "OL_I_ID" &&
                       item_schema.GetColumn(0).GetName() == "I_ID" &&
                       stock_schema.GetColumn(0).GetName() == "S_I_ID" &&
                       (order_line_schema.GetColumn(4).GetType() == item_schema.GetColumn(0).GetType() &&
                        order_line_schema.GetColumn(4).GetType() == stock_schema.GetColumn(0).GetType()),
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
    auto *const order_secondary_index = BuildSecondaryIndex(order_secondary_index_schema);
    auto *const order_line_index = BuildPrimaryIndex(order_line_primary_index_schema);
    auto *const item_index = BuildPrimaryIndex(item_primary_index_schema);
    auto *const stock_index = BuildPrimaryIndex(stock_primary_index_schema);

    return new Database(item_schema, warehouse_schema, stock_schema, district_schema, customer_schema, history_schema,
                        new_order_schema, order_schema, order_line_schema,

                        item_table, warehouse_table, stock_table, district_table, customer_table, history_table,
                        new_order_table, order_table, order_line_table,

                        item_primary_index_schema, warehouse_primary_index_schema, stock_primary_index_schema,
                        district_primary_index_schema, customer_primary_index_schema, customer_secondary_index_schema,
                        new_order_primary_index_schema, order_primary_index_schema, order_secondary_index_schema,
                        order_line_primary_index_schema,

                        item_index, warehouse_index, stock_index, district_index, customer_index,
                        customer_secondary_index, new_order_index, order_index, order_secondary_index,
                        order_line_index);
  }

 private:
  storage::index::Index *BuildPrimaryIndex(const storage::index::IndexKeySchema &key_schema) {
    storage::index::IndexBuilder index_builder;
    index_builder.SetOid(static_cast<catalog::index_oid_t>(static_cast<uint32_t>(++oid_counter_)))
        .SetKeySchema(key_schema)
        .SetConstraintType(storage::index::ConstraintType::UNIQUE);
    return index_builder.Build();
  }
  storage::index::Index *BuildSecondaryIndex(const storage::index::IndexKeySchema &key_schema) {
    storage::index::IndexBuilder index_builder;
    index_builder.SetOid(static_cast<catalog::index_oid_t>(static_cast<uint32_t>(++oid_counter_)))
        .SetKeySchema(key_schema)
        .SetConstraintType(storage::index::ConstraintType::DEFAULT);
    return index_builder.Build();
  }

  storage::BlockStore *const store_;
  uint32_t oid_counter_;
};
}  // namespace terrier::tpcc
