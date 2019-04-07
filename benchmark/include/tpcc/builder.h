#pragma once

#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/sql_table.h"
#include "tpcc/database.h"
#include "tpcc/loader.h"
#include "tpcc/schemas.h"

namespace terrier::tpcc {

template <class Random>
class Builder {
 public:
  Builder(transaction::TransactionManager *const txn_manager, storage::BlockStore *const store, Random *const generator)
      : txn_manager_(txn_manager), store_(store), generator_(generator), oid_counter_(0) {}
  Database *Build() {
    auto item_schema = Schemas::BuildItemTupleSchema(&oid_counter_);
    auto warehouse_schema = Schemas::BuildWarehouseTupleSchema(&oid_counter_);
    auto stock_schema = Schemas::BuildStockTupleSchema(&oid_counter_);
    auto district_schema = Schemas::BuildDistrictTupleSchema(&oid_counter_);
    auto customer_schema = Schemas::BuildCustomerTupleSchema(&oid_counter_);
    auto history_schema = Schemas::BuildHistoryTupleSchema(&oid_counter_);
    auto new_order_schema = Schemas::BuildNewOrderTupleSchema(&oid_counter_);
    auto order_schema = Schemas::BuildOrderTupleSchema(&oid_counter_);
    auto order_line_schema = Schemas::BuildOrderLineTupleSchema(&oid_counter_);
    auto *const item_table =
        new storage::SqlTable(store_, item_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const warehouse_table =
        new storage::SqlTable(store_, warehouse_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const stock_table =
        new storage::SqlTable(store_, stock_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const district_table =
        new storage::SqlTable(store_, district_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const customer_table =
        new storage::SqlTable(store_, customer_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const history_table =
        new storage::SqlTable(store_, history_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const new_order_table =
        new storage::SqlTable(store_, new_order_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const order_table =
        new storage::SqlTable(store_, order_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
    auto *const order_line_table =
        new storage::SqlTable(store_, order_line_schema, static_cast<catalog::table_oid_t>(++oid_counter_));

    Loader::PopulateTables(txn_manager_, generator_, item_schema, warehouse_schema, stock_schema, district_schema,
                           customer_schema, history_schema, new_order_schema, order_schema, order_line_schema,
                           item_table, warehouse_table, stock_table, district_table, customer_table, history_table,
                           new_order_table, order_table, order_line_table);

    return new Database(item_schema, warehouse_schema, stock_schema, district_schema, customer_schema, history_schema,
                        new_order_schema, order_schema, order_line_schema, item_table, warehouse_table, stock_table,
                        district_table, customer_table, history_table, new_order_table, order_table, order_line_table);
  }

 private:
  transaction::TransactionManager *const txn_manager_;
  storage::BlockStore *const store_;
  Random *const generator_;
  uint64_t oid_counter_;
};
}  // namespace terrier::tpcc
