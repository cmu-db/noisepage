#pragma once

#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/sql_table.h"
#include "tpcc/loader.h"
#include "tpcc/schemas.h"

// TODO(Matt): it seems many fields can by smaller than INTEGER

namespace terrier::tpcc {

class Database {
 public:
  template <class Random>
  class Builder {
   public:
    Builder(transaction::TransactionManager *const txn_manager, storage::BlockStore *const store,
            Random *const generator)
        : txn_manager_(txn_manager), store_(store), generator_(generator), oid_counter_(0) {}
    Database *Build() {
      auto item_schema = Schemas::BuildItemSchema(&oid_counter_);
      auto warehouse_schema = Schemas::BuildWarehouseSchema(&oid_counter_);
      auto stock_schema = Schemas::BuildStockSchema(&oid_counter_);
      auto district_schema = Schemas::BuildDistrictSchema(&oid_counter_);
      auto customer_schema = Schemas::BuildCustomerSchema(&oid_counter_);
      auto history_schema = Schemas::BuildHistorySchema(&oid_counter_);
      auto new_order_schema = Schemas::BuildNewOrderSchema(&oid_counter_);
      auto order_schema = Schemas::BuildOrderSchema(&oid_counter_);
      auto order_line_schema = Schemas::BuildOrderLineSchema(&oid_counter_);
      auto *const item = new storage::SqlTable(store_, item_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const warehouse =
          new storage::SqlTable(store_, warehouse_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const stock =
          new storage::SqlTable(store_, stock_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const district =
          new storage::SqlTable(store_, district_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const customer =
          new storage::SqlTable(store_, customer_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const history =
          new storage::SqlTable(store_, history_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const new_order =
          new storage::SqlTable(store_, new_order_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const order =
          new storage::SqlTable(store_, order_schema, static_cast<catalog::table_oid_t>(++oid_counter_));
      auto *const order_line =
          new storage::SqlTable(store_, order_line_schema, static_cast<catalog::table_oid_t>(++oid_counter_));

      Loader::PopulateTables(txn_manager_, generator_, item_schema, warehouse_schema, stock_schema, district_schema,
                             customer_schema, history_schema, new_order_schema, order_schema, order_line_schema, item,
                             warehouse, stock, district, customer, history, new_order, order, order_line);

      return new Database(item_schema, warehouse_schema, stock_schema, district_schema, customer_schema, history_schema,
                          new_order_schema, order_schema, order_line_schema, item, warehouse, stock, district, customer,
                          history, new_order, order, order_line);
    }

   private:
    transaction::TransactionManager *const txn_manager_;
    storage::BlockStore *const store_;
    Random *const generator_;
    uint64_t oid_counter_;
  };

  ~Database() {
    delete item_;
    delete warehouse_;
    delete stock_;
    delete district_;
    delete customer_;
    delete history_;
    delete new_order_;
    delete order_;
    delete order_line_;
  }

 private:
  template <class Random>
  friend class Builder;

  Database(catalog::Schema item_schema, catalog::Schema warehouse_schema, catalog::Schema stock_schema,
           catalog::Schema district_schema, catalog::Schema customer_schema, catalog::Schema history_schema,
           catalog::Schema new_order_schema, catalog::Schema order_schema, catalog::Schema order_line_schema,
           storage::SqlTable *const item, storage::SqlTable *const warehouse, storage::SqlTable *const stock,
           storage::SqlTable *const district, storage::SqlTable *const customer, storage::SqlTable *const history,
           storage::SqlTable *const new_order, storage::SqlTable *const order, storage::SqlTable *const order_line)
      : item_schema_(std::move(item_schema)),
        warehouse_schema_(std::move(warehouse_schema)),
        stock_schema_(std::move(stock_schema)),
        district_schema_(std::move(district_schema)),
        customer_schema_(std::move(customer_schema)),
        history_schema_(std::move(history_schema)),
        new_order_schema_(std::move(new_order_schema)),
        order_schema_(std::move(order_schema)),
        order_line_schema_(std::move(order_line_schema)),
        item_(item),
        warehouse_(warehouse),
        stock_(stock),
        district_(district),
        customer_(customer),
        history_(history),
        new_order_(new_order),
        order_(order),
        order_line_(order_line) {}

  const catalog::Schema item_schema_;
  const catalog::Schema warehouse_schema_;
  const catalog::Schema stock_schema_;
  const catalog::Schema district_schema_;
  const catalog::Schema customer_schema_;
  const catalog::Schema history_schema_;
  const catalog::Schema new_order_schema_;
  const catalog::Schema order_schema_;
  const catalog::Schema order_line_schema_;

  storage::SqlTable *const item_;
  storage::SqlTable *const warehouse_;
  storage::SqlTable *const stock_;
  storage::SqlTable *const district_;
  storage::SqlTable *const customer_;
  storage::SqlTable *const history_;
  storage::SqlTable *const new_order_;
  storage::SqlTable *const order_;
  storage::SqlTable *const order_line_;
};

}  // namespace terrier::tpcc
