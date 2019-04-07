#pragma once

#include <cstring>
#include <random>
#include <string>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/loader.h"
#include "tpcc/schemas.h"
#include "type/type_id.h"
#include "util/random_test_util.h"

// TODO(Matt): it seems many fields can by smaller than INTEGER

namespace terrier::tpcc {

template <class Random>
class TPCC {
 public:
  explicit TPCC(transaction::TransactionManager *txn_manager, storage::BlockStore *store, Random *generator)
      : txn_manager_(txn_manager),
        store_(store),
        generator_(generator),
        oid_counter(0),
        item_schema_(Schemas::BuildItemSchema(&oid_counter)),
        warehouse_schema_(Schemas::BuildWarehouseSchema(&oid_counter)),
        stock_schema_(Schemas::BuildStockSchema(&oid_counter)),
        district_schema_(Schemas::BuildDistrictSchema(&oid_counter)),
        customer_schema_(Schemas::BuildCustomerSchema(&oid_counter)),
        history_schema_(Schemas::BuildHistorySchema(&oid_counter)),
        new_order_schema_(Schemas::BuildNewOrderSchema(&oid_counter)),
        order_schema_(Schemas::BuildOrderSchema(&oid_counter)),
        order_line_schema_(Schemas::BuildOrderLineSchema(&oid_counter)),
        item_(new storage::SqlTable(store_, item_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        warehouse_(new storage::SqlTable(store_, warehouse_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        stock_(new storage::SqlTable(store_, stock_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        district_(new storage::SqlTable(store_, district_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        customer_(new storage::SqlTable(store_, customer_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        history_(new storage::SqlTable(store_, history_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        new_order_(new storage::SqlTable(store_, new_order_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        order_(new storage::SqlTable(store_, order_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        order_line_(new storage::SqlTable(store_, order_line_schema_, static_cast<catalog::table_oid_t>(++oid_counter)))

  {
    Loader::PopulateTables(txn_manager_, generator_, item_, warehouse_, stock_, district_, customer_, history_,
                           new_order_, order_, order_line_, item_schema_, warehouse_schema_, stock_schema_,
                           district_schema_, customer_schema_, history_schema_, new_order_schema_, order_schema_,
                           order_line_schema_);
  }

  ~TPCC() {
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
  transaction::TransactionManager *const txn_manager_;
  storage::BlockStore *const store_;
  Random *const generator_;

  uint64_t oid_counter;

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
