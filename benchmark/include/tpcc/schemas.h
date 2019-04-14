#pragma once

#include <vector>
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/index/index_defs.h"

namespace terrier::tpcc {

// 1.3.1
struct Schemas {
  Schemas() = delete;

  static catalog::Schema BuildItemTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> item_columns;
    item_columns.reserve(num_item_cols);

    item_columns.emplace_back("I_ID", type::TypeId::INTEGER, false,  // 200,000
                              static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    item_columns.emplace_back("I_IM_ID", type::TypeId::INTEGER, false,  // 200,000
                              static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    item_columns.emplace_back("I_NAME", type::TypeId::VARCHAR, 24, false,
                              static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    item_columns.emplace_back("I_PRICE", type::TypeId::DECIMAL, false,  // numeric(5,2)
                              static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    item_columns.emplace_back("I_DATA", type::TypeId::VARCHAR, 50, false,
                              static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(item_columns.size() == num_item_cols, "Wrong number of columns for Item schema.");

    return catalog::Schema(item_columns);
  }

  static storage::index::IndexKeySchema BuildItemKeySchema(const catalog::Schema &schema, uint64_t *const oid_counter) {
    storage::index::IndexKeySchema item_key_schema;
    item_key_schema.reserve(num_item_key_cols);

    // primary key: I_ID
    item_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                 schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(item_key_schema.size() == num_item_key_cols, "Wrong number of columns for Item key schema.");

    return item_key_schema;
  }

  static catalog::Schema BuildWarehouseTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> warehouse_columns;
    warehouse_columns.reserve(num_warehouse_cols);

    warehouse_columns.emplace_back("W_ID", type::TypeId::INTEGER, false,  // 2*W
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_NAME", type::TypeId::VARCHAR, 10, false,
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_STREET_1", type::TypeId::VARCHAR, 20, false,
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_STREET_2", type::TypeId::VARCHAR, 20, false,
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_CITY", type::TypeId::VARCHAR, 20, false,
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_STATE", type::TypeId::VARCHAR, 2, false,  // fixed(2)
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_ZIP", type::TypeId::VARCHAR, 9, false,  // fixed(9)
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_TAX", type::TypeId::DECIMAL, false,  // signed numeric(4,4)
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    warehouse_columns.emplace_back("W_YTD", type::TypeId::DECIMAL, false,  // signed numeric(12,2)
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(warehouse_columns.size() == num_warehouse_cols, "Wrong number of columns for Warehouse schema.");

    return catalog::Schema(warehouse_columns);
  }

  static storage::index::IndexKeySchema BuildWarehouseKeySchema(const catalog::Schema &schema,
                                                                uint64_t *const oid_counter) {
    storage::index::IndexKeySchema warehouse_key_schema;
    warehouse_key_schema.reserve(num_warehouse_key_cols);

    // primary key: W_ID
    warehouse_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                      schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(warehouse_key_schema.size() == num_warehouse_key_cols,
                   "Wrong number of columns for Warehouse key schema.");

    return warehouse_key_schema;
  }

  static catalog::Schema BuildStockTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> stock_columns;
    stock_columns.reserve(num_stock_cols);

    stock_columns.emplace_back("S_I_ID", type::TypeId::INTEGER, false,  // 200,000
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_W_ID", type::TypeId::INTEGER, false,  // 2*W
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_QUANTITY", type::TypeId::SMALLINT, false,  // signed numeric(4)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_01", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_02", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_03", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_04", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_05", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_06", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_07", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_08", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_09", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DIST_10", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_YTD", type::TypeId::INTEGER, false,  // numeric(8)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_ORDER_CNT", type::TypeId::SMALLINT, false,  // numeric(4)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_REMOTE_CNT", type::TypeId::SMALLINT, false,  // numeric(4)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    stock_columns.emplace_back("S_DATA", type::TypeId::VARCHAR, 50, false,
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(stock_columns.size() == num_stock_cols, "Wrong number of columns for Stock schema.");

    return catalog::Schema(stock_columns);
  }

  static storage::index::IndexKeySchema BuildStockKeySchema(const catalog::Schema &schema,
                                                            uint64_t *const oid_counter) {
    storage::index::IndexKeySchema stock_key_schema;
    stock_key_schema.reserve(num_stock_key_cols);

    // primary key: S_W_ID, S_I_ID
    stock_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                  schema.GetColumn(1).GetType(), schema.GetColumn(1).GetNullable());
    stock_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                  schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(stock_key_schema.size() == num_stock_key_cols, "Wrong number of columns for Stock key schema.");

    return stock_key_schema;
  }

  static catalog::Schema BuildDistrictTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> district_columns;
    district_columns.reserve(num_district_cols);

    district_columns.emplace_back("D_ID", type::TypeId::INTEGER, false,  // 20
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_NAME", type::TypeId::VARCHAR, 10, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_STREET_1", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_STREET_2", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_CITY", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_STATE", type::TypeId::VARCHAR, 2, false,  // fixed(2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_ZIP", type::TypeId::VARCHAR, 9, false,  // fixed(9)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_TAX", type::TypeId::DECIMAL, false,  // signed numeric(4,4)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_YTD", type::TypeId::DECIMAL, false,  // signed numeric(12,2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    district_columns.emplace_back("D_NEXT_O_ID", type::TypeId::INTEGER, false,  // 10,000,000
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(district_columns.size() == num_district_cols, "Wrong number of columns for District schema.");

    return catalog::Schema(district_columns);
  }

  static storage::index::IndexKeySchema BuildDistrictKeySchema(const catalog::Schema &schema,
                                                               uint64_t *const oid_counter) {
    storage::index::IndexKeySchema district_key_schema;
    district_key_schema.reserve(num_district_key_cols);

    // primary key: D_W_ID, D_ID
    district_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                     schema.GetColumn(1).GetType(), schema.GetColumn(1).GetNullable());
    district_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                     schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(district_key_schema.size() == num_district_key_cols,
                   "Wrong number of columns for District key schema.");

    return district_key_schema;
  }

  static catalog::Schema BuildCustomerTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> customer_columns;
    customer_columns.reserve(num_customer_cols);

    customer_columns.emplace_back("C_ID", type::TypeId::INTEGER, false,  // 96,000
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_D_ID", type::TypeId::INTEGER, false,  // 20
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_FIRST", type::TypeId::VARCHAR, 16, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_MIDDLE", type::TypeId::VARCHAR, 2, false,  // fixed(2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_LAST", type::TypeId::VARCHAR, 16, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_STREET_1", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_STREET_2", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_CITY", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_STATE", type::TypeId::VARCHAR, 2, false,  // fixed(2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_ZIP", type::TypeId::VARCHAR, 9, false,  // fixed(9)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_PHONE", type::TypeId::VARCHAR, 16, false,  // fixed(16)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_SINCE", type::TypeId::TIMESTAMP, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_CREDIT", type::TypeId::VARCHAR, 2, false,  // fixed(2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_CREDIT_LIM", type::TypeId::DECIMAL, false,  // signed numeric(12,2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_DISCOUNT", type::TypeId::DECIMAL, false,  // signed numeric(4,4)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_BALANCE", type::TypeId::DECIMAL, false,  // signed numeric(12,2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_YTD_PAYMENT", type::TypeId::DECIMAL, false,  // signed numeric(12,2)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_PAYMENT_CNT", type::TypeId::SMALLINT, false,  // numeric(4)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_DELIVERY_CNT", type::TypeId::SMALLINT, false,  // numeric(4)
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    customer_columns.emplace_back("C_DATA", type::TypeId::VARCHAR, 500, false,
                                  static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(customer_columns.size() == num_customer_cols, "Wrong number of columns for Customer schema.");

    return catalog::Schema(customer_columns);
  }

  static storage::index::IndexKeySchema BuildCustomerKeySchema(const catalog::Schema &schema,
                                                               uint64_t *const oid_counter) {
    storage::index::IndexKeySchema customer_key_schema;
    customer_key_schema.reserve(num_customer_key_cols);

    // primary key: C_W_ID, C_D_ID, C_ID
    customer_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                     schema.GetColumn(2).GetType(), schema.GetColumn(2).GetNullable());
    customer_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                     schema.GetColumn(1).GetType(), schema.GetColumn(1).GetNullable());
    customer_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                     schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(customer_key_schema.size() == num_customer_key_cols,
                   "Wrong number of columns for Customer key schema.");

    return customer_key_schema;
  }

  static storage::index::IndexKeySchema BuildCustomerNameKeySchema(const catalog::Schema &schema,
                                                                   uint64_t *const oid_counter) {
    storage::index::IndexKeySchema customer_name_key_schema;
    customer_name_key_schema.reserve(num_customer_name_key_cols);

    // primary key: C_W_ID, C_D_ID, C_LAST
    customer_name_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(2).GetType(),
        schema.GetColumn(2).GetNullable());
    customer_name_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(1).GetType(),
        schema.GetColumn(1).GetNullable());
    customer_name_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(5).GetType(),
        schema.GetColumn(5).GetNullable(), schema.GetColumn(5).GetMaxVarlenSize());

    TERRIER_ASSERT(customer_name_key_schema.size() == num_customer_name_key_cols,
                   "Wrong number of columns for Customer Name key schema.");

    return customer_name_key_schema;
  }

  static catalog::Schema BuildHistoryTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> history_columns;
    history_columns.reserve(num_history_cols);

    history_columns.emplace_back("H_C_ID", type::TypeId::INTEGER, false,  // 96,000
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_C_D_ID", type::TypeId::INTEGER, false,  // 20
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_C_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_D_ID", type::TypeId::INTEGER, false,  // 20
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_DATE", type::TypeId::TIMESTAMP, false,
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_AMOUNT", type::TypeId::DECIMAL, false,  // signed numeric(6,2)
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    history_columns.emplace_back("H_DATA", type::TypeId::VARCHAR, 24, false,
                                 static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(history_columns.size() == num_history_cols, "Wrong number of columns for History schema.");

    return catalog::Schema(history_columns);
  }

  static catalog::Schema BuildNewOrderTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> new_order_columns;
    new_order_columns.reserve(num_new_order_cols);

    new_order_columns.emplace_back("NO_O_ID", type::TypeId::INTEGER, false,  // 10,000,000
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    new_order_columns.emplace_back("NO_D_ID", type::TypeId::INTEGER, false,  // 20
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    new_order_columns.emplace_back("NO_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                   static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(new_order_columns.size() == num_new_order_cols, "Wrong number of columns for New Order schema.");

    return catalog::Schema(new_order_columns);
  }

  static storage::index::IndexKeySchema BuildNewOrderKeySchema(const catalog::Schema &schema,
                                                               uint64_t *const oid_counter) {
    storage::index::IndexKeySchema new_order_key_schema;
    new_order_key_schema.reserve(num_new_order_key_cols);

    // primary key: NO_W_ID, NO_D_ID, NO_O_ID
    new_order_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                      schema.GetColumn(2).GetType(), schema.GetColumn(2).GetNullable());
    new_order_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                      schema.GetColumn(1).GetType(), schema.GetColumn(1).GetNullable());
    new_order_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                      schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(new_order_key_schema.size() == num_new_order_key_cols,
                   "Wrong number of columns for New Order key schema.");

    return new_order_key_schema;
  }

  static catalog::Schema BuildOrderTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> order_columns;
    order_columns.reserve(num_order_cols);

    order_columns.emplace_back("O_ID", type::TypeId::INTEGER, false,  // 10,000,000
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_D_ID", type::TypeId::INTEGER, false,  // 20
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_W_ID", type::TypeId::INTEGER, false,  // 2*W
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_C_ID", type::TypeId::INTEGER, false,  // 96,000
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_ENTRY_D", type::TypeId::TIMESTAMP, false,
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_CARRIER_ID", type::TypeId::INTEGER, true,  // 10 or NULL
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_OL_CNT", type::TypeId::INTEGER, false,  // numeric(2)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_columns.emplace_back("O_ALL_LOCAL", type::TypeId::INTEGER, false,  // numeric(1)
                               static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(order_columns.size() == num_order_cols, "Wrong number of columns for Order schema.");

    return catalog::Schema(order_columns);
  }

  static storage::index::IndexKeySchema BuildOrderKeySchema(const catalog::Schema &schema,
                                                            uint64_t *const oid_counter) {
    storage::index::IndexKeySchema order_key_schema;
    order_key_schema.reserve(num_order_key_cols);

    // primary key: O_W_ID, O_D_ID, O_ID
    order_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                  schema.GetColumn(2).GetType(), schema.GetColumn(2).GetNullable());
    order_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                  schema.GetColumn(1).GetType(), schema.GetColumn(1).GetNullable());
    order_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                  schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(order_key_schema.size() == num_order_key_cols, "Wrong number of columns for Order key schema.");

    return order_key_schema;
  }

  static storage::index::IndexKeySchema BuildOrderSecondaryKeySchema(const catalog::Schema &schema,
                                                                     uint64_t *const oid_counter) {
    storage::index::IndexKeySchema order_secondary_key_schema;
    order_secondary_key_schema.reserve(num_order_secondary_key_cols);

    // key: O_W_ID, O_D_ID, O_C_ID, O_ID (for Order Status)
    order_secondary_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(2).GetType(),
        schema.GetColumn(2).GetNullable());
    order_secondary_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(1).GetType(),
        schema.GetColumn(1).GetNullable());
    order_secondary_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(3).GetType(),
        schema.GetColumn(3).GetNullable());
    order_secondary_key_schema.emplace_back(
        static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))), schema.GetColumn(0).GetType(),
        schema.GetColumn(0).GetNullable());

    TERRIER_ASSERT(order_secondary_key_schema.size() == num_order_secondary_key_cols,
                   "Wrong number of columns for Order secondary key schema.");

    return order_secondary_key_schema;
  }

  static catalog::Schema BuildOrderLineTupleSchema(uint64_t *const oid_counter) {
    std::vector<catalog::Schema::Column> order_line_columns;
    order_line_columns.reserve(num_order_line_cols);

    order_line_columns.emplace_back("OL_O_ID", type::TypeId::INTEGER, false,  // 10,000,000
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_D_ID", type::TypeId::INTEGER, false,  // 20
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_NUMBER", type::TypeId::INTEGER, false,  // 15
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_I_ID", type::TypeId::INTEGER, false,  // 200,000
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_SUPPLY_W_ID", type::TypeId::INTEGER, false,  // 2*W
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_DELIVERY_D", type::TypeId::TIMESTAMP, true,
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_QUANTITY", type::TypeId::INTEGER, false,  // numeric(2)
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_AMOUNT", type::TypeId::DECIMAL, false,  // signed numeric(6,2)
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));
    order_line_columns.emplace_back("OL_DIST_INFO", type::TypeId::VARCHAR, 24, false,  // fixed(24)
                                    static_cast<catalog::col_oid_t>(static_cast<uint32_t>(++(*oid_counter))));

    TERRIER_ASSERT(order_line_columns.size() == num_order_line_cols, "Wrong number of columns for Order Line schema.");

    return catalog::Schema(order_line_columns);
  }

  static storage::index::IndexKeySchema BuildOrderLineKeySchema(const catalog::Schema &schema,
                                                                uint64_t *const oid_counter) {
    storage::index::IndexKeySchema order_line_key_schema;
    order_line_key_schema.reserve(num_order_line_key_cols);

    // primary key: OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
    order_line_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                       schema.GetColumn(2).GetType(), schema.GetColumn(2).GetNullable());
    order_line_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                       schema.GetColumn(1).GetType(), schema.GetColumn(1).GetNullable());
    order_line_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                       schema.GetColumn(0).GetType(), schema.GetColumn(0).GetNullable());
    order_line_key_schema.emplace_back(static_cast<catalog::indexkeycol_oid_t>(static_cast<uint32_t>(++(*oid_counter))),
                                       schema.GetColumn(3).GetType(), schema.GetColumn(3).GetNullable());

    TERRIER_ASSERT(order_line_key_schema.size() == num_order_line_key_cols,
                   "Wrong number of columns for Order Line key schema.");

    return order_line_key_schema;
  }

 private:
  static constexpr uint8_t num_item_cols = 5;
  static constexpr uint8_t num_warehouse_cols = 9;
  static constexpr uint8_t num_stock_cols = 17;
  static constexpr uint8_t num_district_cols = 11;
  static constexpr uint8_t num_customer_cols = 21;
  static constexpr uint8_t num_history_cols = 8;
  static constexpr uint8_t num_new_order_cols = 3;
  static constexpr uint8_t num_order_cols = 8;
  static constexpr uint8_t num_order_line_cols = 10;

  static constexpr uint8_t num_item_key_cols = 1;
  static constexpr uint8_t num_warehouse_key_cols = 1;
  static constexpr uint8_t num_stock_key_cols = 2;
  static constexpr uint8_t num_district_key_cols = 2;
  static constexpr uint8_t num_customer_key_cols = 3;
  static constexpr uint8_t num_customer_name_key_cols = 3;
  static constexpr uint8_t num_new_order_key_cols = 3;
  static constexpr uint8_t num_order_key_cols = 3;
  static constexpr uint8_t num_order_secondary_key_cols = 4;
  static constexpr uint8_t num_order_line_key_cols = 4;
};

}  // namespace terrier::tpcc
