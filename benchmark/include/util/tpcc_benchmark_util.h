#pragma once

#include <iostream>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "util/random_test_util.h"

namespace terrier {

class TPCC {
 public:
  explicit TPCC(storage::BlockStore *const store) : store_(store) {
    CreateWarehouseTable();
    CreateDistrictTable();
    CreateCustomerTable();
  }

  ~TPCC() {
    delete warehouse_;
    delete district_;
    delete customer_;
    //    delete history_;
    //    delete new_order_;
    //    delete order_;
    //    delete order_line_;
    //    delete item_;
    //    delete stock_;
  }

 private:
  catalog::Schema CreateWarehouseSchema() {
    std::vector<catalog::Schema::Column> warehouse_columns;
    warehouse_columns.reserve(9);
    warehouse_columns.emplace_back("W_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_NAME", type::TypeId::VARCHAR, 10, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_STREET_1", type::TypeId::VARCHAR, 20, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_STREET_2", type::TypeId::VARCHAR, 20, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_CITY", type::TypeId::VARCHAR, 20, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_STATE", type::TypeId::VARCHAR, 2, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_ZIP", type::TypeId::VARCHAR, 9, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_TAX", type::TypeId::DECIMAL, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    warehouse_columns.emplace_back("W_YTD", type::TypeId::DECIMAL, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    TERRIER_ASSERT(warehouse_columns.size() == 9, "Wrong number of columns.");
    return catalog::Schema(warehouse_columns);
  }

  catalog::Schema CreateDistrictSchema() {
    std::vector<catalog::Schema::Column> district_columns;
    district_columns.reserve(11);
    district_columns.emplace_back("D_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_W_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_NAME", type::TypeId::VARCHAR, 10, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_STREET_1", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_STREET_2", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_CITY", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_STATE", type::TypeId::VARCHAR, 2, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_ZIP", type::TypeId::VARCHAR, 9, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_TAX", type::TypeId::DECIMAL, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_YTD", type::TypeId::DECIMAL, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    district_columns.emplace_back("D_NEXT_O_ID", type::TypeId::INTEGER, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    TERRIER_ASSERT(district_columns.size() == 11, "Wrong number of columns.");
    return catalog::Schema(district_columns);
  }

  catalog::Schema CreateCustomerSchema() {
    std::vector<catalog::Schema::Column> customer_columns;
    customer_columns.reserve(21);
    customer_columns.emplace_back("C_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_D_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_W_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_FIRST", type::TypeId::VARCHAR, 16, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_MIDDLE", type::TypeId::VARCHAR, 2, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_LAST", type::TypeId::VARCHAR, 16, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_STREET_1", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_STREET_2", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_CITY", type::TypeId::VARCHAR, 20, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_STATE", type::TypeId::VARCHAR, 2, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_ZIP", type::TypeId::VARCHAR, 9, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_PHONE", type::TypeId::VARCHAR, 16, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_SINCE", type::TypeId::TIMESTAMP, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_CREDIT", type::TypeId::VARCHAR, 2, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_CREDIT_LIM", type::TypeId::DECIMAL, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_DISCOUNT", type::TypeId::DECIMAL, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_BALANCE", type::TypeId::DECIMAL, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_YTD_PAYMENT", type::TypeId::DECIMAL, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_PAYMENT_CNT", type::TypeId::SMALLINT, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_DELIVERY_CNT", type::TypeId::SMALLINT, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    customer_columns.emplace_back("C_DATA", type::TypeId::VARCHAR, 500, false,
                                  static_cast<catalog::col_oid_t>(GetNewOid()));
    TERRIER_ASSERT(customer_columns.size() == 21, "Wrong number of columns.");
    return catalog::Schema(customer_columns);
  }

  void CreateWarehouseTable() {
    TERRIER_ASSERT(warehouse_ == nullptr, "Warehouse table already exists.");
    const auto warehouse_schema = CreateWarehouseSchema();
    warehouse_ = new storage::SqlTable(store_, warehouse_schema, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateDistrictTable() {
    TERRIER_ASSERT(district_ == nullptr, "District table already exists.");
    const auto district_schema = CreateDistrictSchema();
    district_ = new storage::SqlTable(store_, district_schema, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateCustomerTable() {
    TERRIER_ASSERT(customer_ == nullptr, "District table already exists.");
    const auto customer_schema = CreateCustomerSchema();
    customer_ = new storage::SqlTable(store_, customer_schema, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  uint64_t GetNewOid() { return ++oid_counter; }

  uint64_t oid_counter = 0;

  storage::SqlTable *warehouse_ = nullptr;
  storage::SqlTable *district_ = nullptr;
  storage::SqlTable *customer_ = nullptr;
  //  storage::SqlTable *history_ = nullptr;
  //  storage::SqlTable *new_order_ = nullptr;
  //  storage::SqlTable *order_ = nullptr;
  //  storage::SqlTable *order_line_ = nullptr;
  //  storage::SqlTable *item_ = nullptr;
  //  storage::SqlTable *stock_ = nullptr;

  storage::BlockStore *const store_;
};

}  // namespace terrier