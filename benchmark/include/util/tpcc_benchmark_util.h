#pragma once

#include <chrono>  // NOLINT
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "util/random_test_util.h"
#include "util/transaction_benchmark_util.h"

// TODO(Matt): it seems many fields can by smaller than INTEGER

namespace terrier {

constexpr uint16_t num_districts_per_warehouse_ = 10;
constexpr uint16_t num_customers_per_district_ = 3000;

template <class Random>
class TPCC {
 public:
  explicit TPCC(transaction::TransactionManager *const txn_manager, storage::BlockStore *const store,
                Random *const generator)
      : txn_manager_(txn_manager), store_(store), generator_(generator) {
    CreateItemTable();
    CreateWarehouseTable();
    CreateStockTable();
    CreateDistrictTable();
    CreateCustomerTable();
    CreateHistoryTable();
    CreateNewOrderTable();
    CreateOrderTable();
    CreateOrderLineTable();

    PopulateTables();
  }

  ~TPCC() {
    delete item_;
    delete item_schema_;
    delete warehouse_;
    delete warehouse_schema_;
    delete stock_;
    delete stock_schema_;
    delete district_;
    delete district_schema_;
    delete customer_;
    delete customer_schema_;
    delete history_;
    delete history_schema_;
    delete new_order_;
    delete new_order_schema_;
    delete order_;
    delete order_schema_;
    delete order_line_;
    delete order_line_schema_;
  }

 private:
  void CreateItemSchema() {
    TERRIER_ASSERT(item_schema_ == nullptr, "Item schema already exists.");
    std::vector<catalog::Schema::Column> item_columns;
    item_columns.reserve(5);

    item_columns.emplace_back("I_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    item_columns.emplace_back("I_IM_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    item_columns.emplace_back("I_NAME", type::TypeId::VARCHAR, 24, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    item_columns.emplace_back("I_PRICE", type::TypeId::DECIMAL, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    item_columns.emplace_back("I_DATA", type::TypeId::VARCHAR, 50, false, static_cast<catalog::col_oid_t>(GetNewOid()));

    TERRIER_ASSERT(item_columns.size() == 5, "Wrong number of columns for Item schema.");

    item_schema_ = new catalog::Schema(item_columns);
  }

  void CreateWarehouseSchema() {
    TERRIER_ASSERT(warehouse_schema_ == nullptr, "Warehouse schema already exists.");
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

    TERRIER_ASSERT(warehouse_columns.size() == 9, "Wrong number of columns for Warehouse schema.");

    warehouse_schema_ = new catalog::Schema(warehouse_columns);
  }

  void CreateStockSchema() {
    TERRIER_ASSERT(stock_schema_ == nullptr, "Stock schema already exists.");
    std::vector<catalog::Schema::Column> stock_columns;
    stock_columns.reserve(17);

    stock_columns.emplace_back("S_I_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_W_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_QUANTITY", type::TypeId::SMALLINT, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_01", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_02", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_03", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_04", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_05", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_06", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_07", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_08", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_09", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DIST_10", type::TypeId::VARCHAR, 24, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_YTD", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_ORDER_CNT", type::TypeId::SMALLINT, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_REMOTE_CNT", type::TypeId::SMALLINT, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    stock_columns.emplace_back("S_DATA", type::TypeId::VARCHAR, 50, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));

    TERRIER_ASSERT(stock_columns.size() == 17, "Wrong number of columns for Stock schema.");

    stock_schema_ = new catalog::Schema(stock_columns);
  }

  void CreateDistrictSchema() {
    TERRIER_ASSERT(district_schema_ == nullptr, "District schema already exists.");
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

    TERRIER_ASSERT(district_columns.size() == 11, "Wrong number of columns for District schema.");

    district_schema_ = new catalog::Schema(district_columns);
  }

  void CreateCustomerSchema() {
    TERRIER_ASSERT(customer_schema_ == nullptr, "Customer schema already exists.");
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

    TERRIER_ASSERT(customer_columns.size() == 21, "Wrong number of columns for Customer schema.");

    customer_schema_ = new catalog::Schema(customer_columns);
  }

  void CreateHistorySchema() {
    TERRIER_ASSERT(history_schema_ == nullptr, "History schema already exists.");
    std::vector<catalog::Schema::Column> history_columns;
    history_columns.reserve(8);

    history_columns.emplace_back("H_C_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_C_D_ID", type::TypeId::INTEGER, false,
                                 static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_C_W_ID", type::TypeId::INTEGER, false,
                                 static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_D_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_W_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_DATE", type::TypeId::TIMESTAMP, false,
                                 static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_AMOUNT", type::TypeId::DECIMAL, false,
                                 static_cast<catalog::col_oid_t>(GetNewOid()));
    history_columns.emplace_back("H_DATA", type::TypeId::VARCHAR, 24, false,
                                 static_cast<catalog::col_oid_t>(GetNewOid()));

    TERRIER_ASSERT(history_columns.size() == 8, "Wrong number of columns for History schema.");

    history_schema_ = new catalog::Schema(history_columns);
  }

  void CreateNewOrderSchema() {
    TERRIER_ASSERT(new_order_schema_ == nullptr, "New Order schema already exists.");
    std::vector<catalog::Schema::Column> new_order_columns;
    new_order_columns.reserve(3);

    new_order_columns.emplace_back("NO_O_ID", type::TypeId::INTEGER, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    new_order_columns.emplace_back("NO_D_ID", type::TypeId::INTEGER, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));
    new_order_columns.emplace_back("NO_W_ID", type::TypeId::INTEGER, false,
                                   static_cast<catalog::col_oid_t>(GetNewOid()));

    TERRIER_ASSERT(new_order_columns.size() == 3, "Wrong number of columns for New Order schema.");

    new_order_schema_ = new catalog::Schema(new_order_columns);
  }

  void CreateOrderSchema() {
    TERRIER_ASSERT(order_schema_ == nullptr, "Order schema already exists.");
    std::vector<catalog::Schema::Column> order_columns;
    order_columns.reserve(8);

    order_columns.emplace_back("O_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_D_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_W_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_C_ID", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_ENTRY_D", type::TypeId::TIMESTAMP, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_CARRIER_ID", type::TypeId::INTEGER, true,
                               static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_OL_CNT", type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
    order_columns.emplace_back("O_ALL_LOCAL", type::TypeId::INTEGER, false,
                               static_cast<catalog::col_oid_t>(GetNewOid()));

    TERRIER_ASSERT(order_columns.size() == 8, "Wrong number of columns for Order schema.");

    order_schema_ = new catalog::Schema(order_columns);
  }

  void CreateOrderLineSchema() {
    TERRIER_ASSERT(order_line_schema_ == nullptr, "Order Line schema already exists.");
    std::vector<catalog::Schema::Column> order_line_columns;
    order_line_columns.reserve(10);

    order_line_columns.emplace_back("OL_O_ID", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_D_ID", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_W_ID", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_NUMBER", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_I_ID", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_SUPPLY_W_ID", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_DELIVERY_D", type::TypeId::TIMESTAMP, true,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_QUANTITY", type::TypeId::INTEGER, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_AMOUNT", type::TypeId::DECIMAL, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));
    order_line_columns.emplace_back("OL_DIST_INFO", type::TypeId::VARCHAR, 24, false,
                                    static_cast<catalog::col_oid_t>(GetNewOid()));

    TERRIER_ASSERT(order_line_columns.size() == 10, "Wrong number of columns for Order Line schema.");

    order_line_schema_ = new catalog::Schema(order_line_columns);
  }

  void CreateItemTable() {
    TERRIER_ASSERT(item_ == nullptr, "Item table already exists.");
    CreateItemSchema();
    item_ = new storage::SqlTable(store_, *item_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateWarehouseTable() {
    TERRIER_ASSERT(warehouse_ == nullptr, "Warehouse table already exists.");
    CreateWarehouseSchema();
    warehouse_ = new storage::SqlTable(store_, *warehouse_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateStockTable() {
    TERRIER_ASSERT(stock_ == nullptr, "Stock table already exists.");
    CreateStockSchema();
    stock_ = new storage::SqlTable(store_, *stock_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateDistrictTable() {
    TERRIER_ASSERT(district_ == nullptr, "District table already exists.");
    CreateDistrictSchema();
    district_ = new storage::SqlTable(store_, *district_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateCustomerTable() {
    TERRIER_ASSERT(customer_ == nullptr, "Customer table already exists.");
    CreateCustomerSchema();
    customer_ = new storage::SqlTable(store_, *customer_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateHistoryTable() {
    TERRIER_ASSERT(history_ == nullptr, "History table already exists.");
    CreateHistorySchema();
    history_ = new storage::SqlTable(store_, *history_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateNewOrderTable() {
    TERRIER_ASSERT(new_order_ == nullptr, "New Order table already exists.");
    CreateNewOrderSchema();
    new_order_ = new storage::SqlTable(store_, *new_order_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateOrderTable() {
    TERRIER_ASSERT(order_ == nullptr, "Order table already exists.");
    CreateOrderSchema();
    order_ = new storage::SqlTable(store_, *order_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  void CreateOrderLineTable() {
    TERRIER_ASSERT(order_line_ == nullptr, "Order Line table already exists.");
    CreateOrderLineSchema();
    order_line_ = new storage::SqlTable(store_, *order_line_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }

  static std::vector<catalog::col_oid_t> AllColOidsForSchema(const catalog::Schema &schema) {
    const auto &cols = schema.GetColumns();
    std::vector<catalog::col_oid_t> col_oids;
    col_oids.reserve(cols.size());
    for (const auto &col : cols) {
      col_oids.emplace_back(col.GetOid());
    }
    return col_oids;
  }

  uint64_t Timestamp() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  char RandomAlphaNumericChar(const bool numeric_only) const {
    static const char *alpha_num = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const size_t length = numeric_only ? 9 : 61;
    return alpha_num[std::uniform_int_distribution(static_cast<size_t>(0), length)(*generator_)];
  }

  // 4.3.2.2
  std::string RandomAlphaNumericString(const uint32_t x, const uint32_t y, const bool numeric_only) const {
    const uint32_t length = std::uniform_int_distribution(x, y)(*generator_);
    std::string astring(length, 'a');
    for (uint32_t i = 0; i < length; i++) {
      astring[i] = RandomAlphaNumericChar(numeric_only);
    }
    return astring;
  }

  storage::VarlenEntry RandomAlphaNumericVarlenEntry(const uint32_t x, const uint32_t y,
                                                     const bool numeric_only) const {
    TERRIER_ASSERT(x <= y, "Minimum cannot be greater than the maximum length.");
    const auto astring = RandomAlphaNumericString(x, y, numeric_only);
    if (astring.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(astring.data()), astring.length());
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(astring.length());
    std::memcpy(varlen, astring.data(), astring.length());
    return storage::VarlenEntry::Create(varlen, astring.length(), true);
  }

  // 4.3.2.3
  storage::VarlenEntry RandomLastNameVarlenEntry(const uint16_t numbers) const {
    TERRIER_ASSERT(numbers >= 0 && numbers <= 999, "Invalid input generating C_LAST.");
    static const char *const syllables[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};

    const uint8_t syllable1 = numbers / 100;
    const uint8_t syllable2 = (numbers / 10 % 10);
    const uint8_t syllable3 = numbers % 10;

    std::string last_name(syllables[syllable1]);
    last_name.append(syllables[syllable2]);
    last_name.append(syllables[syllable3]);

    if (last_name.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(last_name.data()), last_name.length());
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(last_name.length());
    std::memcpy(varlen, last_name.data(), last_name.length());
    return storage::VarlenEntry::Create(varlen, last_name.length(), true);
  }

  // 4.3.2.5
  template <typename T>
  T RandomWithin(uint32_t x, uint32_t y, uint32_t p) const {
    return std::uniform_int_distribution(x, y)(*generator_) / static_cast<T>(std::pow(10, p));
  }

  // 2.1.6
  // NURand(A,x,y)=(((random(0,A)| random(x,y))+C)%(y-x+1))+x
  // where:
  // exp-1 | exp-2 stands for the bitwise logical OR operation between exp-1 and exp-2
  // exp-1 % exp-2 stands for exp-1 modulo exp-2
  // random(x, y) stands for randomly selected within [x .. y] 2.1.6.1
  // A is a constant chosen according to the size of the range [x .. y]
  //       for C_LAST, the range is [0 .. 999] and A = 255
  //       for C_ID, the range is [1 .. 3000] and A = 1023
  //       for OL_I_ID, the range is [1 .. 100000] and A = 8191
  // C is a run-time constant randomly chosen within[0 .. A] that can be varied without altering performance.
  // The same C value, per field (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals.
  int32_t NURand(const int32_t A, const int32_t x, const int32_t y) const {
    TERRIER_ASSERT(
        (A == 255 && x == 0 && y == 999) || (A == 1023 && x == 1 && y == 3000) || (A == 8191 && x == 1 && y == 100000),
        "Invalid inputs to NURand().");

    static const int32_t C_c_last = RandomWithin<int32_t>(0, 255, 0);
    static const int32_t C_c_id = RandomWithin<int32_t>(0, 1023, 0);
    static const int32_t C_ol_i_id = RandomWithin<int32_t>(0, 8191, 0);

    int32_t C;

    if (A == 255) {
      C = C_c_last;
    } else if (A == 1023) {
      C = C_c_id;
    } else {
      C = C_ol_i_id;
    }

    return (((RandomWithin<int32_t>(0, A, 0) | RandomWithin<int32_t>(x, y, 0)) + C) % (y - x - +1)) + x;
  }

  // 4.3.2.7
  storage::VarlenEntry RandomZipVarlenEntry() const {
    auto string = RandomAlphaNumericString(4, 4, true);
    string.append("11111");
    TERRIER_ASSERT(string.length() == 9, "Wrong ZIP code length.");
    return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(string.data()), string.length());
  }

  // 4.3.3.1
  storage::VarlenEntry RandomOriginalVarlenEntry(const uint32_t x, const uint32_t y) const {
    TERRIER_ASSERT(x <= y, "Minimum cannot be greater than the maximum length.");
    auto astring = RandomAlphaNumericString(x, y, false);
    TERRIER_ASSERT(astring.length() >= 8, "Needs enough room for ORIGINAL.");

    const uint32_t original_index = std::uniform_int_distribution(
        static_cast<uint32_t>(0), static_cast<uint32_t>(astring.length() - 8))(*generator_);

    astring.replace(original_index, 8, "ORIGINAL");

    auto *const varlen = common::AllocationUtil::AllocateAligned(astring.length());
    std::memcpy(varlen, astring.data(), astring.length());
    return storage::VarlenEntry::Create(varlen, astring.length(), true);
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildItemTuple(const int32_t i_id, const bool original, byte *const buffer,
                                        const storage::ProjectedRowInitializer &pr_initializer,
                                        const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(i_id >= 1 && i_id <= 100000, "Invalid i_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // I_ID unique within [100,000]
    auto col_oid = item_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(item_schema_->GetColumn(col_offset - 1).GetName() == "I_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = i_id;

    // I_IM_ID random within [1 .. 10,000]
    col_oid = item_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(item_schema_->GetColumn(col_offset - 1).GetName() == "I_IM_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = RandomWithin<int32_t>(1, 10000, 0);

    // I_NAME random a-string [14 .. 24]
    col_oid = item_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(item_schema_->GetColumn(col_offset - 1).GetName() == "I_NAME", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(14, 24, false);

    // I_PRICE random within [1.00 .. 100.00]
    col_oid = item_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(item_schema_->GetColumn(col_offset - 1).GetName() == "I_PRICE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(100, 10000, 2);

    // I_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within I_DATA
    col_oid = item_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(item_schema_->GetColumn(col_offset - 1).GetName() == "I_DATA", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    if (original) {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomOriginalVarlenEntry(26, 50);
    } else {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(26, 50, false);
    }

    TERRIER_ASSERT(col_offset == 5, "Didn't get every attribute for Item tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildWarehouseTuple(const int32_t w_id, byte *const buffer,
                                             const storage::ProjectedRowInitializer &pr_initializer,
                                             const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // W_ID unique within [number_of_configured_warehouses]
    auto col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // W_NAME random a-string [6 .. 10]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_NAME", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(6, 10, false);

    // W_STREET_1 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_STREET_1", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_STREET_2 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_STREET_2", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_CITY random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_CITY", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_STATE random a-string of 2 letters
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_STATE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // W_ZIP generated according to Clause 4.3.2.7
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_ZIP", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // W_TAX random within [0.0000 .. 0.2000]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_TAX", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 2000, 4);

    // W_YTD = 300,000.00
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(warehouse_schema_->GetColumn(col_offset - 1).GetName() == "W_YTD", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 300000.0;

    TERRIER_ASSERT(col_offset == 9, "Didn't get every attribute for Warehouse tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildStockTuple(const int32_t s_i_id, const int32_t w_id, const bool original,
                                         byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                         const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(s_i_id >= 1 && s_i_id <= 100000, "Invalid s_i_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // S_I_ID unique within [100,000]
    auto col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_I_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = s_i_id;

    // S_W_ID = W_ID
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // S_QUANTITY random within [10 .. 100]
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_QUANTITY", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = RandomWithin<int16_t>(10, 100, 0);

    // S_DIST_01 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_01", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_02 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_02", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_03 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_03", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_04 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_04", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_05 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_05", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_06 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_06", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_07 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_07", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_08 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_08", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_09 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_09", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_DIST_10 random a-string of 24 letters
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DIST_10", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    // S_YTD = 0
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_YTD", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = 0;

    // S_ORDER_CNT = 0
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_ORDER_CNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 0;

    // S_REMOTE_CNT = 0
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_REMOTE_CNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 0;

    // S_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within S_DATA
    col_oid = stock_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(stock_schema_->GetColumn(col_offset - 1).GetName() == "S_DATA", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    if (original) {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomOriginalVarlenEntry(26, 50);
    } else {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(26, 50, false);
    }

    TERRIER_ASSERT(col_offset == 17, "Didn't get every attribute for Stock tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildDistrictTuple(const int32_t d_id, const int32_t w_id, byte *const buffer,
                                            const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // D_ID unique within [10]
    auto col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // D_W_ID = W_ID
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // D_NAME random a-string [6 .. 10]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_NAME", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(6, 10, false);

    // D_STREET_1 random a-string [10 .. 20]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_STREET_1", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_STREET_2 random a-string [10 .. 20]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_STREET_2", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_CITY random a-string [10 .. 20]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_CITY", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_STATE random a-string of 2 letters
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_STATE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // D_ZIP generated according to Clause 4.3.2.7
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_ZIP", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // D_TAX random within [0.0000 .. 0.2000]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_TAX", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 2000, 4);

    // D_YTD = 30,000.00
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_YTD", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 30000.0;

    // D_NEXT_O_ID = 3,001
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(district_schema_->GetColumn(col_offset - 1).GetName() == "D_NEXT_O_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = 3001;

    TERRIER_ASSERT(col_offset == 11, "Didn't get every attribute for District tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildCustomerTuple(const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                            const bool good_credit, byte *const buffer,
                                            const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // C_ID unique within [3,000]
    auto col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = c_id;

    // C_D_ID = D_ID
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_D_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // C_W_ID = D_W_ID
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // C_FIRST random a-string [8 .. 16]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_FIRST", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(8, 16, false);

    // C_MIDDLE = "OE"
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_MIDDLE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) =
        storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("OE"), 2);

    // C_LAST generated according to Clause 4.3.2.3, iterating through the range of [0 .. 999] for the first 1,000
    // customers, and generating a non-uniform random number using the function NURand(255,0,999) for each of the
    // remaining 2,000 customers. The run-time constant C (see Clause 2.1.6) used for the database population must be
    // randomly chosen independently from the test run(s).
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_LAST", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    if (c_id < 1000) {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomLastNameVarlenEntry(c_id);
    } else {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomLastNameVarlenEntry(NURand(255, 0, 999));
    }

    // C_STREET_1 random a-string [10 .. 20]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_STREET_1", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_STREET_2 random a-string [10 .. 20]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_STREET_2", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_CITY random a-string [10 .. 20]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_CITY", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_STATE random a-string of 2 letters
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_STATE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // C_ZIP generated according to Clause 4.3.2.7
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_ZIP", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // C_PHONE random n-string of 16 numbers
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_PHONE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(16, 16, true);

    // C_SINCE date/ time given by the operating system when the CUSTOMER table was populated.
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_SINCE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<uint64_t *>(attr) = Timestamp();

    // C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_CREDIT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) =
        good_credit ? storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("GC"), 2)
                    : storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("BC"), 2);

    // C_CREDIT_LIM = 50,000.00
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_CREDIT_LIM", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 50000.0;

    // C_DISCOUNT random within [0.0000 .. 0.5000]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_DISCOUNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 5000, 4);

    // C_BALANCE = -10.00
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_BALANCE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = -10.0;

    // C_YTD_PAYMENT = 10.00
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_YTD_PAYMENT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 10.0;

    // C_PAYMENT_CNT = 1
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_PAYMENT_CNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 1;

    // C_DELIVERY_CNT = 0
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_DELIVERY_CNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 0;

    // C_DATA random a-string [300 .. 500]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(customer_schema_->GetColumn(col_offset - 1).GetName() == "C_DATA", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(300, 500, false);

    TERRIER_ASSERT(col_offset == 21, "Didn't get every attribute for Customer tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildHistoryTuple(const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                           byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                           const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // H_C_ID = C_ID
    auto col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_C_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = c_id;

    // H_C_D_ID = D_ID
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_C_D_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // H_C_W_ID = W_ID
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_C_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // H_D_ID = D_ID
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_D_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // H_W_ID = W_ID
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // H_DATE current date and time
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_DATE", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<uint64_t *>(attr) = Timestamp();

    // H_AMOUNT = 10.00
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_AMOUNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 10.0;

    // H_DATA random a-string [12 .. 24]
    col_oid = history_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(history_schema_->GetColumn(col_offset - 1).GetName() == "H_DATA", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(12, 24, false);

    TERRIER_ASSERT(col_offset == 8, "Didn't get every attribute for History tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildNewOrderTuple(const int32_t o_id, const int32_t d_id, const int32_t w_id,
                                            byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(o_id >= 2101 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // NO_O_ID = O_ID
    auto col_oid = new_order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(new_order_schema_->GetColumn(col_offset - 1).GetName() == "NO_O_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = o_id;

    // NO_D_ID = D_ID
    col_oid = new_order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(new_order_schema_->GetColumn(col_offset - 1).GetName() == "NO_D_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // NO_W_ID = W_ID
    col_oid = new_order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(new_order_schema_->GetColumn(col_offset - 1).GetName() == "NO_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    TERRIER_ASSERT(col_offset == 3, "Didn't get every attribute for New Order tuple.");

    return pr;
  }

  struct OrderTupleResults {
    storage::ProjectedRow *const pr;
    uint64_t o_entry_d;
    int8_t o_ol_cnt;
  };

  // 4.3.3.1
  OrderTupleResults BuildOrderTuple(const int32_t o_id, const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                    byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                    const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // O_ID unique within [3,000]
    auto col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = o_id;

    // O_D_ID = D_ID
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_D_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // O_W_ID = W_ID
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // O_C_ID selected sequentially from a random permutation of [1 .. 3,000]
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_C_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = c_id;

    // O_ENTRY_D current date/ time given by the operating system
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_ENTRY_D", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    const uint64_t entry_d = Timestamp();
    *reinterpret_cast<uint64_t *>(attr) = entry_d;

    // O_CARRIER_ID random within [1 .. 10] if O_ID < 2,101, null otherwise
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_CARRIER_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<int32_t *>(attr) = RandomWithin<int32_t>(1, 10, 0);
    } else {
      pr->SetNull(attr_offset);
    }

    // O_OL_CNT random within [5 .. 15]
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_OL_CNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    const int32_t ol_cnt = RandomWithin<int32_t>(5, 15, 0);
    *reinterpret_cast<int32_t *>(attr) = ol_cnt;

    // O_ALL_LOCAL = 1
    col_oid = order_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_schema_->GetColumn(col_offset - 1).GetName() == "O_ALL_LOCAL", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = 1;

    TERRIER_ASSERT(col_offset == 8, "Didn't get every attribute for Order tuple.");

    return {pr, entry_d, static_cast<int8_t>(ol_cnt)};
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildOrderLineTuple(const int32_t o_id, const int32_t d_id, const int32_t w_id,
                                             const int32_t ol_number, const uint64_t o_entry_d, byte *const buffer,
                                             const storage::ProjectedRowInitializer &pr_initializer,
                                             const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // OL_O_ID = O_ID
    auto col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_O_ID", "Wrong attribute.");
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = o_id;

    // OL_D_ID = D_ID
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_D_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // OL_W_ID = W_ID
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // OL_NUMBER unique within [O_OL_CNT]
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_NUMBER", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = ol_number;

    // OL_I_ID random within [1 .. 100,000]
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_I_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = RandomWithin<int32_t>(1, 100000, 0);

    // OL_SUPPLY_W_ID = W_ID
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_SUPPLY_W_ID", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // OL_DELIVERY_D = O_ENTRY_D if OL_O_ID < 2,101, null otherwise
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_DELIVERY_D", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<uint64_t *>(attr) = o_entry_d;
    } else {
      pr->SetNull(attr_offset);
    }

    // OL_QUANTITY = 5
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_QUANTITY", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = 5;

    // OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_AMOUNT", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = o_id < 2101 ? 0.0 : RandomWithin<double>(1, 999999, 2);

    // OL_DIST_INFO random a-string of 24 letters
    col_oid = order_line_schema_->GetColumn(col_offset++).GetOid();
    TERRIER_ASSERT(order_line_schema_->GetColumn(col_offset - 1).GetName() == "OL_DIST_INFO", "Wrong attribute.");
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(24, 24, false);

    TERRIER_ASSERT(col_offset == 10, "Didn't get every attribute for Order Line tuple.");

    return pr;
  }

  void PopulateTables() {
    TERRIER_ASSERT(txn_manager_ != nullptr, "TransactionManager does not exist.");

    // Item
    const auto item_col_oids = AllColOidsForSchema(*item_schema_);
    const auto item_pr_initializer = item_->InitializerForProjectedRow(item_col_oids).first;
    const auto item_pr_map = item_->InitializerForProjectedRow(item_col_oids).second;
    auto *const item_buffer(common::AllocationUtil::AllocateAligned(item_pr_initializer.ProjectedRowSize()));

    // Warehouse
    const auto warehouse_col_oids = AllColOidsForSchema(*warehouse_schema_);
    const auto warehouse_pr_initializer = warehouse_->InitializerForProjectedRow(warehouse_col_oids).first;
    const auto warehouse_pr_map = warehouse_->InitializerForProjectedRow(warehouse_col_oids).second;
    auto *const warehouse_buffer(common::AllocationUtil::AllocateAligned(warehouse_pr_initializer.ProjectedRowSize()));

    // Stock
    const auto stock_col_oids = AllColOidsForSchema(*stock_schema_);
    const auto stock_pr_initializer = stock_->InitializerForProjectedRow(stock_col_oids).first;
    const auto stock_pr_map = stock_->InitializerForProjectedRow(stock_col_oids).second;
    auto *const stock_buffer(common::AllocationUtil::AllocateAligned(stock_pr_initializer.ProjectedRowSize()));

    // District
    const auto district_col_oids = AllColOidsForSchema(*district_schema_);
    const auto district_pr_initializer = district_->InitializerForProjectedRow(district_col_oids).first;
    const auto district_pr_map = district_->InitializerForProjectedRow(district_col_oids).second;
    auto *const district_buffer(common::AllocationUtil::AllocateAligned(district_pr_initializer.ProjectedRowSize()));

    // Customer
    const auto customer_col_oids = AllColOidsForSchema(*customer_schema_);
    const auto customer_pr_initializer = customer_->InitializerForProjectedRow(customer_col_oids).first;
    const auto customer_pr_map = customer_->InitializerForProjectedRow(customer_col_oids).second;
    auto *const customer_buffer(common::AllocationUtil::AllocateAligned(customer_pr_initializer.ProjectedRowSize()));

    // History
    const auto history_col_oids = AllColOidsForSchema(*history_schema_);
    const auto history_pr_initializer = history_->InitializerForProjectedRow(history_col_oids).first;
    const auto history_pr_map = history_->InitializerForProjectedRow(history_col_oids).second;
    auto *const history_buffer(common::AllocationUtil::AllocateAligned(history_pr_initializer.ProjectedRowSize()));

    // Order
    const auto order_col_oids = AllColOidsForSchema(*order_schema_);
    const auto order_pr_initializer = order_->InitializerForProjectedRow(order_col_oids).first;
    const auto order_pr_map = order_->InitializerForProjectedRow(order_col_oids).second;
    auto *const order_buffer(common::AllocationUtil::AllocateAligned(order_pr_initializer.ProjectedRowSize()));

    // New Order
    const auto new_order_col_oids = AllColOidsForSchema(*new_order_schema_);
    const auto new_order_pr_initializer = new_order_->InitializerForProjectedRow(new_order_col_oids).first;
    const auto new_order_pr_map = new_order_->InitializerForProjectedRow(new_order_col_oids).second;
    auto *const new_order_buffer(common::AllocationUtil::AllocateAligned(new_order_pr_initializer.ProjectedRowSize()));

    // Order Line
    const auto order_line_col_oids = AllColOidsForSchema(*order_line_schema_);
    const auto order_line_pr_initializer = order_line_->InitializerForProjectedRow(order_line_col_oids).first;
    const auto order_line_pr_map = order_line_->InitializerForProjectedRow(order_line_col_oids).second;
    auto *const order_line_buffer(
        common::AllocationUtil::AllocateAligned(order_line_pr_initializer.ProjectedRowSize()));

    auto *const txn = txn_manager_->BeginTransaction();

    // generate booleans to represent ORIGINAL for item and stock. 10% are ORIGINAL (true), and then shuffled
    std::vector<bool> original;
    original.reserve(100000);
    for (uint32_t i_id = 0; i_id < 100000; i_id++) {
      original.emplace_back(i_id < 10000);
    }
    std::shuffle(original.begin(), original.end(), *generator_);

    for (uint32_t i_id = 0; i_id < 100000; i_id++) {
      // 100,000 rows in the ITEM table
      item_->Insert(txn, *BuildItemTuple(i_id + 1, original[i_id], item_buffer, item_pr_initializer, item_pr_map));
    }

    for (uint32_t w_id = 0; w_id < num_warehouses_; w_id++) {
      // 1 row in the WAREHOUSE table for each configured warehouse
      warehouse_->Insert(txn,
                         *BuildWarehouseTuple(w_id + 1, warehouse_buffer, warehouse_pr_initializer, warehouse_pr_map));

      // shuffle the ORIGINAL vector again since we reuse it for stock table
      std::shuffle(original.begin(), original.end(), *generator_);

      for (uint32_t s_i_id = 0; s_i_id < 100000; s_i_id++) {
        // For each row in the WAREHOUSE table:
        // 100,000 rows in the STOCK table
        stock_->Insert(txn, *BuildStockTuple(s_i_id + 1, w_id + 1, original[s_i_id], stock_buffer, stock_pr_initializer,
                                             stock_pr_map));
      }

      for (uint32_t d_id = 0; d_id < num_districts_per_warehouse_; d_id++) {
        // For each row in the WAREHOUSE table:
        // 10 rows in the DISTRICT table
        district_->Insert(
            txn, *BuildDistrictTuple(d_id + 1, w_id + 1, district_buffer, district_pr_initializer, district_pr_map));

        // O_C_ID selected sequentially from a random permutation of [1 .. 3,000] for Order table
        std::vector<uint32_t> o_c_ids;
        o_c_ids.reserve(num_customers_per_district_);
        // generate booleans to represent GC or BC for customers. 90% are GC (true), and then shuffled
        std::vector<bool> c_credit;
        c_credit.reserve(num_customers_per_district_);
        for (uint32_t c_id = 0; c_id < num_customers_per_district_; c_id++) {
          c_credit.emplace_back(c_id < num_customers_per_district_ / 10);
          o_c_ids.emplace_back(c_id + 1);
        }
        std::shuffle(c_credit.begin(), c_credit.end(), *generator_);
        std::shuffle(o_c_ids.begin(), o_c_ids.end(), *generator_);

        for (uint32_t c_id = 0; c_id < num_customers_per_district_; c_id++) {
          // For each row in the DISTRICT table:
          // 3,000 rows in the CUSTOMER table
          customer_->Insert(txn, *BuildCustomerTuple(c_id + 1, d_id + 1, w_id + 1, c_credit[c_id], customer_buffer,
                                                     customer_pr_initializer, customer_pr_map));

          // For each row in the CUSTOMER table:
          // 1 row in the HISTORY table
          history_->Insert(txn, *BuildHistoryTuple(c_id + 1, d_id + 1, w_id + 1, history_buffer, history_pr_initializer,
                                                   history_pr_map));

          // For each row in the DISTRICT table:
          // 3,000 rows in the ORDER table
          const auto o_id = c_id;
          const auto order_results = BuildOrderTuple(o_id + 1, o_c_ids[c_id], d_id + 1, w_id + 1, order_buffer,
                                                     order_pr_initializer, order_pr_map);
          order_->Insert(txn, *(order_results.pr));

          // For each row in the ORDER table:
          // A number of rows in the ORDER-LINE table equal to O_OL_CNT, generated according to the rules for input
          // data generation of the New-Order transaction (see Clause 2.4.1)
          for (int8_t ol_number = 0; ol_number < order_results.o_ol_cnt; ol_number++) {
            order_line_->Insert(
                txn, *BuildOrderLineTuple(o_id + 1, d_id + 1, w_id + 1, ol_number + 1, order_results.o_entry_d,
                                          order_line_buffer, order_line_pr_initializer, order_line_pr_map));
          }

          // For each row in the DISTRICT table:
          // 900 rows in the NEW-ORDER table corresponding to the last 900 rows in the ORDER table for that district
          // (i.e., with NO_O_ID between 2,101 and 3,000)
          if (o_id + 1 >= 2101) {
            new_order_->Insert(txn, *BuildNewOrderTuple(o_id + 1, d_id + 1, w_id + 1, new_order_buffer,
                                                        new_order_pr_initializer, new_order_pr_map));
          }
        }
      }
    }

    txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    delete[] item_buffer;
    delete[] warehouse_buffer;
    delete[] stock_buffer;
    delete[] district_buffer;
    delete[] customer_buffer;
    delete[] history_buffer;
    delete[] order_buffer;
    delete[] new_order_buffer;
    delete[] order_line_buffer;
  }

  uint64_t GetNewOid() { return ++oid_counter; }

  uint64_t oid_counter = 0;

  uint32_t num_warehouses_ = 4;  // TODO(Matt): don't hardcode this

  storage::SqlTable *item_ = nullptr;
  catalog::Schema *item_schema_ = nullptr;
  storage::SqlTable *warehouse_ = nullptr;
  catalog::Schema *warehouse_schema_ = nullptr;
  storage::SqlTable *stock_ = nullptr;
  catalog::Schema *stock_schema_ = nullptr;
  storage::SqlTable *district_ = nullptr;
  catalog::Schema *district_schema_ = nullptr;
  storage::SqlTable *customer_ = nullptr;
  catalog::Schema *customer_schema_ = nullptr;
  storage::SqlTable *history_ = nullptr;
  catalog::Schema *history_schema_ = nullptr;
  storage::SqlTable *new_order_ = nullptr;
  catalog::Schema *new_order_schema_ = nullptr;
  storage::SqlTable *order_ = nullptr;
  catalog::Schema *order_schema_ = nullptr;
  storage::SqlTable *order_line_ = nullptr;
  catalog::Schema *order_line_schema_ = nullptr;

  transaction::TransactionManager *const txn_manager_;
  storage::BlockStore *const store_;
  Random *const generator_;
};

}  // namespace terrier
