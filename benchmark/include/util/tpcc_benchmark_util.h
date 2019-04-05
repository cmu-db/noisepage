#pragma once

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
#include "type/type_id.h"
#include "util/random_test_util.h"

namespace terrier {

template <class Random>
class TPCC {
 public:
  explicit TPCC(storage::BlockStore *const store, Random *generator) : store_(store), generator_(generator) {
    CreateWarehouseTable();
    PopulateWarehouseTable(3);
    //    CreateDistrictTable();
    //    CreateCustomerTable();
  }

  ~TPCC() {
    delete warehouse_;
    delete warehouse_schema_;
    //    delete district_;
    //    delete customer_;
    //    delete history_;
    //    delete new_order_;
    //    delete order_;
    //    delete order_line_;
    //    delete item_;
    //    delete stock_;
  }

 private:
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
    TERRIER_ASSERT(warehouse_columns.size() == 9, "Wrong number of columns.");
    warehouse_schema_ = new catalog::Schema(warehouse_columns);
  }
  //
  //  catalog::Schema CreateDistrictSchema() {
  //    std::vector<catalog::Schema::Column> district_columns;
  //    district_columns.reserve(11);
  //    district_columns.emplace_back("D_ID", type::TypeId::INTEGER, false,
  //    static_cast<catalog::col_oid_t>(GetNewOid())); district_columns.emplace_back("D_W_ID", type::TypeId::INTEGER,
  //    false, static_cast<catalog::col_oid_t>(GetNewOid())); district_columns.emplace_back("D_NAME",
  //    type::TypeId::VARCHAR, 10, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    district_columns.emplace_back("D_STREET_1", type::TypeId::VARCHAR, 20, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    district_columns.emplace_back("D_STREET_2", type::TypeId::VARCHAR, 20, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    district_columns.emplace_back("D_CITY", type::TypeId::VARCHAR, 20, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    district_columns.emplace_back("D_STATE", type::TypeId::VARCHAR, 2, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    district_columns.emplace_back("D_ZIP", type::TypeId::VARCHAR, 9, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    district_columns.emplace_back("D_TAX", type::TypeId::DECIMAL, false,
  //    static_cast<catalog::col_oid_t>(GetNewOid())); district_columns.emplace_back("D_YTD", type::TypeId::DECIMAL,
  //    false, static_cast<catalog::col_oid_t>(GetNewOid())); district_columns.emplace_back("D_NEXT_O_ID",
  //    type::TypeId::INTEGER, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    TERRIER_ASSERT(district_columns.size() == 11, "Wrong number of columns.");
  //    return catalog::Schema(district_columns);
  //  }
  //
  //  catalog::Schema CreateCustomerSchema() {
  //    std::vector<catalog::Schema::Column> customer_columns;
  //    customer_columns.reserve(21);
  //    customer_columns.emplace_back("C_ID", type::TypeId::INTEGER, false,
  //    static_cast<catalog::col_oid_t>(GetNewOid())); customer_columns.emplace_back("C_D_ID", type::TypeId::INTEGER,
  //    false, static_cast<catalog::col_oid_t>(GetNewOid())); customer_columns.emplace_back("C_W_ID",
  //    type::TypeId::INTEGER, false, static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_FIRST", type::TypeId::VARCHAR, 16, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_MIDDLE", type::TypeId::VARCHAR, 2, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_LAST", type::TypeId::VARCHAR, 16, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_STREET_1", type::TypeId::VARCHAR, 20, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_STREET_2", type::TypeId::VARCHAR, 20, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_CITY", type::TypeId::VARCHAR, 20, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_STATE", type::TypeId::VARCHAR, 2, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_ZIP", type::TypeId::VARCHAR, 9, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_PHONE", type::TypeId::VARCHAR, 16, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_SINCE", type::TypeId::TIMESTAMP, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_CREDIT", type::TypeId::VARCHAR, 2, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_CREDIT_LIM", type::TypeId::DECIMAL, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_DISCOUNT", type::TypeId::DECIMAL, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_BALANCE", type::TypeId::DECIMAL, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_YTD_PAYMENT", type::TypeId::DECIMAL, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_PAYMENT_CNT", type::TypeId::SMALLINT, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_DELIVERY_CNT", type::TypeId::SMALLINT, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    customer_columns.emplace_back("C_DATA", type::TypeId::VARCHAR, 500, false,
  //                                  static_cast<catalog::col_oid_t>(GetNewOid()));
  //    TERRIER_ASSERT(customer_columns.size() == 21, "Wrong number of columns.");
  //    return catalog::Schema(customer_columns);
  //  }

  void CreateWarehouseTable() {
    TERRIER_ASSERT(warehouse_ == nullptr, "Warehouse table already exists.");
    CreateWarehouseSchema();
    warehouse_ = new storage::SqlTable(store_, *warehouse_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
  }
  //
  //  void CreateDistrictTable() {
  //    TERRIER_ASSERT(district_ == nullptr, "District table already exists.");
  //    const auto district_schema = CreateDistrictSchema();
  //    district_ = new storage::SqlTable(store_, district_schema, static_cast<catalog::table_oid_t>(GetNewOid()));
  //  }
  //
  //  void CreateCustomerTable() {
  //    TERRIER_ASSERT(customer_ == nullptr, "Customer table already exists.");
  //    const auto customer_schema = CreateCustomerSchema();
  //    customer_ = new storage::SqlTable(store_, customer_schema, static_cast<catalog::table_oid_t>(GetNewOid()));
  //  }

  static std::vector<catalog::col_oid_t> AllColOidsForSchema(const catalog::Schema &schema) {
    const auto &cols = schema.GetColumns();
    std::vector<catalog::col_oid_t> col_oids;
    col_oids.reserve(cols.size());
    for (const auto &col : cols) {
      col_oids.emplace_back(col.GetOid());
    }
    return col_oids;
  }

  char RandomAlphaNumericChar(const bool numeric_only) const {
    static const char *alpha_num = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const size_t length = numeric_only ? 9 : std::strlen(alpha_num) - 1;
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

  storage::VarlenEntry RandomAlphaNumericVarlenEntry(const uint32_t x, const uint32_t y, const bool numeric_only) const {
    TERRIER_ASSERT(x <= y, "Minimum length cannot be greater than the maximum length.");
    const auto string = RandomAlphaNumericString(x, y, numeric_only);
    if (string.length() <= storage::VarlenEntry::InlineThreshold()) {
      return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(string.data()), string.length());
    }

    auto *const varlen = common::AllocationUtil::AllocateAligned(string.length());
    std::memcpy(varlen, string.data(), string.length());
    return storage::VarlenEntry::Create(varlen, string.length(), true);
  }

  // 4.3.2.5
  template <typename T>
  T RandomWithin(uint32_t x, uint32_t y, uint32_t p) {
    return std::uniform_int_distribution(x, y)(*generator_) / static_cast<T>(std::pow(10, p));
  }

  // 4.3.2.7
  storage::VarlenEntry RandomZipVarlenEntry() const {
    auto string = RandomAlphaNumericString(4,4, true);
    string.append("11111");
    TERRIER_ASSERT(string.length() == 9, "Wrong ZIP code length.");
    return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(string.data()), string.length());
  }

  void PopulateWarehouseTable(const int32_t w_id) {
    TERRIER_ASSERT(warehouse_ != nullptr, "Warehouse table doesn't exist.");
    const auto col_oids = AllColOidsForSchema(*warehouse_schema_);
    const auto pr_initializer = warehouse_->InitializerForProjectedRow(col_oids).first;
    const auto projection_map = warehouse_->InitializerForProjectedRow(col_oids).second;

    auto *const insert_buffer(common::AllocationUtil::AllocateAligned(pr_initializer.ProjectedRowSize()));
    auto *const insert_pr = pr_initializer.InitializeRow(insert_buffer);

    // W_ID
    auto col_oid = warehouse_schema_->GetColumn(0).GetOid();
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // W_NAME
    col_oid = warehouse_schema_->GetColumn(1).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(6, 10, false);

    // W_STREET_1
    col_oid = warehouse_schema_->GetColumn(2).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_STREET_2
    col_oid = warehouse_schema_->GetColumn(3).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_CITY
    col_oid = warehouse_schema_->GetColumn(4).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_STATE
    col_oid = warehouse_schema_->GetColumn(5).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // W_ZIP
    col_oid = warehouse_schema_->GetColumn(6).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // W_TAX
    col_oid = warehouse_schema_->GetColumn(7).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 2000, 4);

    // W_YTD
    col_oid = warehouse_schema_->GetColumn(8).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = insert_pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 300000;

    delete[] insert_buffer;
  }

  uint64_t GetNewOid() { return ++oid_counter; }

  uint64_t oid_counter = 0;

  storage::SqlTable *warehouse_ = nullptr;
  catalog::Schema *warehouse_schema_ = nullptr;
  //  storage::SqlTable *district_ = nullptr;
  //  storage::SqlTable *customer_ = nullptr;
  //  storage::SqlTable *history_ = nullptr;
  //  storage::SqlTable *new_order_ = nullptr;
  //  storage::SqlTable *order_ = nullptr;
  //  storage::SqlTable *order_line_ = nullptr;
  //  storage::SqlTable *item_ = nullptr;
  //  storage::SqlTable *stock_ = nullptr;

  storage::BlockStore *const store_;
  Random *const generator_;
};

}  // namespace terrier