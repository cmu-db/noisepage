#pragma once

#include <chrono>
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

namespace terrier {

template <class Random>
class TPCC {
 public:
  explicit TPCC(transaction::TransactionManager *const txn_manager, storage::BlockStore *const store, Random *generator)
      : txn_manager_(txn_manager), store_(store), generator_(generator) {
    CreateWarehouseTable();
    PopulateWarehouseTable();
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

  int64_t Timestamp() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
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

  storage::VarlenEntry RandomAlphaNumericVarlenEntry(const uint32_t x, const uint32_t y,
                                                     const bool numeric_only) const {
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
  T RandomWithin(uint32_t x, uint32_t y, uint32_t p) const {
    return std::uniform_int_distribution(x, y)(*generator_) / static_cast<T>(std::pow(10, p));
  }

  // 4.3.2.7
  storage::VarlenEntry RandomZipVarlenEntry() const {
    auto string = RandomAlphaNumericString(4, 4, true);
    string.append("11111");
    TERRIER_ASSERT(string.length() == 9, "Wrong ZIP code length.");
    return storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(string.data()), string.length());
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildWarehouseTuple(const int32_t w_id, byte *const buffer,
                                             const storage::ProjectedRowInitializer &pr_initializer,
                                             const storage::ProjectionMap &projection_map) const {
    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // W_ID unique within [number_of_configured_warehouses]
    auto col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // W_NAME random a-string [6 .. 10]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(6, 10, false);

    // W_STREET_1 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_STREET_2 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_CITY random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // W_STATE random a-string of 2 letters
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // W_ZIP generated according to Clause 4.3.2.7
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // W_TAX random within [0.0000 .. 0.2000]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 2000, 4);

    // W_YTD = 300,000.00
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 300000;

    TERRIER_ASSERT(col_offset == 9, "Didn't get every attribute for Warehouse tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildDistrictTuple(const int32_t d_id, const int32_t w_id, byte *const buffer,
                                            const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // D_ID unique within [10]
    auto col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // D_W_ID = W_ID
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // D_NAME random a-string [6 .. 10]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(6, 10, false);

    // D_STREET_1 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_STREET_2 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_CITY random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_STATE random a-string of 2 letters
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // D_ZIP generated according to Clause 4.3.2.7
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // D_TAX random within [0.0000 .. 0.2000]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 2000, 4);

    // D_YTD = 30,000.00
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 30000;

    // D_NEXT_O_ID = 3,001
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
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
    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // C_ID unique within [3,000]
    auto col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = c_id;

    // C_D_ID = D_ID
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // C_W_ID = D_W_ID
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // C_FIRST random a-string [8 .. 16]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(8, 16, false);

    // C_MIDDLE = "OE"
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) =
        storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("OE"), 2);

    // C_LAST generated according to Clause 4.3.2.3, iterating through the range of [0 .. 999] for the first 1,000
    // customers, and generating a non-uniform random number using the function NURand(255,0,999) for each of the
    // remaining 2,000 customers. The run-time constant C (see Clause 2.1.6) used for the database population must be
    // randomly chosen independently from the test run(s).
    // TODO(Matt): not following the rule yet
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(8, 16, false);

    // C_STREET_1 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_STREET_2 random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_CITY random a-string [10 .. 20]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_STATE random a-string of 2 letters
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // C_ZIP generated according to Clause 4.3.2.7
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // C_PHONE random n-string of 16 numbers
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(16, 16, true);

    // C_SINCE date/ time given by the operating system when the CUSTOMER table was populated.
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int64_t *>(attr) = Timestamp();

    // C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) =
        good_credit ? storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("GC"), 2)
                    : storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("BC"), 2);

    // C_CREDIT_LIM = 50,000.00
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 50000;

    // C_DISCOUNT random within [0.0000 .. 0.5000]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 5000, 4);

    // C_BALANCE = -10.00
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = -10;

    // C_YTD_PAYMENT = 10.00
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 10;

    // C_PAYMENT_CNT = 1
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 1;

    // C_DELIVERY_CNT = 0
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 0;

    // C_DATA random a-string [300 .. 500]
    col_oid = warehouse_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(300, 500, false);

    TERRIER_ASSERT(col_offset == 11, "Didn't get every attribute for Customer tuple.");

    return pr;
  }

  void PopulateWarehouseTable() {
    TERRIER_ASSERT(txn_manager_ != nullptr, "TransactionManager does not exist.");
    TERRIER_ASSERT(warehouse_ != nullptr, "Warehouse table doesn't exist.");
    const auto col_oids = AllColOidsForSchema(*warehouse_schema_);
    const auto pr_initializer = warehouse_->InitializerForProjectedRow(col_oids).first;
    const auto projection_map = warehouse_->InitializerForProjectedRow(col_oids).second;

    auto *const insert_buffer(common::AllocationUtil::AllocateAligned(pr_initializer.ProjectedRowSize()));

    auto *const txn = txn_manager_->BeginTransaction();
    for (uint32_t w_id = 0; w_id < num_warehouses_; w_id++) {
      warehouse_->Insert(txn, *BuildWarehouseTuple(w_id, insert_buffer, pr_initializer, projection_map));
    }
    txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    delete[] insert_buffer;
  }

  uint64_t GetNewOid() { return ++oid_counter; }

  uint64_t oid_counter = 0;

  uint32_t num_warehouses_ = 10;  // TODO(Matt): don't hardcode this

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

  transaction::TransactionManager *const txn_manager_;
  storage::BlockStore *const store_;
  Random *const generator_;
};

}  // namespace terrier