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
  explicit TPCC(transaction::TransactionManager *const txn_manager, storage::BlockStore *const store,
                Random *const generator)
      : txn_manager_(txn_manager), store_(store), generator_(generator) {
    CreateWarehouseTable();
    CreateDistrictTable();
    CreateCustomerTable();

    PopulateTables();
  }

  ~TPCC() {
    delete warehouse_;
    delete warehouse_schema_;
    delete district_;
    delete district_schema_;
    delete customer_;
    delete customer_schema_;
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

    TERRIER_ASSERT(warehouse_columns.size() == 9, "Wrong number of columns for Warehouse schema.");

    warehouse_schema_ = new catalog::Schema(warehouse_columns);
  }

  void CreateDistrictSchema() {
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

  void CreateWarehouseTable() {
    TERRIER_ASSERT(warehouse_ == nullptr, "Warehouse table already exists.");
    CreateWarehouseSchema();
    warehouse_ = new storage::SqlTable(store_, *warehouse_schema_, static_cast<catalog::table_oid_t>(GetNewOid()));
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
    auto col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // D_W_ID = W_ID
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // D_NAME random a-string [6 .. 10]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(6, 10, false);

    // D_STREET_1 random a-string [10 .. 20]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_STREET_2 random a-string [10 .. 20]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_CITY random a-string [10 .. 20]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // D_STATE random a-string of 2 letters
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // D_ZIP generated according to Clause 4.3.2.7
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // D_TAX random within [0.0000 .. 0.2000]
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 2000, 4);

    // D_YTD = 30,000.00
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 30000;

    // D_NEXT_O_ID = 3,001
    col_oid = district_schema_->GetColumn(col_offset++).GetOid();
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
    TERRIER_ASSERT(c_id >= 0 && c_id < 3000, "Invalid c_id for BuildCustomerTuple().");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // C_ID unique within [3,000]
    auto col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    auto attr_offset = projection_map.at(col_oid);
    auto *attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = c_id;

    // C_D_ID = D_ID
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = d_id;

    // C_W_ID = D_W_ID
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int32_t *>(attr) = w_id;

    // C_FIRST random a-string [8 .. 16]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(8, 16, false);

    // C_MIDDLE = "OE"
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) =
        storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("OE"), 2);

    // C_LAST generated according to Clause 4.3.2.3, iterating through the range of [0 .. 999] for the first 1,000
    // customers, and generating a non-uniform random number using the function NURand(255,0,999) for each of the
    // remaining 2,000 customers. The run-time constant C (see Clause 2.1.6) used for the database population must be
    // randomly chosen independently from the test run(s).
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    if (c_id < 1000) {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomLastNameVarlenEntry(c_id);
    } else {
      *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomLastNameVarlenEntry(NURand(255, 0, 999));
    }

    // C_STREET_1 random a-string [10 .. 20]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_STREET_2 random a-string [10 .. 20]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_CITY random a-string [10 .. 20]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(10, 20, false);

    // C_STATE random a-string of 2 letters
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(2, 2, false);

    // C_ZIP generated according to Clause 4.3.2.7
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomZipVarlenEntry();

    // C_PHONE random n-string of 16 numbers
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(16, 16, true);

    // C_SINCE date/ time given by the operating system when the CUSTOMER table was populated.
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int64_t *>(attr) = Timestamp();

    // C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) =
        good_credit ? storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("GC"), 2)
                    : storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("BC"), 2);

    // C_CREDIT_LIM = 50,000.00
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 50000;

    // C_DISCOUNT random within [0.0000 .. 0.5000]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = RandomWithin<double>(0, 5000, 4);

    // C_BALANCE = -10.00
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = -10;

    // C_YTD_PAYMENT = 10.00
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<double *>(attr) = 10;

    // C_PAYMENT_CNT = 1
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 1;

    // C_DELIVERY_CNT = 0
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<int16_t *>(attr) = 0;

    // C_DATA random a-string [300 .. 500]
    col_oid = customer_schema_->GetColumn(col_offset++).GetOid();
    attr_offset = projection_map.at(col_oid);
    attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<storage::VarlenEntry *>(attr) = RandomAlphaNumericVarlenEntry(300, 500, false);

    TERRIER_ASSERT(col_offset == 21, "Didn't get every attribute for Customer tuple.");

    return pr;
  }

  void PopulateTables() {
    TERRIER_ASSERT(txn_manager_ != nullptr, "TransactionManager does not exist.");

    // Warehouse
    const auto warehouse_col_oids = AllColOidsForSchema(*warehouse_schema_);
    const auto warehouse_pr_initializer = warehouse_->InitializerForProjectedRow(warehouse_col_oids).first;
    const auto warehouse_pr_map = warehouse_->InitializerForProjectedRow(warehouse_col_oids).second;
    auto *const warehouse_buffer(common::AllocationUtil::AllocateAligned(warehouse_pr_initializer.ProjectedRowSize()));

    // District
    const auto district_col_oids = AllColOidsForSchema(*district_schema_);
    const auto district_pr_initializer = district_->InitializerForProjectedRow(district_col_oids).first;
    const auto district_pr_map = district_->InitializerForProjectedRow(district_col_oids).second;
    auto *const district_buffer(common::AllocationUtil::AllocateAligned(district_pr_initializer.ProjectedRowSize()));

    // District
    const auto customer_col_oids = AllColOidsForSchema(*customer_schema_);
    const auto customer_pr_initializer = customer_->InitializerForProjectedRow(customer_col_oids).first;
    const auto customer_pr_map = customer_->InitializerForProjectedRow(customer_col_oids).second;
    auto *const customer_buffer(common::AllocationUtil::AllocateAligned(customer_pr_initializer.ProjectedRowSize()));

    auto *const txn = txn_manager_->BeginTransaction();

    for (uint32_t w_id = 0; w_id < num_warehouses_; w_id++) {
      warehouse_->Insert(txn, *BuildWarehouseTuple(w_id, warehouse_buffer, warehouse_pr_initializer, warehouse_pr_map));

      for (uint32_t d_id = 0; d_id < 10; d_id++) {
        district_->Insert(txn,
                          *BuildDistrictTuple(d_id, w_id, district_buffer, district_pr_initializer, district_pr_map));

        // generate booleans to represent GC or BC for customers. 90% are GC (true), and then shuffled
        std::vector<bool> c_credit;
        c_credit.reserve(3000);
        for (uint32_t c_id = 0; c_id < 3000; c_id++) {
          c_credit.emplace_back(c_id < 300);
        }
        std::shuffle(c_credit.begin(), c_credit.end(), *generator_);

        for (uint32_t c_id = 0; c_id < 3000; c_id++) {
          customer_->Insert(txn, *BuildCustomerTuple(c_id, d_id, w_id, c_credit[c_id], customer_buffer,
                                                     customer_pr_initializer, customer_pr_map));
        }
      }
    }

    txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    delete[] warehouse_buffer;
  }

  uint64_t GetNewOid() { return ++oid_counter; }

  uint64_t oid_counter = 0;

  uint32_t num_warehouses_ = 10;  // TODO(Matt): don't hardcode this

  storage::SqlTable *warehouse_ = nullptr;
  catalog::Schema *warehouse_schema_ = nullptr;
  storage::SqlTable *district_ = nullptr;
  catalog::Schema *district_schema_ = nullptr;
  storage::SqlTable *customer_ = nullptr;
  catalog::Schema *customer_schema_ = nullptr;
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