#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/index/index_defs.h"

namespace noisepage::tpcc {

/**
 * Schemas is a utility class that defines all 9 table schemas and 8 index schemas according to section 1.3 of the TPC-C
 * specification. It also defines 2 secondary indexes that improve query performance.
 */
class Schemas {
 public:
  Schemas() = delete;

  /**
   * @param oid_counter global OID counter to be incremented
   * @return Warehouse table schema
   */
  static catalog::Schema BuildWarehouseTableSchema() {
    std::vector<catalog::Schema::Column> warehouse_columns;
    warehouse_columns.reserve(NUM_WAREHOUSE_TABLE_COLS);

    // 2*W unique IDs
    warehouse_columns.emplace_back("w_id", execution::sql::SqlTypeId::TinyInt, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // variable text, size 10
    warehouse_columns.emplace_back("w_name", execution::sql::SqlTypeId::Varchar, 10, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    warehouse_columns.emplace_back("w_street_1", execution::sql::SqlTypeId::Varchar, 20, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    warehouse_columns.emplace_back("w_street_2", execution::sql::SqlTypeId::Varchar, 20, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    warehouse_columns.emplace_back("w_city", execution::sql::SqlTypeId::Varchar, 20, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 2
    warehouse_columns.emplace_back("w_state", execution::sql::SqlTypeId::Varchar, 2, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 9
    warehouse_columns.emplace_back("w_zip", execution::sql::SqlTypeId::Varchar, 9, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // signed numeric(4,4)
    warehouse_columns.emplace_back("w_tax", execution::sql::SqlTypeId::Double, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // signed numeric(12,2)
    warehouse_columns.emplace_back("w_ytd", execution::sql::SqlTypeId::Double, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));

    NOISEPAGE_ASSERT(warehouse_columns.size() == NUM_WAREHOUSE_TABLE_COLS,
                     "Wrong number of columns for Warehouse table schema.");

    return catalog::Schema(warehouse_columns);
  }

  /**
   * @param schema Warehouse table schema
   * @param oid_counter global OID counter to be incremented
   * @return Warehouse primary index schema
   */
  static catalog::IndexSchema BuildWarehousePrimaryIndexSchema(const catalog::Schema &schema,
                                                               const storage::index::IndexType index_type,
                                                               const catalog::db_oid_t db_oid,
                                                               const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> warehouse_key_schema;
    warehouse_key_schema.reserve(NUM_WAREHOUSE_PRIMARY_INDEX_COLS);

    // Primary Key: W_ID
    warehouse_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                      schema.GetColumn(0).Nullable(),
                                      parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(warehouse_key_schema.size() == NUM_WAREHOUSE_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for Warehouse primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(warehouse_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return District table schema
   */
  static catalog::Schema BuildDistrictTableSchema() {
    std::vector<catalog::Schema::Column> district_columns;
    district_columns.reserve(NUM_DISTRICT_TABLE_COLS);

    // 20 unique IDs
    district_columns.emplace_back("d_id", execution::sql::SqlTypeId::TinyInt, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    district_columns.emplace_back("d_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // variable text, size 10
    district_columns.emplace_back("d_name", execution::sql::SqlTypeId::Varchar, 10, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    district_columns.emplace_back("d_street_1", execution::sql::SqlTypeId::Varchar, 20, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    district_columns.emplace_back("d_street_2", execution::sql::SqlTypeId::Varchar, 20, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    district_columns.emplace_back("d_city", execution::sql::SqlTypeId::Varchar, 20, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 2
    district_columns.emplace_back("d_state", execution::sql::SqlTypeId::Varchar, 2, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 9
    district_columns.emplace_back("d_zip", execution::sql::SqlTypeId::Varchar, 9, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // signed numeric(4,4)
    district_columns.emplace_back("d_tax", execution::sql::SqlTypeId::Double, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // signed numeric(12,2)
    district_columns.emplace_back("d_ytd", execution::sql::SqlTypeId::Double, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // 10,000,000 unique IDs
    district_columns.emplace_back("d_next_o_id", execution::sql::SqlTypeId::Integer, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));

    NOISEPAGE_ASSERT(district_columns.size() == NUM_DISTRICT_TABLE_COLS,
                     "Wrong number of columns for District table schema.");

    return catalog::Schema(district_columns);
  }

  /**
   * @param schema District table schema
   * @param oid_counter global OID counter to be incremented
   * @return District primary index schema
   */
  static catalog::IndexSchema BuildDistrictPrimaryIndexSchema(const catalog::Schema &schema,
                                                              const storage::index::IndexType index_type,
                                                              const catalog::db_oid_t db_oid,
                                                              const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> district_key_schema;
    district_key_schema.reserve(NUM_DISTRICT_PRIMARY_INDEX_COLS);

    // Primary Key: (D_W_ID, D_ID)
    district_key_schema.emplace_back(schema.GetColumn(1).Name(), schema.GetColumn(1).Type(),
                                     schema.GetColumn(1).Nullable(),
                                     parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    district_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                     schema.GetColumn(0).Nullable(),
                                     parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(district_key_schema.size() == NUM_DISTRICT_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for District primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(district_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return Customer table schema
   */
  static catalog::Schema BuildCustomerTableSchema() {
    std::vector<catalog::Schema::Column> customer_columns;
    customer_columns.reserve(NUM_CUSTOMER_TABLE_COLS);

    // 96,000 unique IDs
    customer_columns.emplace_back("c_id", execution::sql::SqlTypeId::Integer, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 20 unique IDs
    customer_columns.emplace_back("c_d_id", execution::sql::SqlTypeId::TinyInt, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    customer_columns.emplace_back("c_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // variable text, size 16
    customer_columns.emplace_back("c_first", execution::sql::SqlTypeId::Varchar, 16, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 2
    customer_columns.emplace_back("c_middle", execution::sql::SqlTypeId::Varchar, 2, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 16
    customer_columns.emplace_back("c_last", execution::sql::SqlTypeId::Varchar, 16, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    customer_columns.emplace_back("c_street_1", execution::sql::SqlTypeId::Varchar, 20, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    customer_columns.emplace_back("c_street_2", execution::sql::SqlTypeId::Varchar, 20, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // variable text, size 20
    customer_columns.emplace_back("c_city", execution::sql::SqlTypeId::Varchar, 20, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 2
    customer_columns.emplace_back("c_state", execution::sql::SqlTypeId::Varchar, 2, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 9
    customer_columns.emplace_back("c_zip", execution::sql::SqlTypeId::Varchar, 9, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 16
    customer_columns.emplace_back("c_phone", execution::sql::SqlTypeId::Varchar, 16, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // date and time
    customer_columns.emplace_back("c_since", execution::sql::SqlTypeId::Timestamp, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Timestamp));
    // fixed text, size 2
    customer_columns.emplace_back("c_credit", execution::sql::SqlTypeId::Varchar, 2, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // signed numeric(12,2)
    customer_columns.emplace_back("c_credit_lim", execution::sql::SqlTypeId::Double, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // signed numeric(4,4)
    customer_columns.emplace_back("c_discount", execution::sql::SqlTypeId::Double, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // signed numeric(12,2)
    customer_columns.emplace_back("c_balance", execution::sql::SqlTypeId::Double, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // signed numeric(12,2)
    customer_columns.emplace_back("c_ytd_payment", execution::sql::SqlTypeId::Double, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // numeric(4)
    customer_columns.emplace_back("c_payment_cnt", execution::sql::SqlTypeId::SmallInt, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::SmallInt));
    // numeric(4)
    customer_columns.emplace_back("c_delivery_cnt", execution::sql::SqlTypeId::SmallInt, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::SmallInt));
    // variable text, size 500
    customer_columns.emplace_back("c_data", execution::sql::SqlTypeId::Varchar, 500, false,
                                  parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));

    NOISEPAGE_ASSERT(customer_columns.size() == NUM_CUSTOMER_TABLE_COLS,
                     "Wrong number of columns for Customer table schema.");

    return catalog::Schema(customer_columns);
  }

  /**
   * @param schema Customer table schema
   * @param oid_counter global OID counter to be incremented
   * @return Customer primary index schema
   */
  static catalog::IndexSchema BuildCustomerPrimaryIndexSchema(const catalog::Schema &schema,
                                                              const storage::index::IndexType index_type,
                                                              const catalog::db_oid_t db_oid,
                                                              const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> customer_key_schema;
    customer_key_schema.reserve(NUM_CUSTOMER_PRIMARY_INDEX_COLS);

    // Primary Key: (C_W_ID, C_D_ID, C_ID)
    customer_key_schema.emplace_back(schema.GetColumn(2).Name(), schema.GetColumn(2).Type(),
                                     schema.GetColumn(2).Nullable(),
                                     parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(2).Oid()));
    customer_key_schema.emplace_back(schema.GetColumn(1).Name(), schema.GetColumn(1).Type(),
                                     schema.GetColumn(1).Nullable(),
                                     parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    customer_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                     schema.GetColumn(0).Nullable(),
                                     parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(customer_key_schema.size() == NUM_CUSTOMER_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for Customer primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(customer_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param schema Customer table schema
   * @param oid_counter global OID counter to be incremented
   * @return Customer secondary index schema
   */
  static catalog::IndexSchema BuildCustomerSecondaryIndexSchema(const catalog::Schema &schema,
                                                                const storage::index::IndexType index_type,
                                                                const catalog::db_oid_t db_oid,
                                                                const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> customer_secondary_key_schema;
    customer_secondary_key_schema.reserve(NUM_CUSTOMER_SECONDARY_INDEX_COLS);

    // C_W_ID, C_D_ID, C_LAST for Order Status and Payment transactions
    customer_secondary_key_schema.emplace_back(
        schema.GetColumn(2).Name(), schema.GetColumn(2).Type(), schema.GetColumn(2).Nullable(),
        parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(2).Oid()));
    customer_secondary_key_schema.emplace_back(
        schema.GetColumn(1).Name(), schema.GetColumn(1).Type(), schema.GetColumn(1).Nullable(),
        parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    customer_secondary_key_schema.emplace_back(
        schema.GetColumn(5).Name(), schema.GetColumn(5).Type(), schema.GetColumn(5).TypeModifier(),
        schema.GetColumn(5).Nullable(), parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(5).Oid()));

    NOISEPAGE_ASSERT(customer_secondary_key_schema.size() == NUM_CUSTOMER_SECONDARY_INDEX_COLS,
                     "Wrong number of columns for Customer secondary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(customer_secondary_key_schema, index_type, false, false, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return History table schema
   */
  static catalog::Schema BuildHistoryTableSchema() {
    std::vector<catalog::Schema::Column> history_columns;
    history_columns.reserve(NUM_HISTORY_TABLE_COLS);

    // 96,000 unique IDs
    history_columns.emplace_back("h_c_id", execution::sql::SqlTypeId::Integer, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 20 unique IDs
    history_columns.emplace_back("h_c_d_id", execution::sql::SqlTypeId::TinyInt, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    history_columns.emplace_back("h_c_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 20 unique IDs
    history_columns.emplace_back("h_d_id", execution::sql::SqlTypeId::TinyInt, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    history_columns.emplace_back("h_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // date and time
    history_columns.emplace_back("h_date", execution::sql::SqlTypeId::Timestamp, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::Timestamp));
    // signed numeric(6,2)
    history_columns.emplace_back("h_amount", execution::sql::SqlTypeId::Double, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // variable text, size 24
    history_columns.emplace_back("h_data", execution::sql::SqlTypeId::Varchar, 24, false,
                                 parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));

    NOISEPAGE_ASSERT(history_columns.size() == NUM_HISTORY_TABLE_COLS,
                     "Wrong number of columns for History table schema.");

    return catalog::Schema(history_columns);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return New Order table schema
   */
  static catalog::Schema BuildNewOrderTableSchema() {
    std::vector<catalog::Schema::Column> new_order_columns;
    new_order_columns.reserve(NUM_NEW_ORDER_TABLE_COLS);

    // 10,000,000 unique IDs
    new_order_columns.emplace_back("no_o_id", execution::sql::SqlTypeId::Integer, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 20 unique IDs
    new_order_columns.emplace_back("no_d_id", execution::sql::SqlTypeId::TinyInt, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    new_order_columns.emplace_back("no_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                   parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));

    NOISEPAGE_ASSERT(new_order_columns.size() == NUM_NEW_ORDER_TABLE_COLS,
                     "Wrong number of columns for New Order table schema.");

    return catalog::Schema(new_order_columns);
  }

  /**
   * @param schema New Order table schema
   * @param oid_counter global OID counter to be incremented
   * @return New Order primary index schema
   */
  static catalog::IndexSchema BuildNewOrderPrimaryIndexSchema(const catalog::Schema &schema,
                                                              const storage::index::IndexType index_type,
                                                              const catalog::db_oid_t db_oid,
                                                              const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> new_order_key_schema;
    new_order_key_schema.reserve(NUM_NEW_ORDER_PRIMARY_INDEX_COLS);

    // Primary Key: (NO_W_ID, NO_D_ID, NO_O_ID)
    new_order_key_schema.emplace_back(schema.GetColumn(2).Name(), schema.GetColumn(2).Type(),
                                      schema.GetColumn(2).Nullable(),
                                      parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(2).Oid()));
    new_order_key_schema.emplace_back(schema.GetColumn(1).Name(), schema.GetColumn(1).Type(),
                                      schema.GetColumn(1).Nullable(),
                                      parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    new_order_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                      schema.GetColumn(0).Nullable(),
                                      parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(new_order_key_schema.size() == NUM_NEW_ORDER_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for New Order primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(new_order_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return Order table schema
   */
  static catalog::Schema BuildOrderTableSchema() {
    std::vector<catalog::Schema::Column> order_columns;
    order_columns.reserve(NUM_ORDER_TABLE_COLS);

    // 10,000,000 unique IDs
    order_columns.emplace_back("o_id", execution::sql::SqlTypeId::Integer, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 20 unique IDs
    order_columns.emplace_back("o_d_id", execution::sql::SqlTypeId::TinyInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    order_columns.emplace_back("o_w_id", execution::sql::SqlTypeId::TinyInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 96,000 unique IDs
    order_columns.emplace_back("o_c_id", execution::sql::SqlTypeId::Integer, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // date and time
    order_columns.emplace_back("o_entry_d", execution::sql::SqlTypeId::Timestamp, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Timestamp));
    // 10 unique IDs, or null
    order_columns.emplace_back("o_carrier_id", execution::sql::SqlTypeId::TinyInt, true,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // numeric(2)
    order_columns.emplace_back("o_ol_cnt", execution::sql::SqlTypeId::TinyInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // numeric(1)
    order_columns.emplace_back("o_all_local", execution::sql::SqlTypeId::TinyInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));

    NOISEPAGE_ASSERT(order_columns.size() == NUM_ORDER_TABLE_COLS, "Wrong number of columns for Order table schema.");

    return catalog::Schema(order_columns);
  }

  /**
   * @param schema Order table schema
   * @param oid_counter global OID counter to be incremented
   * @return Order primary index schema
   */
  static catalog::IndexSchema BuildOrderPrimaryIndexSchema(const catalog::Schema &schema,
                                                           const storage::index::IndexType index_type,
                                                           const catalog::db_oid_t db_oid,
                                                           const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> order_key_schema;
    order_key_schema.reserve(NUM_ORDER_PRIMARY_INDEX_COLS);

    // Primary Key: (O_W_ID, O_D_ID, O_ID)
    order_key_schema.emplace_back(schema.GetColumn(2).Name(), schema.GetColumn(2).Type(),
                                  schema.GetColumn(2).Nullable(),
                                  parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(2).Oid()));
    order_key_schema.emplace_back(schema.GetColumn(1).Name(), schema.GetColumn(1).Type(),
                                  schema.GetColumn(1).Nullable(),
                                  parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    order_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                  schema.GetColumn(0).Nullable(),
                                  parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(order_key_schema.size() == NUM_ORDER_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for Order primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(order_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param schema Order table schema
   * @param oid_counter global OID counter to be incremented
   * @return Order secondary index schema
   */
  static catalog::IndexSchema BuildOrderSecondaryIndexSchema(const catalog::Schema &schema,
                                                             const storage::index::IndexType index_type,
                                                             const catalog::db_oid_t db_oid,
                                                             const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> order_secondary_key_schema;
    order_secondary_key_schema.reserve(NUM_ORDER_SECONDARY_INDEX_COLS);

    // O_W_ID, O_D_ID, O_C_ID, O_ID for Order Status transaction
    order_secondary_key_schema.emplace_back(
        schema.GetColumn(2).Name(), schema.GetColumn(2).Type(), schema.GetColumn(2).Nullable(),
        parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(2).Oid()));
    order_secondary_key_schema.emplace_back(
        schema.GetColumn(1).Name(), schema.GetColumn(1).Type(), schema.GetColumn(1).Nullable(),
        parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    order_secondary_key_schema.emplace_back(
        schema.GetColumn(3).Name(), schema.GetColumn(3).Type(), schema.GetColumn(3).Nullable(),
        parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(3).Oid()));
    order_secondary_key_schema.emplace_back(
        schema.GetColumn(0).Name(), schema.GetColumn(0).Type(), schema.GetColumn(0).Nullable(),
        parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(order_secondary_key_schema.size() == NUM_ORDER_SECONDARY_INDEX_COLS,
                     "Wrong number of columns for Order secondary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(order_secondary_key_schema, index_type, true, false, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return Order Line table schema
   */
  static catalog::Schema BuildOrderLineTableSchema() {
    std::vector<catalog::Schema::Column> order_line_columns;
    order_line_columns.reserve(NUM_ORDER_LINE_TABLE_COLS);

    // 10,000,000 unique IDs
    order_line_columns.emplace_back("ol_o_id", execution::sql::SqlTypeId::Integer, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 20 unique IDs
    order_line_columns.emplace_back("ol_d_id", execution::sql::SqlTypeId::TinyInt, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 2*W unique IDs
    order_line_columns.emplace_back("ol_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 15 unique IDs
    order_line_columns.emplace_back("ol_number", execution::sql::SqlTypeId::TinyInt, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // 200,000 unique IDs
    order_line_columns.emplace_back("ol_i_id", execution::sql::SqlTypeId::Integer, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 2*W unique IDs
    order_line_columns.emplace_back("ol_supply_w_id", execution::sql::SqlTypeId::TinyInt, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // date and time, or null
    order_line_columns.emplace_back("ol_delivery_d", execution::sql::SqlTypeId::Timestamp, true,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::Timestamp));
    // numeric(2)
    order_line_columns.emplace_back("ol_quantity", execution::sql::SqlTypeId::TinyInt, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // signed numeric(6,2)
    order_line_columns.emplace_back("ol_amount", execution::sql::SqlTypeId::Double, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // fixed text, size 24
    order_line_columns.emplace_back("ol_dist_info", execution::sql::SqlTypeId::Varchar, 24, false,
                                    parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));

    NOISEPAGE_ASSERT(order_line_columns.size() == NUM_ORDER_LINE_TABLE_COLS,
                     "Wrong number of columns for Order Line table schema.");

    return catalog::Schema(order_line_columns);
  }

  /**
   * @param schema Order Line table schema
   * @param oid_counter global OID counter to be incremented
   * @return Order Line primary index schema
   */
  static catalog::IndexSchema BuildOrderLinePrimaryIndexSchema(const catalog::Schema &schema,
                                                               const storage::index::IndexType index_type,
                                                               const catalog::db_oid_t db_oid,
                                                               const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> order_line_key_schema;
    order_line_key_schema.reserve(NUM_ORDER_LINE_PRIMARY_INDEX_COLS);

    // Primary Key: (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
    order_line_key_schema.emplace_back(schema.GetColumn(2).Name(), schema.GetColumn(2).Type(),
                                       schema.GetColumn(2).Nullable(),
                                       parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(2).Oid()));
    order_line_key_schema.emplace_back(schema.GetColumn(1).Name(), schema.GetColumn(1).Type(),
                                       schema.GetColumn(1).Nullable(),
                                       parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    order_line_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                       schema.GetColumn(0).Nullable(),
                                       parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));
    order_line_key_schema.emplace_back(schema.GetColumn(3).Name(), schema.GetColumn(3).Type(),
                                       schema.GetColumn(3).Nullable(),
                                       parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(3).Oid()));

    NOISEPAGE_ASSERT(order_line_key_schema.size() == NUM_ORDER_LINE_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for Order Line key schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(order_line_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return Item table schema
   */
  static catalog::Schema BuildItemTableSchema() {
    std::vector<catalog::Schema::Column> item_columns;
    item_columns.reserve(NUM_ITEM_TABLE_COLS);

    // 200,000 unique IDs
    item_columns.emplace_back("i_id", execution::sql::SqlTypeId::Integer, false,
                              parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 200,000 unique IDs
    item_columns.emplace_back("i_im_id", execution::sql::SqlTypeId::Integer, false,
                              parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // variable text, size 24
    item_columns.emplace_back("i_name", execution::sql::SqlTypeId::Varchar, 24, false,
                              parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // numeric(5,2)
    item_columns.emplace_back("i_price", execution::sql::SqlTypeId::Double, false,
                              parser::ConstantValueExpression(execution::sql::SqlTypeId::Double));
    // variable text, size 50
    item_columns.emplace_back("i_data", execution::sql::SqlTypeId::Varchar, 50, false,
                              parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));

    NOISEPAGE_ASSERT(item_columns.size() == NUM_ITEM_TABLE_COLS, "Wrong number of columns for Item table schema.");

    return catalog::Schema(item_columns);
  }

  /**
   * @param schema Item table schema
   * @param oid_counter global OID counter to be incremented
   * @return Item primary index schema
   */
  static catalog::IndexSchema BuildItemPrimaryIndexSchema(const catalog::Schema &schema,
                                                          const storage::index::IndexType index_type,
                                                          const catalog::db_oid_t db_oid,
                                                          const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> item_key_schema;
    item_key_schema.reserve(NUM_ITEM_PRIMARY_INDEX_COLS);

    // Primary Key: I_ID
    item_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(), schema.GetColumn(0).Nullable(),
                                 parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(item_key_schema.size() == NUM_ITEM_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for Item primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(item_key_schema, index_type, true, true, false, true, options);
  }

  /**
   * @param oid_counter global OID counter to be incremented
   * @return Stock table schema
   */
  static catalog::Schema BuildStockTableSchema() {
    std::vector<catalog::Schema::Column> stock_columns;
    stock_columns.reserve(NUM_STOCK_TABLE_COLS);

    // 200,000 unique IDs
    stock_columns.emplace_back("s_i_id", execution::sql::SqlTypeId::Integer, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // 2*W unique IDs
    stock_columns.emplace_back("s_w_id", execution::sql::SqlTypeId::TinyInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::TinyInt));
    // signed numeric(4)
    stock_columns.emplace_back("s_quantity", execution::sql::SqlTypeId::SmallInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::SmallInt));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_01", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_02", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_03", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_04", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_05", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_06", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_07", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_08", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_09", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // fixed text, size 24
    stock_columns.emplace_back("s_dist_10", execution::sql::SqlTypeId::Varchar, 24, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));
    // numeric(8)
    stock_columns.emplace_back("s_ytd", execution::sql::SqlTypeId::Integer, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer));
    // numeric(4)
    stock_columns.emplace_back("s_order_cnt", execution::sql::SqlTypeId::SmallInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::SmallInt));
    // numeric(4)
    stock_columns.emplace_back("s_remote_cnt", execution::sql::SqlTypeId::SmallInt, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::SmallInt));
    // variable text, size 50
    stock_columns.emplace_back("s_data", execution::sql::SqlTypeId::Varchar, 50, false,
                               parser::ConstantValueExpression(execution::sql::SqlTypeId::Varchar));

    NOISEPAGE_ASSERT(stock_columns.size() == NUM_STOCK_TABLE_COLS, "Wrong number of columns for Stock table schema.");

    return catalog::Schema(stock_columns);
  }

  /**
   * @param schema Stock table schema
   * @param oid_counter global OID counter to be incremented
   * @return Stock primary index schema
   */
  static catalog::IndexSchema BuildStockPrimaryIndexSchema(const catalog::Schema &schema,
                                                           const storage::index::IndexType index_type,
                                                           const catalog::db_oid_t db_oid,
                                                           const catalog::table_oid_t table_oid) {
    std::vector<catalog::IndexSchema::Column> stock_key_schema;
    stock_key_schema.reserve(NUM_STOCK_PRIMARY_INDEX_COLS);

    // Primary Key: (S_W_ID, S_I_ID)
    stock_key_schema.emplace_back(schema.GetColumn(1).Name(), schema.GetColumn(1).Type(),
                                  schema.GetColumn(1).Nullable(),
                                  parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(1).Oid()));
    stock_key_schema.emplace_back(schema.GetColumn(0).Name(), schema.GetColumn(0).Type(),
                                  schema.GetColumn(0).Nullable(),
                                  parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));

    NOISEPAGE_ASSERT(stock_key_schema.size() == NUM_STOCK_PRIMARY_INDEX_COLS,
                     "Wrong number of columns for Stock primary index schema.");

    catalog::IndexOptions options;
    return catalog::IndexSchema(stock_key_schema, index_type, true, true, false, true, options);
  }

 private:
  // The values below are just to sanity check the schema functions
  static constexpr uint8_t NUM_WAREHOUSE_TABLE_COLS = 9;
  static constexpr uint8_t NUM_DISTRICT_TABLE_COLS = 11;
  static constexpr uint8_t NUM_CUSTOMER_TABLE_COLS = 21;
  static constexpr uint8_t NUM_HISTORY_TABLE_COLS = 8;
  static constexpr uint8_t NUM_NEW_ORDER_TABLE_COLS = 3;
  static constexpr uint8_t NUM_ORDER_TABLE_COLS = 8;
  static constexpr uint8_t NUM_ORDER_LINE_TABLE_COLS = 10;
  static constexpr uint8_t NUM_ITEM_TABLE_COLS = 5;
  static constexpr uint8_t NUM_STOCK_TABLE_COLS = 17;

  static constexpr uint8_t NUM_WAREHOUSE_PRIMARY_INDEX_COLS = 1;
  static constexpr uint8_t NUM_DISTRICT_PRIMARY_INDEX_COLS = 2;
  static constexpr uint8_t NUM_CUSTOMER_PRIMARY_INDEX_COLS = 3;
  static constexpr uint8_t NUM_CUSTOMER_SECONDARY_INDEX_COLS = 3;
  static constexpr uint8_t NUM_NEW_ORDER_PRIMARY_INDEX_COLS = 3;
  static constexpr uint8_t NUM_ORDER_PRIMARY_INDEX_COLS = 3;
  static constexpr uint8_t NUM_ORDER_SECONDARY_INDEX_COLS = 4;
  static constexpr uint8_t NUM_ORDER_LINE_PRIMARY_INDEX_COLS = 4;
  static constexpr uint8_t NUM_ITEM_PRIMARY_INDEX_COLS = 1;
  static constexpr uint8_t NUM_STOCK_PRIMARY_INDEX_COLS = 2;
};

}  // namespace noisepage::tpcc
