#pragma once

#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "common/macros.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"

namespace terrier::storage {
class SqlTable;
}  // namespace terrier::storage

namespace terrier::catalog {
class Schema;
}

namespace terrier::tpcc {

/*
 * Contains all of the tables, indexes, and associated schemas for the TPC-C benchmark. This is effectively a
 * replacement for not having a catalog. Created by the Builder class.
 */
class Database {
 public:
  ~Database() {
    delete item_table_;
    delete warehouse_table_;
    delete stock_table_;
    delete district_table_;
    delete customer_table_;
    delete history_table_;
    delete new_order_table_;
    delete order_table_;
    delete order_line_table_;

    delete item_primary_index_;
    delete warehouse_primary_index_;
    delete stock_primary_index_;
    delete district_primary_index_;
    delete customer_primary_index_;
    delete customer_secondary_index_;
    delete new_order_primary_index_;
    delete order_primary_index_;
    delete order_secondary_index_;
    delete order_line_primary_index_;
  }

  const catalog::Schema item_schema_;
  const catalog::Schema warehouse_schema_;
  const catalog::Schema stock_schema_;
  const catalog::Schema district_schema_;
  const catalog::Schema customer_schema_;
  const catalog::Schema history_schema_;
  const catalog::Schema new_order_schema_;
  const catalog::Schema order_schema_;
  const catalog::Schema order_line_schema_;

  storage::SqlTable *const item_table_;
  storage::SqlTable *const warehouse_table_;
  storage::SqlTable *const stock_table_;
  storage::SqlTable *const district_table_;
  storage::SqlTable *const customer_table_;
  storage::SqlTable *const history_table_;
  storage::SqlTable *const new_order_table_;
  storage::SqlTable *const order_table_;
  storage::SqlTable *const order_line_table_;

  const catalog::IndexSchema item_primary_index_schema_;
  const catalog::IndexSchema warehouse_primary_index_schema_;
  const catalog::IndexSchema stock_primary_index_schema_;
  const catalog::IndexSchema district_primary_index_schema_;
  const catalog::IndexSchema customer_primary_index_schema_;
  const catalog::IndexSchema customer_secondary_index_schema_;
  const catalog::IndexSchema new_order_primary_index_schema_;
  const catalog::IndexSchema order_primary_index_schema_;
  const catalog::IndexSchema order_secondary_index_schema_;
  const catalog::IndexSchema order_line_primary_index_schema_;

  storage::index::Index *const item_primary_index_;
  storage::index::Index *const warehouse_primary_index_;
  storage::index::Index *const stock_primary_index_;
  storage::index::Index *const district_primary_index_;
  storage::index::Index *const customer_primary_index_;
  storage::index::Index *const customer_secondary_index_;
  storage::index::Index *const new_order_primary_index_;
  storage::index::Index *const order_primary_index_;
  storage::index::Index *const order_secondary_index_;
  storage::index::Index *const order_line_primary_index_;

  const catalog::db_oid_t db_oid_;

  const catalog::table_oid_t item_table_oid_;
  const catalog::table_oid_t warehouse_table_oid_;
  const catalog::table_oid_t stock_table_oid_;
  const catalog::table_oid_t district_table_oid_;
  const catalog::table_oid_t customer_table_oid_;
  const catalog::table_oid_t history_table_oid_;
  const catalog::table_oid_t new_order_table_oid_;
  const catalog::table_oid_t order_table_oid_;
  const catalog::table_oid_t order_line_table_oid_;

 private:
  friend class Builder;

  Database(catalog::Schema item_schema, catalog::Schema warehouse_schema, catalog::Schema stock_schema,
           catalog::Schema district_schema, catalog::Schema customer_schema, catalog::Schema history_schema,
           catalog::Schema new_order_schema, catalog::Schema order_schema, catalog::Schema order_line_schema,

           storage::SqlTable *const item, storage::SqlTable *const warehouse, storage::SqlTable *const stock,
           storage::SqlTable *const district, storage::SqlTable *const customer, storage::SqlTable *const history,
           storage::SqlTable *const new_order, storage::SqlTable *const order, storage::SqlTable *const order_line,

           catalog::IndexSchema item_primary_index_schema, catalog::IndexSchema warehouse_primary_index_schema,
           catalog::IndexSchema stock_primary_index_schema, catalog::IndexSchema district_primary_index_schema,
           catalog::IndexSchema customer_primary_index_schema, catalog::IndexSchema customer_secondary_index_schema,
           catalog::IndexSchema new_order_primary_index_schema, catalog::IndexSchema order_primary_index_schema,
           catalog::IndexSchema order_secondary_index_schema, catalog::IndexSchema order_line_primary_index_schema,

           storage::index::Index *const item_index, storage::index::Index *const warehouse_index,
           storage::index::Index *const stock_index, storage::index::Index *const district_index,
           storage::index::Index *const customer_index, storage::index::Index *const customer_name_index,
           storage::index::Index *const new_order_index, storage::index::Index *const order_index,
           storage::index::Index *const order_secondary_index, storage::index::Index *const order_line_index,

           const catalog::db_oid_t db_oid,

           const catalog::table_oid_t item_table_oid, const catalog::table_oid_t warehouse_table_oid,
           const catalog::table_oid_t stock_table_oid, const catalog::table_oid_t district_table_oid,
           const catalog::table_oid_t customer_table_oid, const catalog::table_oid_t history_table_oid,
           const catalog::table_oid_t new_order_table_oid, const catalog::table_oid_t order_table_oid,
           const catalog::table_oid_t order_line_table_oid)

      : item_schema_(std::move(item_schema)),
        warehouse_schema_(std::move(warehouse_schema)),
        stock_schema_(std::move(stock_schema)),
        district_schema_(std::move(district_schema)),
        customer_schema_(std::move(customer_schema)),
        history_schema_(std::move(history_schema)),
        new_order_schema_(std::move(new_order_schema)),
        order_schema_(std::move(order_schema)),
        order_line_schema_(std::move(order_line_schema)),
        item_table_(item),
        warehouse_table_(warehouse),
        stock_table_(stock),
        district_table_(district),
        customer_table_(customer),
        history_table_(history),
        new_order_table_(new_order),
        order_table_(order),
        order_line_table_(order_line),
        item_primary_index_schema_(std::move(item_primary_index_schema)),
        warehouse_primary_index_schema_(std::move(warehouse_primary_index_schema)),
        stock_primary_index_schema_(std::move(stock_primary_index_schema)),
        district_primary_index_schema_(std::move(district_primary_index_schema)),
        customer_primary_index_schema_(std::move(customer_primary_index_schema)),
        customer_secondary_index_schema_(std::move(customer_secondary_index_schema)),
        new_order_primary_index_schema_(std::move(new_order_primary_index_schema)),
        order_primary_index_schema_(std::move(order_primary_index_schema)),
        order_secondary_index_schema_(std::move(order_secondary_index_schema)),
        order_line_primary_index_schema_(std::move(order_line_primary_index_schema)),
        item_primary_index_(item_index),
        warehouse_primary_index_(warehouse_index),
        stock_primary_index_(stock_index),
        district_primary_index_(district_index),
        customer_primary_index_(customer_index),
        customer_secondary_index_(customer_name_index),
        new_order_primary_index_(new_order_index),
        order_primary_index_(order_index),
        order_secondary_index_(order_secondary_index),
        order_line_primary_index_(order_line_index),
        db_oid_(db_oid),
        item_table_oid_(item_table_oid),
        warehouse_table_oid_(warehouse_table_oid),
        stock_table_oid_(stock_table_oid),
        district_table_oid_(district_table_oid),
        customer_table_oid_(customer_table_oid),
        history_table_oid_(history_table_oid),
        new_order_table_oid_(new_order_table_oid),
        order_table_oid_(order_table_oid),
        order_line_table_oid_(order_line_table_oid) {}
};

}  // namespace terrier::tpcc
