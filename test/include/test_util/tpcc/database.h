#pragma once

#include <utility>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "storage/index/index.h"

namespace terrier::storage {
class SqlTable;
}  // namespace terrier::storage

namespace terrier::tpcc {

/*
 * Contains all of the tables, indexes, and associated schemas for the TPC-C benchmark. This is effectively a
 * replacement for not having a catalog. Created by the Builder class.
 */
class Database {
 public:
  const catalog::Schema item_schema_;
  const catalog::Schema warehouse_schema_;
  const catalog::Schema stock_schema_;
  const catalog::Schema district_schema_;
  const catalog::Schema customer_schema_;
  const catalog::Schema history_schema_;
  const catalog::Schema new_order_schema_;
  const catalog::Schema order_schema_;
  const catalog::Schema order_line_schema_;

  const common::ManagedPointer<storage::SqlTable> item_table_;
  const common::ManagedPointer<storage::SqlTable> warehouse_table_;
  const common::ManagedPointer<storage::SqlTable> stock_table_;
  const common::ManagedPointer<storage::SqlTable> district_table_;
  const common::ManagedPointer<storage::SqlTable> customer_table_;
  const common::ManagedPointer<storage::SqlTable> history_table_;
  const common::ManagedPointer<storage::SqlTable> new_order_table_;
  const common::ManagedPointer<storage::SqlTable> order_table_;
  const common::ManagedPointer<storage::SqlTable> order_line_table_;

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

  const common::ManagedPointer<storage::index::Index> item_primary_index_;
  const common::ManagedPointer<storage::index::Index> warehouse_primary_index_;
  const common::ManagedPointer<storage::index::Index> stock_primary_index_;
  const common::ManagedPointer<storage::index::Index> district_primary_index_;
  const common::ManagedPointer<storage::index::Index> customer_primary_index_;
  const common::ManagedPointer<storage::index::Index> customer_secondary_index_;
  const common::ManagedPointer<storage::index::Index> new_order_primary_index_;
  const common::ManagedPointer<storage::index::Index> order_primary_index_;
  const common::ManagedPointer<storage::index::Index> order_secondary_index_;
  const common::ManagedPointer<storage::index::Index> order_line_primary_index_;

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

  const catalog::index_oid_t warehouse_primary_index_oid_;
  const catalog::index_oid_t district_primary_index_oid_;
  const catalog::index_oid_t customer_primary_index_oid_;
  const catalog::index_oid_t customer_secondary_index_oid_;
  const catalog::index_oid_t new_order_primary_index_oid_;
  const catalog::index_oid_t order_primary_index_oid_;
  const catalog::index_oid_t order_secondary_index_oid_;
  const catalog::index_oid_t order_line_primary_index_oid_;
  const catalog::index_oid_t item_primary_index_oid_;
  const catalog::index_oid_t stock_primary_index_oid_;

 private:
  friend class Builder;

  Database(
      catalog::Schema item_schema, catalog::Schema warehouse_schema, catalog::Schema stock_schema,
      catalog::Schema district_schema, catalog::Schema customer_schema, catalog::Schema history_schema,
      catalog::Schema new_order_schema, catalog::Schema order_schema, catalog::Schema order_line_schema,

      const common::ManagedPointer<storage::SqlTable> item, const common::ManagedPointer<storage::SqlTable> warehouse,
      const common::ManagedPointer<storage::SqlTable> stock, const common::ManagedPointer<storage::SqlTable> district,
      const common::ManagedPointer<storage::SqlTable> customer, const common::ManagedPointer<storage::SqlTable> history,
      const common::ManagedPointer<storage::SqlTable> new_order, const common::ManagedPointer<storage::SqlTable> order,
      const common::ManagedPointer<storage::SqlTable> order_line,

      catalog::IndexSchema item_primary_index_schema, catalog::IndexSchema warehouse_primary_index_schema,
      catalog::IndexSchema stock_primary_index_schema, catalog::IndexSchema district_primary_index_schema,
      catalog::IndexSchema customer_primary_index_schema, catalog::IndexSchema customer_secondary_index_schema,
      catalog::IndexSchema new_order_primary_index_schema, catalog::IndexSchema order_primary_index_schema,
      catalog::IndexSchema order_secondary_index_schema, catalog::IndexSchema order_line_primary_index_schema,

      const common::ManagedPointer<storage::index::Index> item_index,
      const common::ManagedPointer<storage::index::Index> warehouse_index,
      const common::ManagedPointer<storage::index::Index> stock_index,
      const common::ManagedPointer<storage::index::Index> district_index,
      const common::ManagedPointer<storage::index::Index> customer_index,
      const common::ManagedPointer<storage::index::Index> customer_name_index,
      const common::ManagedPointer<storage::index::Index> new_order_index,
      const common::ManagedPointer<storage::index::Index> order_index,
      const common::ManagedPointer<storage::index::Index> order_secondary_index,
      const common::ManagedPointer<storage::index::Index> order_line_index,

      const catalog::db_oid_t db_oid,

      const catalog::table_oid_t item_table_oid, const catalog::table_oid_t warehouse_table_oid,
      const catalog::table_oid_t stock_table_oid, const catalog::table_oid_t district_table_oid,
      const catalog::table_oid_t customer_table_oid, const catalog::table_oid_t history_table_oid,
      const catalog::table_oid_t new_order_table_oid, const catalog::table_oid_t order_table_oid,
      const catalog::table_oid_t order_line_table_oid,

      const catalog::index_oid_t warehouse_primary_index_oid, const catalog::index_oid_t district_primary_index_oid,
      const catalog::index_oid_t customer_primary_index_oid, const catalog::index_oid_t customer_secondary_index_oid,
      const catalog::index_oid_t new_order_primary_index_oid, const catalog::index_oid_t order_primary_index_oid,
      const catalog::index_oid_t order_secondary_index_oid, const catalog::index_oid_t order_line_primary_index_oid,
      const catalog::index_oid_t item_primary_index_oid, const catalog::index_oid_t stock_primary_index_oid)

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
        order_line_table_oid_(order_line_table_oid),
        warehouse_primary_index_oid_(warehouse_primary_index_oid),
        district_primary_index_oid_(district_primary_index_oid),
        customer_primary_index_oid_(customer_primary_index_oid),
        customer_secondary_index_oid_(customer_secondary_index_oid),
        new_order_primary_index_oid_(new_order_primary_index_oid),
        order_primary_index_oid_(order_primary_index_oid),
        order_secondary_index_oid_(order_secondary_index_oid),
        order_line_primary_index_oid_(order_line_primary_index_oid),
        item_primary_index_oid_(item_primary_index_oid),
        stock_primary_index_oid_(stock_primary_index_oid) {}
};

}  // namespace terrier::tpcc
