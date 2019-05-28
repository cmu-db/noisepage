#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::catalog {

/**
 * Wrapper around the storage layer's SqlTable.
 */
class SqlTableRW {
 public:
  /**
   * Constructor
   * @param schema table's schema
   * @param table_oid table's oid
   * @param block_store table's blockstore
   */
  SqlTableRW(const Schema &schema, table_oid_t table_oid, storage::BlockStore *block_store) : table_oid_(table_oid) {
    for (const auto &col : schema.GetColumns()) {
      cols_.push_back(col);
    }
    schema_ = std::make_shared<Schema>(cols_);
    table_ = std::make_shared<storage::SqlTable>(block_store, *schema_, table_oid_);

    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
    }

    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }

  /**
   * Destructor
   */
  ~SqlTableRW() {
    delete pri_;
    delete pr_map_;
  }

  /**
   * Misc access.
   */
  std::shared_ptr<storage::SqlTable> GetSqlTable() { return table_; }

  /**
   * Return the oid of the sql table
   * @return table oid  row_p = table.FindRow(txn, search_vec);

   */
  catalog::table_oid_t Oid() { return table_->Oid(); }

  /**
   * Return a pointer to the projection map
   * @return pointer to the projection map
   */
  // shared ptr?
  storage::ProjectionMap *GetPRMap() { return pr_map_; }

  /**
   * @return The projected row initializer
   */
  storage::ProjectedRowInitializer *GetPRI() { return pri_; }

  /**
   * Get the offset of the column in the projection map
   * @param col_num the column number
   * @return the offset
   */
  uint16_t ColNumToOffset(int32_t col_num) { return pr_map_->at(col_oids_[col_num]); }

 private:
  // Table's oid
  catalog::table_oid_t table_oid_;
  // Sql Table
  std::shared_ptr<storage::SqlTable> table_ = nullptr;
  // Schema
  std::shared_ptr<catalog::Schema> schema_ = nullptr;
  // Columns
  std::vector<catalog::Schema::Column> cols_;
  // Column oids
  std::vector<catalog::col_oid_t> col_oids_;
  // PRI
  storage::ProjectedRowInitializer *pri_ = nullptr;
  // PR_MAP
  storage::ProjectionMap *pr_map_ = nullptr;
};
};  // namespace terrier::catalog
