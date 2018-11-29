#pragma once

#include <vector>
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

// clang-format off
#define IndexCounterMembers(f) \
  f(uint64_t, NumSelect) \
  f(uint64_t, NumUpdate) \
  f(uint64_t, NumInsert) \
  f(uint64_t, NumDelete) \
  f(uint64_t, NumNewBlock)
// clang-format on
DEFINE_PERFORMANCE_CLASS(IndexCounter, IndexCounterMembers)
#undef IndexCounterMembers

class BwTreeIndex {
 private:
  BwTreeIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type)
      : oid_{oid}, constraint_type_{constraint_type} {}
  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;

 public:
  class Builder {
   private:
    catalog::index_oid_t index_oid_;
    ConstraintType constraint_type_;
    std::vector<catalog::col_oid_t> col_oids_;
    const SqlTable *sql_table_;

   public:
    Builder() = default;

    void Build() const { auto stuffs = sql_table_->InitializerForProjectedRow(col_oids_); }

    Builder &SetOid(const catalog::index_oid_t index_oid) {
      index_oid_ = index_oid;
      return *this;
    }

    Builder &SetConstraintType(const ConstraintType constraint_type) {
      constraint_type_ = constraint_type;
      return *this;
    }

    Builder &SetColOids(const std::vector<catalog::col_oid_t> &col_oids) {
      col_oids_ = col_oids;
      return *this;
    }

    Builder &SetSqlTable(const SqlTable *const sql_table) {
      sql_table_ = sql_table;
      return *this;
    }
  };  // class Builder

  ConstraintType GetConstraintType() const { return constraint_type_; }
  catalog::index_oid_t GetOid() const { return oid_; }
};

}  // namespace terrier::storage::index
