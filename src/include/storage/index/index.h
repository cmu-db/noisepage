#pragma once

#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

class Index {
 private:
  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;

 protected:
  Index(const catalog::index_oid_t oid, const ConstraintType constraint_type)
      : oid_{oid}, constraint_type_{constraint_type} {}

 public:
  virtual ~Index() = default;

  virtual bool Insert(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool Delete(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool ConditionalInsert(const ProjectedRow &tuple, TupleSlot location,
                                 std::function<bool(const void *)> predicate) = 0;

  ConstraintType GetConstraintType() const { return constraint_type_; }
  catalog::index_oid_t GetOid() const { return oid_; }
};

class Builder {
 private:
  catalog::index_oid_t index_oid_;
  ConstraintType constraint_type_;
  std::vector<catalog::col_oid_t> col_oids_;
  const SqlTable *sql_table_;

 public:
  Builder() = default;

  Index *Build() const { return nullptr; }

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

}  // namespace terrier::storage::index
