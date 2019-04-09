#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "type/transient_value.h"

namespace terrier::catalog {

/**
 * A catalog entry that is indexed by type Oid. It should *NOT* be used by views(e.g. pg_tables).
 * View catalogs require special treatments. Most catalog entries are essentially the same as this one.
 * However, we enforce differnet names for type safety.
 *
 * @tparam Oid this catalog is indexed by type Oid.
 */
template <typename Oid>
class CatalogEntry {
 public:
  /**
   * Get the a transient value of a column.
   */
  const type::TransientValue &GetColumn(int32_t col) { return entry_[col]; }
  /**
   * Get oid, e.g. tablespace_oid_t, table_oid_t.
   */
  Oid GetOid() { return oid_; }

 protected:
  /**
   * Constructor, should not be directly instantiated.
   */
  CatalogEntry(Oid oid, std::vector<type::TransientValue> &&entry) : oid_(oid), entry_(std::move(entry)) {}

 private:
  Oid oid_;
  std::vector<type::TransientValue> entry_;
};

}  // namespace terrier::catalog
