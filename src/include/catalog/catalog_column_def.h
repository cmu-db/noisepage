#pragma once

#include "catalog/catalog_defs.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {

template <typename CatalogType, typename StorageType = CatalogType>
class CatalogColumnDef {
 public:
  constexpr explicit CatalogColumnDef(const col_oid_t col_oid) : oid_(col_oid) {}

  void Set(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm, CatalogType val) const {
    pr->Set<StorageType, false>(pm.at(oid_), static_cast<StorageType>(val), false);
  }

  void Set(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset, CatalogType val) const {
    pr->Set<StorageType, false>(offset, static_cast<StorageType>(val), false);
  }

  CatalogType Get(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm) const {
    return static_cast<CatalogType>(*pr->Get<StorageType, false>(pm.at(oid_), nullptr));
  }

  CatalogType Get(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset) const {
    return static_cast<CatalogType>(*pr->Get<StorageType, false>(offset, nullptr));
  }

  const col_oid_t oid_;
};



}  // namespace noisepage::catalog