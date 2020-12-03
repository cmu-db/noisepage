#pragma once

#include "catalog/catalog_defs.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {

template <typename CatalogType, typename StorageType = CatalogType>
class CatalogColumnDef {
 public:
  constexpr explicit CatalogColumnDef(const col_oid_t col_oid) : oid_(col_oid) {}

  void SetNull(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset) const {
    pr->SetNull(offset);
  }

  void SetNotNull(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset) const {
    pr->SetNotNull(offset);
  }

  void SetNull(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm) const {
    pr->SetNull(pm.at(oid_));
  }

  void SetNotNull(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm) const {
    pr->SetNotNull(pm.at(oid_));
  }

  void Set(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm, CatalogType val) const {
    if constexpr (std::is_pointer_v<CatalogType>) {
      pr->Set<StorageType, false>(pm.at(oid_), reinterpret_cast<StorageType>(val), false);
    } else {
      pr->Set<StorageType, false>(pm.at(oid_), static_cast<StorageType>(val), false);
    }
  }

  void Set(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset, CatalogType val) const {
    if constexpr (std::is_pointer_v<CatalogType>) {
      pr->Set<StorageType, false>(offset, reinterpret_cast<StorageType>(val), false);
    } else {
      pr->Set<StorageType, false>(offset, static_cast<StorageType>(val), false);
    }
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