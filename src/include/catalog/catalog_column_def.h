#pragma once

#include "catalog/catalog_defs.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {

/**
 * The definition of a column in the catalog.
 *
 * This class exists to prevent a class of bugs like #1322, of which one was resolved in #1337.
 * Those bugs existed because the user of a raw ProjectedRow API is required to constantly be correct in their
 * knowledge and usage of what the underlying storage type and high level catalog types are.
 *
 * @warning Until builder.cpp is refactored into non-existence, the definitions of CreateCatalogColumnDef may still be
 *          out of sync with what builder.cpp is declaring. This can lead to mysterious bugs, which will primarily
 *          manifest over SQL as corruption of nearby column values or strange values being read out of columns.
 *
 * @tparam CatalogType  The high-level type that the catalog wants to get out of the column.
 * @tparam StorageType  The low-level storage type that the column's values are actually stored as.
 *                      This defaults to CatalogType to simplify writing templates for common use cases.
 */
template <typename CatalogType, typename StorageType = CatalogType>
class CatalogColumnDef {
 public:
  /** Create a new catalog column definition. */
  constexpr explicit CatalogColumnDef(const col_oid_t col_oid) : oid_(col_oid) {}

  /** @see ProjectedRow::SetNull */
  void SetNull(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset) const { pr->SetNull(offset); }

  /** @see ProjectedRow::SetNotNull */
  void SetNotNull(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset) const {
    pr->SetNotNull(offset);
  }

  /** @see ProjectedRow::SetNull */
  void SetNull(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm) const {
    pr->SetNull(pm.at(oid_));
  }

  /** @see ProjectedRow::SetNotNull */
  void SetNotNull(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm) const {
    pr->SetNotNull(pm.at(oid_));
  }

  /** @see ProjectedRow::Set */
  void Set(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm, CatalogType val) const {
    if constexpr (std::is_pointer_v<CatalogType>) {
      pr->Set<StorageType, false>(pm.at(oid_), reinterpret_cast<StorageType>(val), false);
    } else {  // NOLINT (for some reason, clang-tidy sees this as misleading indentation)
      pr->Set<StorageType, false>(pm.at(oid_), static_cast<StorageType>(val), false);
    }
  }

  /** @see ProjectedRow::Set */
  void Set(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset, CatalogType val) const {
    if constexpr (std::is_pointer_v<CatalogType>) {
      pr->Set<StorageType, false>(offset, reinterpret_cast<StorageType>(val), false);
    } else {  // NOLINT (for some reason, clang-tidy won't complain here, but gets VERY confused without this)
      pr->Set<StorageType, false>(offset, static_cast<StorageType>(val), false);
    }
  }

  /** @see ProjectedRow::Get */
  const CatalogType *Get(common::ManagedPointer<storage::ProjectedRow> pr, const storage::ProjectionMap &pm) const {
    return reinterpret_cast<const CatalogType *>(pr->Get<StorageType, false>(pm.at(oid_), nullptr));
  }

  /** @see ProjectedRow::Get */
  const CatalogType *Get(common::ManagedPointer<storage::ProjectedRow> pr, const uint16_t offset) const {
    return reinterpret_cast<const CatalogType *>(pr->Get<StorageType, false>(offset, nullptr));
  }

  const col_oid_t oid_;  ///< The OID of the column.
};

}  // namespace noisepage::catalog
