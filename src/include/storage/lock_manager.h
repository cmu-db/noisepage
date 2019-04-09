#pragma once

#include <memory>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "common/shared_latch.h"
#include "common/spin_latch.h"

namespace terrier::storage {
/**
 * A lock manager maintains table locks of sql tables. It is only effective when there are
 * concurrent transactions with creating index. The shared/exclusive lock is used in the
 * lock manager and the semantics of the lock is
 *
 * Add index:            acquire exclusive lock
 * Insert/Update/Delete:å acquire shared lock
 * Read:                 bypass lock manager
 */
class LockManager {
 public:
  /**
   * Acquire a shared lock on the table with oid.
   *
   * @param oid the object id of the target table
   */
  void LockSharedTable(catalog::table_oid_t oid);

  /**
   * Acquire an exclusive lock on the table with oid.
   *
   * @param oid the object id of the target table
   */
  void LockExclusiveTable(catalog::table_oid_t oid);

  /**
   * Release the lock held on the table with oid.
   *
   * @param oid
   */
  void ReleaseLock(catalog::table_oid_t oid);

 private:
  std::unordered_map<catalog::table_oid_t, std::unique_ptr<common::SharedLatch>> lock_table_;
  mutable common::SpinLatch latch_;

  /**
   * Get the shared/exclusive lock of the target table in the lock table. If there is no
   * corresponding lock, create a new one.
   *
   * @param oid
   */
  common::SharedLatch *GetLock(catalog::table_oid_t oid);
};
}  // namespace terrier::storage
