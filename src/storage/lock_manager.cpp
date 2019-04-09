#include "storage/lock_manager.h"
#include <memory>

namespace terrier::storage {

common::SharedLatch *LockManager::GetLock(const catalog::table_oid_t oid) {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  if (lock_table_.find(oid) == lock_table_.end()) {
    lock_table_[oid] = std::make_unique<common::SharedLatch>();
  }

  return lock_table_.at(oid).get();
}

void LockManager::LockSharedTable(const catalog::table_oid_t oid) {
  common::SharedLatch *tableLock = GetLock(oid);
  tableLock->LockShared();
}

void LockManager::LockExclusiveTable(const catalog::table_oid_t oid) {
  common::SharedLatch *tableLock = GetLock(oid);
  tableLock->LockExclusive();
}

void LockManager::ReleaseLock(const catalog::table_oid_t oid) {
  common::SharedLatch *tableLock = GetLock(oid);
  tableLock->Unlock();
}
}  // namespace terrier::storage
