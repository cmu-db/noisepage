

#include "storage/checkpoints/checkpoint.h"

namespace terrier::storage{

void Checkpoint::take_checkpoint(const std::string &path) {
  // TODO (Xuanxuan): fake code for build purpose, change later
  catalog::db_oid_t db_;
  auto accessor = catalog_->GetAccessor(txn_, db_);
  block_store_->Get();
}

bool Checkpoint::write_to_disk(const std::string &path) {
  return true;
}



} //namespace terrier::storage
