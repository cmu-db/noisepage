#include "storage/log_record.h"
#include "storage/log_manager.h"
#include "execution/sql_table.h"

namespace terrier::storage {

void RedoRecord::SerializeToLog(LogManager *manager) const {
  manager->WriteValue(LogRecordType::REDO);
  manager->WriteValue(txn_begin_);
  manager->WriteValue(table_->TableOid());
  manager->WriteValue(tuple_id_);
  // TODO(Tianyu): Deal with varlen and other non-memcpy-able things
  // This also will write all the padding out into disk, which is fine for correctness,
  // albeit not space-efficient
  manager->Write(Delta(), Delta()->Size());
}

void CommitRecord::SerializeToLog(LogManager *manager) const {
  manager->WriteValue(LogRecordType::COMMIT);
  manager->WriteValue(txn_begin_);
  manager->WriteValue(txn_commit_);
}

}  // terrier::storage