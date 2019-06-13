#include "transaction/transaction_context.h"
#include "storage/write_ahead_log/log_record.h"

namespace terrier::transaction {
storage::RedoRecord *TransactionContext::StageWrite(const catalog::db_oid_t db_oid,
                                                    const catalog::table_oid_t table_oid,
                                                    const storage::ProjectedRowInitializer &initializer) {
  const uint32_t size = storage::RedoRecord::Size(initializer);
  auto *const log_record =
      storage::RedoRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, db_oid, table_oid, initializer);
  return log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
}

void TransactionContext::StageDelete(const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                                     const storage::TupleSlot slot) {
  const uint32_t size = storage::DeleteRecord::Size();
  storage::DeleteRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, db_oid, table_oid, slot);
}

}  // namespace terrier::transaction
