#include "transaction/transaction_context.h"

namespace terrier::transaction {
void TransactionContext::Unlink(timestamp_t oldest_txn, std::unordered_set<storage::TupleSlot> *const visited_slots) {
  for (auto &undo_record : undo_buffer_) {
    // It is possible for the table field to be null, for aborted transaction's last conflicting record
    storage::DataTable *&table = undo_record.Table();
    if (table != nullptr && visited_slots->insert(undo_record.Slot()).second)
      table->TruncateVersionChain(undo_record.Slot(), oldest_txn);
    // Regardless of the version chain we will need to reclaim deleted slots and any dangling pointers to varlens,
    // unless the transaction is aborted, and the record holds a version that is still visible.
    if (!Aborted()) {
      undo_record.ReclaimSlotIfDeleted();
      undo_record.ReclaimBufferIfVarlen(this);
    }
  }
}
}  // namespace terrier::transaction
