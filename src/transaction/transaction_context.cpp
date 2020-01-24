#include "transaction/transaction_context.h"

namespace terrier::transaction {
void TransactionContext::Unlink(timestamp_t oldest_txn) {
  for (auto &undo_record : undo_buffer_) {
    // It is possible for the table field to be null, for aborted transaction's last conflicting record
    DataTable *&table = undo_record.Table();
    // Each version chain needs to be traversed and truncated at most once every GC period. Check
    // if we have already visited this tuple slot; if not, proceed to prune the version chain.
    if (table != nullptr && visited_slots.insert(undo_record.Slot()).second)
      table->TruncateVersionChain(undo_record.Slot(), oldest_txn);
    // Regardless of the version chain we will need to reclaim deleted slots and any dangling pointers to varlens,
    // unless the transaction is aborted, and the record holds a version that is still visible.
    if (!Aborted()) {
      ReclaimSlotIfDeleted(&undo_record);
      ReclaimBufferIfVarlen(&undo_record);
    }
    if (observer_ != nullptr) observer_->ObserveWrite(undo_record.Slot().GetBlock());
  }
}  // namespace terrier::transaction
