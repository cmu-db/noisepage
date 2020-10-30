#include "storage/undo_record.h"

#include "storage/data_table.h"
#include "transaction/transaction_context.h"
namespace noisepage::storage {

void UndoRecord::ReclaimSlotIfDeleted() const {
  if (type_ == DeltaRecordType::DELETE) table_->accessor_.Deallocate(slot_);
}

void UndoRecord::ReclaimBufferIfVarlen(transaction::TransactionContext *const txn) const {
  const TupleAccessStrategy &accessor = table_->accessor_;
  const BlockLayout &layout = table_->GetBlockLayout();
  switch (type_) {
    case DeltaRecordType::INSERT:
      return;  // no possibility of outdated varlen to gc
    case DeltaRecordType::DELETE:
      for (uint16_t i = 0; i < layout.NumColumns(); i++) {
        col_id_t col_id(i);
        // Okay to include version vector, as it is never varlen
        if (layout.IsVarlen(col_id)) {
          auto *varlen = reinterpret_cast<VarlenEntry *>(accessor.AccessWithNullCheck(slot_, col_id));
          if (varlen != nullptr && varlen->NeedReclaim()) txn->AddReclaimableVarlen(varlen->Content());
        } else if (i > 0) {
          break;  // Once done with varlens we won't see them again.
        }
      }
      break;
    case DeltaRecordType::UPDATE: {
      const ProjectedRow *delta = this->Delta();
      for (uint16_t i = 0; i < delta->NumColumns(); i++) {
        col_id_t col_id = UndoRecord::Delta()->ColumnIds()[i];
        if (layout.IsVarlen(col_id)) {
          auto *varlen = reinterpret_cast<const VarlenEntry *>(delta->AccessWithNullCheck(i));
          if (varlen != nullptr && varlen->NeedReclaim()) txn->AddReclaimableVarlen(varlen->Content());
        } else if (i > 0) {
          break;  // Once done with varlens we won't see them again.
        }
      }
      break;
    }
    default:
      throw std::runtime_error("unexpected delta record type");
  }
}

}  // namespace noisepage::storage
