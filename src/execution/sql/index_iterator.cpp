#include "execution/sql/index_iterator.h"
#include "execution/sql/value.h"

namespace tpl::sql {

IndexIterator::IndexIterator(uint32_t table_oid, uint32_t index_oid, exec::ExecutionContext *exec_ctx)
    : exec_ctx_(exec_ctx) {
  // TODO(Amadou): Use the catalog accessor once it's merged
  // Get index from the catalog
  catalog_index_ = exec_ctx_->GetAccessor()->GetCatalogIndex(terrier::catalog::index_oid_t(index_oid));
  // Get table from the catalog
  catalog_table_ = exec_ctx_->GetAccessor()->GetUserTable(terrier::catalog::table_oid_t(table_oid));
  // Initialize projected rows for the index and the table
  auto &index_pri = catalog_index_->GetMetadata()->GetProjectedRowInitializer();
  auto &row_pri = *catalog_table_->GetPRI();
  index_buffer_ = terrier::common::AllocationUtil::AllocateAligned(index_pri.ProjectedRowSize());
  row_buffer_ = terrier::common::AllocationUtil::AllocateAligned(row_pri.ProjectedRowSize());
  index_pr_ = index_pri.InitializeRow(index_buffer_);
  row_pr_ = row_pri.InitializeRow(row_buffer_);
}

IndexIterator::~IndexIterator() {
  // Free allocated buffers
  delete[] index_buffer_;
  delete[] row_buffer_;
}

void IndexIterator::ScanKey(byte *sql_key) {
  auto *metadata = catalog_index_->GetMetadata();
  uint16_t col_idx = 0;
  // Construct an index projected row from the sql types
  for (const auto &index_col : metadata->GetKeySchema()) {
    switch (index_col.GetType()) {
      case TypeId::TINYINT:
      case TypeId::SMALLINT:
      case TypeId::INTEGER:
      case TypeId::BIGINT: {
        auto val = reinterpret_cast<sql::Integer *>(sql_key);
        if (index_col.IsNullable() && val->is_null) {
          index_pr_->SetNull(col_idx);
        } else {
          // std::cout << "ScanKey val=" << val->val << std::endl;
          auto index_data = index_pr_->AccessForceNotNull(col_idx);
          std::memcpy(index_data, &val->val, terrier::type::TypeUtil::GetTypeSize(index_col.GetType()));
        }
        break;
      }
      default: { UNREACHABLE("Index type not yet implemented"); }
    }
    sql_key += ValUtil::GetSqlSize(index_col.GetType());
    col_idx++;
  }
  // Scan the index
  index_values_.clear();
  catalog_index_->GetIndex()->ScanKey(*exec_ctx_->GetTxn(), *index_pr_, &index_values_);
  // FOR DEBUGGING ONLY: print to check if output is correct.
  /*if (!index_values_.empty()) std::cout << "Scan key rows:" << std::endl;
  for (const auto &slot : index_values_) {
    catalog_table_->GetSqlTable()->Select(txn_, slot, row_pr_);
    for (uint16_t i = 0; i < row_pr_->NumColumns(); i++) {
      uint16_t offset = catalog_table_->ColNumToOffset(i);
      byte *result = row_pr_->AccessWithNullCheck(offset);
      if (result == nullptr) {
        std::cout << "NULL";
      } else {
        std::cout << *reinterpret_cast<i16 *>(result);
      }
      std::cout << "(" << offset << "); " << std::endl;
    }
    std::cout << std::endl;
  }*/
}

bool IndexIterator::Advance() {
  if (curr_index_ < index_values_.size()) {
    catalog_table_->GetSqlTable()->Select(exec_ctx_->GetTxn(), index_values_[curr_index_], row_pr_);
    ++curr_index_;
    return true;
  }
  return false;
}

}  // namespace tpl::sql
