#include "execution/sql/index_iterator.h"
#include "execution/sql/execution_structures.h"

namespace tpl::sql {

IndexIterator::IndexIterator(uint32_t index_oid,
                             TransactionContext *txn)
    : txn_(txn) {
  // Get index from the catalog
  auto *exec = ExecutionStructures::Instance();
  auto *catalog = exec->GetCatalog();
  catalog_index_ = catalog->GetCatalogIndex(terrier::catalog::index_oid_t(index_oid));
  // Get table from the catalog
  auto db_table = catalog_index_->GetTable();
  catalog_table_ = catalog->GetCatalogTable(db_table.first, db_table.second);
  // Initialize projected rows for the index and the table
  auto &index_pri = catalog_index_->GetMetadata()->GetProjectedRowInitializer();
  auto &row_pri = *catalog_table_->GetPRI();
  index_buffer_ =
      terrier::common::AllocationUtil::AllocateAligned(index_pri.ProjectedRowSize());
  row_buffer_ =
      terrier::common::AllocationUtil::AllocateAligned(row_pri.ProjectedRowSize());
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
          //std::cout << "ScanKey val=" << val->val << std::endl;
          auto index_data = index_pr_->AccessForceNotNull(col_idx);
          std::memcpy(index_data, &val->val,
                      terrier::type::TypeUtil::GetTypeSize(index_col.GetType()));
        }
        break;
      }
      default: { UNREACHABLE("Index type not yet implemented"); }
    }
    sql_key += ValUtil::GetSqlSize(index_col.GetType());
    col_idx++;
  }
  // Scan the table
  curr_index_ = 0;
  index_values_.clear();
  catalog_index_->GetIndex()->ScanKey(*index_pr_, &index_values_);
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

void IndexIterator::Advance() {
  // Select the next tuple slot
  catalog_table_->GetSqlTable()->Select(txn_, index_values_[curr_index_],
                                        row_pr_);
  ++curr_index_;
}

bool IndexIterator::HasNext() { return curr_index_ < index_values_.size(); }

}  // namespace tpl::sql