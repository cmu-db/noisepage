

#include "storage/checkpoints/checkpoint.h"
#include <common/worker_pool.h>
#include <fstream>
namespace terrier::storage {

bool Checkpoint::TakeCheckpoint(const std::string &path, catalog::db_oid_t db) {
  std::cout << "here" << std::endl;
  // get db catalog accessor
  auto accessor = catalog_->GetAccessor(txn_, db);
  std::unordered_set<catalog::table_oid_t> table_oids = accessor->GetAllTableOids();

  for (const auto &oid : table_oids) {
    queue.push_back(oid);
  }

  // initalize threads
  auto num_threads = 1u;
  common::WorkerPool thread_pool_{num_threads, {}};
  thread_pool_.Startup();
  auto workload = [&](uint32_t worker_id) {
    // copy contents of table to disk
    WriteToDisk(path, accessor, db);
  };
  for (auto i = 0u; i < num_threads; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();
  if (queue.size() > 0) {
    // the table oid that failed to be backup
    return false;
  }
  return true;
}


void Checkpoint::WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                             catalog::db_oid_t db_oid) {
  while (queue.size() > 0) {
    queue_latch.lock();
    if (queue.size() <= 0) {
      queue_latch.unlock();
      return;
    }
    catalog::table_oid_t curr_table_oid = queue[0];
    queue.erase(queue.begin());
    queue_latch.unlock();
    common::ManagedPointer<storage::SqlTable> curr_table = accessor->GetTable(curr_table_oid);
    std::string out_file = GenFileName(db_oid, curr_table_oid);
    std::ofstream f;

    auto curr_data_table = curr_table->table_.data_table_;
    const BlockLayout &layout = curr_data_table->GetBlockLayout();
    const std::vector<col_id_t> varlens = layout.Varlens();
    auto column_ids = layout.AllColumns();
    size_t col_num = column_ids.size();
    std::cout << "col num: " << col_num <<std::endl;

    curr_data_table->blocks_latch_.Lock();
    const std::list<RawBlock *> blocks = curr_data_table->blocks_;
    curr_data_table->blocks_latch_.Unlock();
    f.open(path + out_file, std::ios::binary);

    // record # of blocks in block list, # of varlen col and # of dict col in the table
    if (f.is_open()) {
      unsigned long block_num = blocks.size();
      unsigned long var_col_num = varlens.size();
      unsigned long dict_col_num = 0;
      std::cout << "block num: " << block_num <<std::endl;
      std::cout << "var col num: " << var_col_num <<std::endl;
      std::cout << "dict col num: " << dict_col_num <<std::endl;
      ArrowBlockMetadata &metadata = curr_data_table->accessor_.GetArrowBlockMetadata(blocks.front());
      //count dict_col num
      for (auto i = 0u; i < col_num; i++) {
        col_id_t col_id = column_ids[i];
        ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
        if (col_info.Type() == ArrowColumnType::DICTIONARY_COMPRESSED) {
          dict_col_num += 1;
        }
      }
      f.write(reinterpret_cast<const char *>(&block_num), sizeof(unsigned long));
      f.write(reinterpret_cast<const char *>(&var_col_num), sizeof(unsigned long));
      f.write(reinterpret_cast<const char *>(&dict_col_num), sizeof(unsigned long));
    } else {
      // failed to copy to disk, add the table_oid back to queue
      queue_latch.lock();
      queue.push_back(curr_table_oid);
      queue_latch.unlock();
      return;
    }

    // record varlen_col data for all blocks
    for (RawBlock *block : blocks) {
      ArrowBlockMetadata &metadata = curr_data_table->accessor_.GetArrowBlockMetadata(block);
      for (auto i = 0u; i < col_num; i++) {
        col_id_t col_id = column_ids[i];
        ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
        if (layout.IsVarlen(col_id) && col_info.Type() == ArrowColumnType::GATHERED_VARLEN) {
          std::cout << "varchar" << std::endl;
          ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
          uint32_t value_len = varlen_col.ValuesLength();
          uint32_t offset_len = varlen_col.OffsetsLength();
          f.write(reinterpret_cast<const char *>(&col_id), sizeof(col_id_t));
          f.write(reinterpret_cast<const char *>(&offset_len), sizeof(uint32_t));
          f.write(reinterpret_cast<const char *>(&value_len), sizeof(uint32_t));
          f.write(reinterpret_cast<const char *>(varlen_col.Offsets()), offset_len * sizeof(uint64_t));
          f.write(reinterpret_cast<const char *>(varlen_col.Values()), value_len);

        }
      }
    }

    // record all dict cols data
    for (RawBlock *block : blocks) {
      ArrowBlockMetadata &metadata = curr_data_table->accessor_.GetArrowBlockMetadata(block);
      for (auto i = 0u; i < col_num; i++) {
        col_id_t col_id = column_ids[i];
        ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
        if (layout.IsVarlen(col_id) && col_info.Type() == ArrowColumnType::DICTIONARY_COMPRESSED) {
          std::cout << "dictionary" << std::endl;
          uint32_t num_slots = metadata.NumRecords();
          auto indices = col_info.Indices();
          f.write(reinterpret_cast<const char *>(&col_id), sizeof(col_id_t));
          f.write(reinterpret_cast<const char *>(&num_slots), sizeof(uint32_t));
          f.write(reinterpret_cast<const char *>(indices), num_slots * sizeof(uint64_t));
        }
      }
    }

    // record block contents
    for (RawBlock *block : blocks) {
      f.write(reinterpret_cast<const char *>(block->content_), sizeof(block->content_));
    }


  }
}

}  // namespace terrier::storage
