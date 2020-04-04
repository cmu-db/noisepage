

#include "storage/checkpoints/checkpoint.h"
#include <common/worker_pool.h>
#include <fstream>
namespace terrier::storage{


bool Checkpoint::TakeCheckpoint(const std::string &path, catalog::db_oid_t db) {
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
  auto workload = [&](uint32_t worker_id){
    // copy contents of table to disk
    WriteToDisk(path, accessor, db);

  };
  for (auto i = 0u; i < num_threads; i++){
    thread_pool_.SubmitTask([i, &workload]{workload(i);});
  }
  thread_pool_.WaitUntilAllFinished();
  if (queue.size() > 0){
    // the table oid that failed to be backup
    return false;
  }
  return true;
}

void Checkpoint::WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor,
    catalog::db_oid_t db_oid) {
  while (queue.size() > 0){
    queue_latch.lock();
    if (queue.size() <= 0){
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

    curr_data_table->blocks_latch_.Lock();
    std::list<RawBlock *> blocks = curr_data_table->blocks_;
    curr_data_table->blocks_latch_.Unlock();
    f.open(path + out_file, std::ios::binary);

    // write # of varlen col in the table
    if (f.is_open()){
      unsigned long var_col_num = varlens.size();
      f.write(reinterpret_cast<const char *>(&var_col_num), sizeof(unsigned long));
    } else {
      // failed to copy to disk, add the table_oid back to queue
      queue_latch.lock();
      queue.push_back(curr_table_oid);
      queue_latch.unlock();
      return;
    }

    // traverse each block in the block list
    for (RawBlock *block : blocks){
      ArrowBlockMetadata &metadata = curr_data_table->accessor_.GetArrowBlockMetadata(block);

      // traverse all cols
      for (auto i = 0u; i < col_num; i++){
        col_id_t col_id = column_ids[i];
        ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);

        // if varlength or dictionary
        if (layout.IsVarlen(col_id) && !(col_info.Type() == ArrowColumnType::FIXED_LENGTH)){
          switch (col_info.Type()){
            case ArrowColumnType::GATHERED_VARLEN:{
              ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
              uint32_t value_len = varlen_col.ValuesLength();
              uint32_t offset_len = varlen_col.OffsetsLength();
              f.write(reinterpret_cast<const char *>(&col_id), sizeof(col_id_t));
              f.write(reinterpret_cast<const char *>(&offset_len), sizeof(uint32_t));
              f.write(reinterpret_cast<const char *>(&value_len), sizeof(uint32_t));
              f.write(reinterpret_cast<const char *>(varlen_col.Offsets()), offset_len*sizeof(uint64_t));
              f.write(reinterpret_cast<const char *>(varlen_col.Values()), value_len);
            }
            case ArrowColumnType::DICTIONARY_COMPRESSED:{
              uint32_t num_slots = metadata.NumRecords();
              auto indices = col_info.Indices();
              f.write(reinterpret_cast<const char *>(&col_id), sizeof(col_id_t));
              f.write(reinterpret_cast<const char *>(&num_slots), sizeof(uint32_t));
              f.write(reinterpret_cast<const char *>(indices), num_slots * sizeof(uint64_t));
              continue;
            }
            default:
              throw std::runtime_error("unexpected control flow");
          }

        }

      }
      // write block contents
      f.write(reinterpret_cast<const char *>(block->content_), sizeof(block->content_));
    }

  }
}


} //namespace terrier::storage
