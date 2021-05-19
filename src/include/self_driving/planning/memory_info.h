#pragma once

#include "catalog/catalog_defs.h"

namespace noisepage::selfdriving::pilot {
/** Stores the information related to table and index memory consumption and the table growth ratios */
struct MemoryInfo {
  // The tables that we track memory with
  std::vector<catalog::table_oid_t> table_oids_;

  // <segment index, <table id, ratio between the predicted table size and the original table size>>
  std::unordered_map<uint64_t, std::unordered_map<catalog::table_oid_t, double>> segment_table_size_ratios_;

  // <table id, table heap usage in bytes>
  std::unordered_map<catalog::table_oid_t, size_t> table_memory_bytes_;

  // <table id, <index name, index heap usage in bytes>>
  std::unordered_map<catalog::table_oid_t, std::unordered_map<std::string, size_t>> table_index_memory_bytes_;

  // Memory consumption at the start of the planning
  size_t initial_memory_bytes_;
};

}  // namespace noisepage::selfdriving::pilot
