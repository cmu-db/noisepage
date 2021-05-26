#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/hash_util.h"

namespace noisepage::selfdriving::pilot {
using db_table_oid_pair = std::pair<catalog::db_oid_t, catalog::table_oid_t>;

/** Hasher for db_table_oid_pair used in STL containers */
struct DBTableOidPairHasher {
  /** @brief Hash operator */
  size_t operator()(db_table_oid_pair const &v) const {
    return common::HashUtil::CombineHashes(common::HashUtil::Hash(v.first), common::HashUtil::Hash(v.second));
  }
};

/** Stores the information related to table and index memory consumption and the table growth ratios */
struct MemoryInfo {
  /** The tables that we track memory with */
  std::vector<db_table_oid_pair> table_oids_;

  /** <segment index, <db and table id, ratio between the predicted table size and the original table size>> */
  std::unordered_map<uint64_t, std::unordered_map<db_table_oid_pair, double, DBTableOidPairHasher>>
      segment_table_size_ratios_;

  /** <db and table id, table heap usage in bytes> */
  std::unordered_map<db_table_oid_pair, size_t, DBTableOidPairHasher> table_memory_bytes_;

  /** <db and table id, <index name, index heap usage in bytes>> */
  std::unordered_map<db_table_oid_pair, std::unordered_map<std::string, size_t>, DBTableOidPairHasher>
      table_index_memory_bytes_;

  /** Memory consumption at the start of the planning */
  size_t initial_memory_bytes_;
};

}  // namespace noisepage::selfdriving::pilot
