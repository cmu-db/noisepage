#include "parser/create_statement.h"

#include "common/hash_util.h"

namespace terrier::parser {

hash_t ColumnDefinition::Hash() const {
  hash_t hash = common::HashUtil::Hash(name_);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_info_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(is_primary_)));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(is_not_null_)));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(is_unique_)));
  if (default_expr_ != nullptr) hash = common::HashUtil::CombineHashes(hash, default_expr_->Hash());
  if (check_expr_ != nullptr) hash = common::HashUtil::CombineHashes(hash, check_expr_->Hash());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(varlen_));
  hash = common::HashUtil::CombineHashInRange(hash, fk_sources_.begin(), fk_sources_.end());
  hash = common::HashUtil::CombineHashInRange(hash, fk_sinks_.begin(), fk_sinks_.end());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_sink_table_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_delete_action_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_update_action_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_match_type_));
  return hash;
}

}  // namespace terrier::parser
