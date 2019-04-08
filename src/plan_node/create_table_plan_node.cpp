#include "plan_node/create_table_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser_defs.h"

namespace terrier::plan_node {
common::hash_t CreateTablePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash schema_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetSchemaName()));

  // TODO(Gus,Wen) Hash catalog::Schema

  // Hash has primary_key
  auto has_primary_key = HasPrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&has_primary_key));

  // Hash primary_key
  hash = common::HashUtil::CombineHashes(hash, primary_key_.Hash());

  // Hash foreign_keys
  for (const auto &foreign_key : foreign_keys_) {
    hash = common::HashUtil::CombineHashes(hash, foreign_key.Hash());
  }

  // Hash con_uniques
  for (const auto &con_unique : con_uniques_) {
    hash = common::HashUtil::CombineHashes(hash, con_unique.Hash());
  }

  // Hash con_checks
  for (const auto &con_check : con_checks_) {
    hash = common::HashUtil::CombineHashes(hash, con_check.Hash());
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateTablePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateTablePlanNode &>(rhs);

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // Schema name
  if (GetSchemaName() != other.GetSchemaName()) return false;

  // TODO(Gus,Wen) Compare catalog::Schema

  // Has primary key
  if (HasPrimaryKey() != other.HasPrimaryKey()) return false;

  // Foreign key
  const auto &foreign_keys_ = GetForeignKeys();
  const auto &other_foreign_keys_ = other.GetForeignKeys();
  if (foreign_keys_.size() != other_foreign_keys_.size()) return false;

  for (size_t i = 0; i < foreign_keys_.size(); i++) {
    if (foreign_keys_[i] != other_foreign_keys_[i]) {
      return false;
    }
  }

  // Unique constraints
  const auto &con_uniques = GetUniqueConstraintss();
  const auto &other_con_uniques = other.GetUniqueConstraintss();
  if (con_uniques.size() != other_con_uniques.size()) return false;

  for (size_t i = 0; i < con_uniques.size(); i++) {
    if (con_uniques[i] != other_con_uniques[i]) {
      return false;
    }
  }

  // Check constraints
  const auto &con_checks = GetCheckConstrinats();
  const auto &other_con_check = other.GetCheckConstrinats();
  if (con_checks.size() != other_con_check.size()) return false;

  for (size_t i = 0; i < con_checks.size(); i++) {
    if (con_checks[i] != other_con_check[i]) {
      return false;
    }
  }

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
