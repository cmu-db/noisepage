#include "planner/plannodes/export_external_file_plan_node.h"

#include <memory>
#include <string>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t ExportExternalFilePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Filename
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));

  // Delimiter
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));

  // Quote Char
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));

  // Escape Char
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(format_));

  return hash;
}

bool ExportExternalFilePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const ExportExternalFilePlanNode &>(rhs);

  // Filename
  if (file_name_ != other.file_name_) return false;

  // Delimiter
  if (delimiter_ != other.delimiter_) return false;

  // Quote Char
  if (quote_ != other.quote_) return false;

  // Escape Char
  if (escape_ != other.escape_) return false;

  if (format_ != other.format_) return false;

  return true;
}

nlohmann::json ExportExternalFilePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["file_name"] = file_name_;
  j["delimiter"] = delimiter_;
  j["quote"] = quote_;
  j["escape"] = escape_;
  j["format"] = format_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> ExportExternalFilePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  file_name_ = j.at("file_name").get<std::string>();
  delimiter_ = j.at("delimiter").get<char>();
  quote_ = j.at("quote").get<char>();
  escape_ = j.at("escape").get<char>();
  format_ = j.at("format").get<parser::ExternalFileFormat>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ExportExternalFilePlanNode);

}  // namespace noisepage::planner
