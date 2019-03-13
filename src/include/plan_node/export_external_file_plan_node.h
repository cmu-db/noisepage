#pragma once

#include "abstract_plan_node.h"

// TODO(Gus,Wen): This plan probably needs a different way of generating the output schema. The output schema should be
// the childs output schema. But also maybe this node doesnt even need an output schema in the execution layer, so I put
// it as null_ptr for now

namespace terrier::plan_node {

/**
 * This is the plan node when exporting data from the database into an external
 * file. It is configured with the name of the file to write content into, and
 * the delimiter, quote, and escape characters to use when writing content.
 */
class ExportExternalFilePlanNode : public AbstractPlanNode {
 public:
  explicit ExportExternalFilePlanNode(std::string file_name, char delimiter = ',', char quote = '"', char escape = '\"')
      : AbstractPlanNode(nullptr),
        file_name_(std::move(file_name)),
        delimiter_(delimiter),
        quote_(quote),
        escape_(escape) {}

  PlanNodeType GetPlanNodeType() const override;

  const std::string &GetFileName() const { return file_name_; }
  char GetDelimiterChar() const { return delimiter_; }
  char GetQuoteChar() const { return quote_; }
  char GetEscapeChar() const { return escape_; }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  std::unique_ptr<AbstractPlanNode> Copy() const override;

 private:
  std::string file_name_;
  char delimiter_;
  char quote_;
  char escape_;

  DISALLOW_COPY_AND_MOVE(ExportExternalFilePlanNode);
};

}  // namespace terrier::plan_node
