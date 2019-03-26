#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_plan_node.h"

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
 protected:
  /**
   * Builder for a export external file scan plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param file_name file path for external file file
     * @return builder object
     */
    Builder &SetFileName(std::string file_name) {
      file_name_ = std::move(file_name);
      return *this;
    }

    /**
     * @param delimiter delimiter character for external file
     * @return builder object
     */
    Builder &SetDelimiter(char delimiter) {
      delimiter_ = delimiter;
      return *this;
    }

    /**
     * @param quote quote character for external file
     * @return builder object
     */
    Builder &SetQuote(char quote) {
      quote_ = quote;
      return *this;
    }

    /**
     * @param escape escape character for external file
     * @return builder object
     */
    Builder &SetEscape(char escape) {
      escape_ = escape;
      return *this;
    }

    /**
     * Build the export external file scan plan node
     * @return plan node
     */
    std::shared_ptr<ExportExternalFilePlanNode> Build() {
      return std::shared_ptr<ExportExternalFilePlanNode>(
          new ExportExternalFilePlanNode(std::move(children_), file_name_, delimiter_, quote_, escape_));
    }

   protected:
    std::string file_name_;
    char delimiter_ = ',';
    char quote_ = '"';
    char escape_ = '"';
  };

  /**
   * @param children child plan nodes
   * @param file_name string representation of file name
   * @param delimiter delimiter character
   * @param quote quote character
   * @param escape escape character
   */
  explicit ExportExternalFilePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::string file_name,
                                      char delimiter = ',', char quote = '"', char escape = '\"')
      : AbstractPlanNode(std::move(children), nullptr, 0),
        file_name_(std::move(file_name)),
        delimiter_(delimiter),
        quote_(quote),
        escape_(escape) {}

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::EXPORT_EXTERNAL_FILE; }

  /**
   * @return string representation of file name
   */
  const std::string &GetFileName() const { return file_name_; }

  /**
   * @return delimiter character
   */
  char GetDelimiterChar() const { return delimiter_; }

  /**
   * @return quote character
   */
  char GetQuoteChar() const { return quote_; }

  /**
   * @return escape character
   */
  char GetEscapeChar() const { return escape_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  std::string file_name_;
  char delimiter_;
  char quote_;
  char escape_;

 public:
  DISALLOW_COPY_AND_MOVE(ExportExternalFilePlanNode);
};

}  // namespace terrier::plan_node
