#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/parser_defs.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

// TODO(Gus,Wen): This plan probably needs a different way of generating the output schema. The output schema should be
// the childs output schema. But also maybe this node doesnt even need an output schema in the execution layer, so I put
// it as null_ptr for now

namespace noisepage::planner {

/**
 * This is the plan node when exporting data from the database into an external
 * file. It is configured with the name of the file to write content into, and
 * the delimiter, quote, and escape characters to use when writing content.
 */
class ExportExternalFilePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a export external file scan plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param format ExternalFileFormat
     * @return builder object
     */
    Builder &SetFileFormat(parser::ExternalFileFormat format) {
      format_ = format;
      return *this;
    }

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
    std::unique_ptr<ExportExternalFilePlanNode> Build();

   protected:
    /**
     * Format
     */
    parser::ExternalFileFormat format_;
    /**
     * string representation of file name
     */
    std::string file_name_;
    /**
     * delimiter character
     */
    char delimiter_ = DEFAULT_DELIMETER_CHAR;
    /**
     * quote character
     */
    char quote_ = DEFAULT_QUOTE_CHAR;
    /**
     * escape character
     */
    char escape_ = DEFAULT_ESCAPE_CHAR;
  };

 private:
  /**
   * @param children child plan nodes
   * @param format Format
   * @param file_name string representation of file name
   * @param delimiter delimiter character
   * @param quote quote character
   * @param escape escape character
   */
  explicit ExportExternalFilePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                      parser::ExternalFileFormat format, std::string file_name, char delimiter,
                                      char quote, char escape);

 public:
  /**
   * Default constructor for deserialization
   */
  ExportExternalFilePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(ExportExternalFilePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::EXPORT_EXTERNAL_FILE; }

  /**
   * @return format
   */
  const parser::ExternalFileFormat &GetFormat() const { return format_; }

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

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  parser::ExternalFileFormat format_;
  std::string file_name_;
  char delimiter_;
  char quote_;
  char escape_;
};

DEFINE_JSON_HEADER_DECLARATIONS(ExportExternalFilePlanNode);

}  // namespace noisepage::planner
