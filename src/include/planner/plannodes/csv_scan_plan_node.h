#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "planner/plannodes/abstract_scan_plan_node.h"

namespace terrier::planner {
/**
 * Plan node for a CSV scan
 */
class CSVScanPlanNode : public AbstractScanPlanNode {
 public:
  /**
   * Builder for a CSV scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param file_name file path for CSV file
     * @return builder object
     */
    Builder &SetFileName(std::string file_name) {
      file_name_ = std::move(file_name);
      return *this;
    }

    /**
     * @param delimiter delimiter character for CSV
     * @return builder object
     */
    Builder &SetDelimiter(char delimiter) {
      delimiter_ = delimiter;
      return *this;
    }

    /**
     * @param quote quote character for CSV
     * @return builder object
     */
    Builder &SetQuote(char quote) {
      quote_ = quote;
      return *this;
    }

    /**
     * @param escape escape character for CSV
     * @return builder object
     */
    Builder &SetEscape(char escape) {
      escape_ = escape;
      return *this;
    }

    /**
     * @param null_string null string for CSV
     * @return builder object
     */
    Builder &SetNullString(std::string null_string) {
      null_string_ = std::move(null_string);
      return *this;
    }

    /**
     * Build the csv scan plan node
     * @return plan node
     */
    std::shared_ptr<CSVScanPlanNode> Build() {
      return std::shared_ptr<CSVScanPlanNode>(new CSVScanPlanNode(
          std::move(children_), std::move(output_schema_), nullptr /* predicate */, is_for_update_, is_parallel_,
          database_oid_, namespace_oid_, file_name_, delimiter_, quote_, escape_, null_string_));
    }

   protected:
    /**
     * string representation of file name
     */
    std::string file_name_;
    /**
     * delimiter character for CSV
     */
    char delimiter_ = DEFAULT_DELIMETER_CHAR;
    /**
     * quote character for CSV
     */
    char quote_ = DEFAULT_QUOTE_CHAR;
    /**
     * escape character for CSV
     */
    char escape_ = DEFAULT_ESCAPE_CHAR;
    /**
     * null string for CSV
     */
    std::string null_string_ = DEFAULT_NULL_STRING;
  };

 private:
  /**
   * Constructs a sequential scan over a CSV file
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate nullptr for csv scans
   * @param is_for_update false for csv scans
   * @param is_parallel false for csv scans
   * @param database_oid database oid for scan
   * @param file_name The file path
   * @param delimiter The character that separates columns within a row
   * @param quote The character used to quote data (i.e., strings)
   * @param escape The character that should appear before any data characters that match the quote character.
   * @param null_string the null string for the file
   */
  CSVScanPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                  std::shared_ptr<OutputSchema> output_schema, std::shared_ptr<parser::AbstractExpression> predicate,
                  bool is_for_update, bool is_parallel, catalog::db_oid_t database_oid,
                  catalog::namespace_oid_t namespace_oid, std::string file_name, char delimiter, char quote,
                  char escape, std::string null_string)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), std::move(predicate), is_for_update,
                             is_parallel, database_oid, namespace_oid),
        file_name_(std::move(file_name)),
        delimiter_(delimiter),
        quote_(quote),
        escape_(escape),
        null_string_(std::move(null_string)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  CSVScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CSVScanPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CSVSCAN; }

  /**
   * @return string representation of file name
   */
  const std::string &GetFileName() const { return file_name_; }

  /**
   * @return delimiter character for CSV
   */
  char GetDelimiterChar() const { return delimiter_; }

  /**
   * @return quote character for CSV
   */
  char GetQuoteChar() const { return quote_; }

  /**
   * @return escape character for CSV
   */
  char GetEscapeChar() const { return escape_; }

  /**
   * @return null string for CSV
   */
  const std::string &GetNullString() const { return null_string_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  std::string file_name_;
  char delimiter_;
  char quote_;
  char escape_;
  std::string null_string_;
};

DEFINE_JSON_DECLARATIONS(CSVScanPlanNode);

}  // namespace terrier::planner
