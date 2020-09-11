#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "planner/plannodes/abstract_scan_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

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
     * @param val_types vector of value types for columns
     * @return builder object
     */
    Builder &SetValueTypes(std::vector<type::TypeId> val_types) {
      value_types_ = std::move(val_types);
      return *this;
    }

    /**
     * Build the csv scan plan node
     * @return plan node
     */
    std::unique_ptr<CSVScanPlanNode> Build() {
      return std::unique_ptr<CSVScanPlanNode>(
          new CSVScanPlanNode(std::move(children_), std::move(output_schema_), nullptr /* predicate */, is_for_update_,
                              database_oid_, file_name_, delimiter_, quote_, escape_, value_types_, scan_limit_,
                              scan_has_limit_, scan_offset_, scan_has_offset_));
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
     * Value Types vector
     */
    std::vector<type::TypeId> value_types_;
  };

 private:
  /**
   * Constructs a sequential scan over a CSV file
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate nullptr for csv scans
   * @param is_for_update false for csv scans
   * @param database_oid database oid for scan
   * @param file_name The file path
   * @param delimiter The character that separates columns within a row
   * @param quote The character used to quote data (i.e., strings)
   * @param escape The character that should appear before any data characters that match the quote character.
   * @param value_types Value types for vector of columns
   */
  CSVScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                  std::unique_ptr<OutputSchema> output_schema,
                  common::ManagedPointer<parser::AbstractExpression> predicate, bool is_for_update,
                  catalog::db_oid_t database_oid, std::string file_name, char delimiter, char quote, char escape,
                  std::vector<type::TypeId> value_types, uint32_t scan_limit, bool scan_has_limit, uint32_t scan_offset,
                  bool scan_has_offset)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), predicate, is_for_update, database_oid,
                             scan_limit, scan_has_limit, scan_offset, scan_has_offset),
        file_name_(std::move(file_name)),
        delimiter_(delimiter),
        quote_(quote),
        escape_(escape),
        value_types_(std::move(value_types)) {}

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
   * @return value types
   */
  const std::vector<type::TypeId> &GetValueTypes() const { return value_types_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  std::string file_name_;
  char delimiter_;
  char quote_;
  char escape_;
  std::vector<type::TypeId> value_types_;
};

DEFINE_JSON_HEADER_DECLARATIONS(CSVScanPlanNode);

}  // namespace terrier::planner
