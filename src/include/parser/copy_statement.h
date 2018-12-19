#pragma once

#include <memory>
#include <string>
#include <utility>

#include "common/sql_node_visitor.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {

/**
 * Represents PSQL COPY statements.
 */
class CopyStatement : public SQLStatement {
 public:
  /**
   * @param table table to copy from
   * @param select_stmt select statement to copy from
   * @param file_path path to output file
   * @param format file format
   * @param is_from true if FROM, false if TO
   * @param delimiter delimiter to be used for copying
   * @param quote quote character
   * @param escape escape character
   */
  CopyStatement(std::shared_ptr<TableRef> table, std::shared_ptr<SelectStatement> select_stmt, std::string file_path,
                ExternalFileFormat format, bool is_from, char delimiter, char quote, char escape)
      : SQLStatement(StatementType::COPY),
        table_(std::move(table)),
        select_stmt_(std::move(select_stmt)),
        file_path_(std::move(file_path)),
        format_(format),
        is_from_(is_from),
        delimiter_(delimiter),
        quote_(quote),
        escape_(escape) {}

  ~CopyStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return copy table
   */
  std::shared_ptr<TableRef> GetCopyTable() { return table_; }

  /**
   * @return select statement
   */
  std::shared_ptr<SelectStatement> GetSelectStatement() { return select_stmt_; }

  /**
   * @return file path
   */
  std::string GetFilePath() { return file_path_; }

  /**
   * @return external file format
   */
  ExternalFileFormat GetExternalFileFormat() { return format_; }

  /**
   * @return true if FROM, false if TO
   */
  bool IsFrom() { return is_from_; }

  /**
   * @return delimiter
   */
  char GetDelimiter() { return delimiter_; }

  /**
   * @return quote char
   */
  char GetQuoteChar() { return quote_; }

  /**
   * @return escape char
   */
  char GetEscapeChar() { return escape_; }

 private:
  const std::shared_ptr<TableRef> table_;
  const std::shared_ptr<SelectStatement> select_stmt_;
  const std::string file_path_;
  const ExternalFileFormat format_;

  const bool is_from_;
  const char delimiter_;
  const char quote_;
  const char escape_;
};

}  // namespace parser
}  // namespace terrier
