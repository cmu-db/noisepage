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
  CopyStatement(std::unique_ptr<TableRef> table, std::unique_ptr<SelectStatement> select_stmt, std::string file_path,
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

  const std::unique_ptr<TableRef> table_;
  const std::unique_ptr<SelectStatement> select_stmt_;
  const std::string file_path_;
  const ExternalFileFormat format_;

  const bool is_from_;
  const char delimiter_;
  const char quote_;
  const char escape_;
};

}  // namespace parser
}  // namespace terrier
