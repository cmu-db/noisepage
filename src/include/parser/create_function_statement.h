#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"

// TODO(WAN): this file is messy
namespace terrier {
namespace parser {

/**
 * Function parameter.
 */
struct Parameter {
  // TODO(WAN): there used to be a FuncParamMode that was never used?

  enum class DataType {
    INT,
    INTEGER,
    TINYINT,
    SMALLINT,
    BIGINT,
    CHAR,
    DOUBLE,
    FLOAT,
    DECIMAL,
    VARCHAR,
    TEXT,
    BOOL,
    BOOLEAN
  };

  explicit Parameter(DataType datatype) : datatype_(datatype) {}

  virtual ~Parameter() = default;

  const DataType datatype_;

  // TODO(WAN): why not just do this immediately upon parsing? move to defs?
  /**
   * Returns the underlying type used to represent the datatype.
   * @param datatype underlying type
   * @return underying type
   */
  static type::TypeId GetValueType(DataType datatype) {
    switch (datatype) {
      case DataType::INT:
      case DataType::INTEGER:
        return type::TypeId::INTEGER;
      case DataType::TINYINT:
        return type::TypeId::TINYINT;
      case DataType::SMALLINT:
        return type::TypeId::SMALLINT;
      case DataType::BIGINT:
        return type::TypeId::BIGINT;
      case DataType::DECIMAL:
      case DataType::DOUBLE:
      case DataType::FLOAT:
        return type::TypeId::DECIMAL;
      case DataType::CHAR:
      case DataType::TEXT:
      case DataType::VARCHAR:
        return type::TypeId::VARCHAR;
      case DataType::BOOL:
      case DataType::BOOLEAN:
        return type::TypeId::BOOLEAN;
      default:
        return type::TypeId::INVALID;
    }
  }
};

struct ReturnType : Parameter {
  explicit ReturnType(DataType datatype) : Parameter(datatype) {}
  ~ReturnType() override = default;
};

struct FuncParameter : Parameter {
  FuncParameter(DataType datatype, std::string name) : Parameter(datatype), name_(std::move(name)) {}
  ~FuncParameter() override = default;
  const std::string name_;
};

class CreateFunctionStatement : public SQLStatement {
 public:
  CreateFunctionStatement(bool replace, std::string func_name, std::vector<std::string> func_body,
                          std::unique_ptr<ReturnType> return_type,
                          std::vector<std::unique_ptr<FuncParameter>> func_parameters, PLType pl_type, AsType as_type)
      : SQLStatement(StatementType::CREATE_FUNC),
        replace_(replace),
        func_name_(std::move(func_name)),
        return_type_(std::move(return_type)),
        func_body_(std::move(func_body)),
        func_parameters_(std::move(func_parameters)),
        pl_type_(pl_type),
        as_type_(as_type) {}

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const bool replace_ = false;
  const std::string func_name_;
  const std::unique_ptr<ReturnType> return_type_;
  const std::vector<std::string> func_body_;
  const std::vector<std::unique_ptr<FuncParameter>> func_parameters_;
  const PLType pl_type_;
  const AsType as_type_;
};

}  // namespace parser
}  // namespace terrier
