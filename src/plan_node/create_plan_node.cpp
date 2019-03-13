#include "plan_node/create_plan_node.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/parser_defs.h"
#include "storage/data_table.h"

namespace terrier::plan_node {

CreatePlanNode::CreatePlanNode(std::string database_name, CreateType c_type)
    : database_name_(std::move(database_name)), create_type_(c_type) {}

CreatePlanNode::CreatePlanNode(std::string table_name, std::string schema_name, std::string database_name,
                               std::shared_ptr<catalog::Schema> schema, CreateType c_type)
    : table_name_(std::move(table_name)),
      schema_name_(std::move(schema_name)),
      database_name_(std::move(database_name)),
      table_schema_(std::move(schema)),
      create_type_(c_type) {}

CreatePlanNode::CreatePlanNode(parser::CreateStatement *create_stmt) {
  switch (create_stmt->GetCreateType()) {
    case parser::CreateStatement::CreateType::kDatabase: {
      create_type_ = CreateType::DB;
      database_name_ = std::string(create_stmt->GetDatabaseName());
      break;
    }

    case parser::CreateStatement::CreateType::kSchema: {
      create_type_ = CreateType::SCHEMA;
      database_name_ = std::string(create_stmt->GetDatabaseName());
      schema_name_ = std::string(create_stmt->GetSchemaName());
      break;
    }

    case parser::CreateStatement::CreateType::kTable: {
      table_name_ = std::string(create_stmt->GetTableName());
      schema_name_ = std::string(create_stmt->GetSchemaName());
      database_name_ = std::string(create_stmt->GetDatabaseName());
      std::vector<catalog::Schema::Column> columns;
      std::vector<std::string> pri_cols;

      create_type_ = CreateType::TABLE;

      for (auto &col : create_stmt->GetColumns()) {
        type::TypeId val = col->GetValueType(col->GetColumnType());

        // Create column
        // TODO(Gus,WEN) create columns using the catalog once it is available
        auto column = catalog::Schema::Column(std::string(col->GetColumnName()), val, false, catalog::col_oid_t(0));

        // Add DEFAULT constraints to the column
        if (col->GetDefaultExpression() != nullptr) {
          // Referenced from insert_plan.cpp
          if (col->GetDefaultExpression()->GetExpressionType() != parser::ExpressionType::VALUE_PARAMETER) {
            // TODO(Gus,Wen) set default value
            // parser::ConstantValueExpression *const_expr_elem =
            //    dynamic_cast<parser::ConstantValueExpression *>(col->GetDefaultExpression().get());
            // column.SetDefaultValue(const_expr_elem->GetValue());
          }
        }

        columns.emplace_back(column);

        // Collect Multi-column constraints information

        // Primary key
        if (col->IsPrimaryKey()) {
          pri_cols.push_back(col->GetColumnName());
        }

        // Unique constraint
        // Currently only supports for single column
        if (col->IsUnique()) {
          ProcessUniqueConstraint(col);
        }

        // Check expression constraint
        // Currently only supports simple boolean forms like (a > 0)
        if (col->GetCheckExpression() != nullptr) {
          ProcessCheckConstraint(col);
        }
      }

      // The parser puts the multi-column constraint information
      // into an artificial ColumnDefinition.
      // primary key constraint
      if (!pri_cols.empty()) {
        primary_key_.primary_key_cols = pri_cols;
        primary_key_.constraint_name = "con_primary";
        has_primary_key_ = true;
      }

      // foreign key
      for (auto &fk : create_stmt->GetForeignKeys()) {
        ProcessForeignKeyConstraint(table_name_, fk);
      }

      // TODO (Gus,Wen) UNIQUE and CHECK constraints

      table_schema_ = std::make_shared<catalog::Schema>(columns);
      break;
    }
    case parser::CreateStatement::CreateType::kIndex: {
      create_type_ = CreateType::INDEX;
      index_name_ = std::string(create_stmt->GetIndexName());
      table_name_ = std::string(create_stmt->GetTableName());
      schema_name_ = std::string(create_stmt->GetSchemaName());
      database_name_ = std::string(create_stmt->GetDatabaseName());

      // This holds the attribute names.
      // This is a fix for a bug where
      // The vector<char*>* items gets deleted when passed
      // To the Executor.

      std::vector<std::string> index_attrs_holder;

      for (auto &attr : create_stmt->GetIndexAttributes()) {
        index_attrs_holder.push_back(attr);
      }

      index_attrs_ = index_attrs_holder;

      index_type_ = create_stmt->GetIndexType();

      unique_index_ = create_stmt->IsUniqueIndex();
      break;
    }

    case parser::CreateStatement::CreateType::kTrigger: {
      create_type_ = CreateType::TRIGGER;
      trigger_name_ = std::string(create_stmt->GetTriggerName());
      table_name_ = std::string(create_stmt->GetTableName());
      schema_name_ = std::string(create_stmt->GetSchemaName());
      database_name_ = std::string(create_stmt->GetDatabaseName());

      if (create_stmt->GetTriggerWhen()) {
        trigger_when_ = create_stmt->GetTriggerWhen()->Copy();
      } else {
        trigger_when_ = nullptr;
      }
      trigger_type_ = create_stmt->GetTriggerType();

      for (auto &s : create_stmt->GetTriggerFuncNames()) {
        trigger_funcnames_.push_back(s);
      }
      for (auto &s : create_stmt->GetTriggerArgs()) {
        trigger_args_.push_back(s);
      }
      for (auto &s : create_stmt->GetTriggerColumns()) {
        trigger_columns_.push_back(s);
      }

      break;
    }
    default:
      break;
  }

  // TODO (Gus,Wen) check type parser::CreateStatement::CreateType::kDatabase
  // TODO (Gus, Wen) check type parser::CreateStatement::CreateType::kView
}

void CreatePlanNode::ProcessForeignKeyConstraint(const std::string &table_name,
                                                 const std::shared_ptr<parser::ColumnDefinition> &col) {
  ForeignKeyInfo fkey_info;

  fkey_info.foreign_key_sources = std::vector<std::string>();
  fkey_info.foreign_key_sinks = std::vector<std::string>();

  // Extract source and sink column names
  for (auto &key : col->GetForeignKeySources()) {
    fkey_info.foreign_key_sources.push_back(key);
  }
  for (auto &key : col->GetForeignKeySinks()) {
    fkey_info.foreign_key_sinks.push_back(key);
  }

  // Extract table names
  fkey_info.sink_table_name = col->GetForeignKeySinkTableName();

  // Extract delete and update actions
  fkey_info.upd_action = col->GetForeignKeyUpdateAction();
  fkey_info.del_action = col->GetForeignKeyDeleteAction();

  fkey_info.constraint_name = "FK_" + table_name + "->" + fkey_info.sink_table_name;

  foreign_keys_.push_back(fkey_info);
}

void CreatePlanNode::ProcessUniqueConstraint(const std::shared_ptr<parser::ColumnDefinition> &col) {
  UniqueInfo unique_info;

  unique_info.unique_cols = {col->GetColumnName()};
  unique_info.constraint_name = "con_unique";

  con_uniques_.push_back(unique_info);
}

void CreatePlanNode::ProcessCheckConstraint(const std::shared_ptr<parser::ColumnDefinition> &col) {
  auto check_cols = std::vector<std::string>();

  // TODO(Gus,Wen) more expression types need to be supported
  if (col->GetCheckExpression()->GetReturnValueType() == type::TypeId::BOOLEAN) {
    check_cols.push_back(col->GetColumnName());

    const parser::ConstantValueExpression *const_expr_elem =
        dynamic_cast<const parser::ConstantValueExpression *>(col->GetCheckExpression()->GetChild(1).get());
    type::TransientValue tmp_value = const_expr_elem->GetValue();

    CheckInfo check_info(check_cols, "con_check", col->GetCheckExpression()->GetExpressionType(), std::move(tmp_value));
    con_checks_.emplace_back(std::move(check_info));
  }
}

}  // namespace terrier::plan_node
