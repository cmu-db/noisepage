#include "traffic_cop/traffic_cop_util.h"

#include "parser/parser_defs.h"
#include "parser/postgresparser.h"

namespace terrier::trafficcop {

network::QueryType TrafficCopUtil::QueryTypeForStatement(const common::ManagedPointer<parser::SQLStatement> statement) {
  const auto statement_type = statement->GetType();
  switch (statement_type) {
    case parser::StatementType::TRANSACTION: {
      const auto txn_type = statement.CastManagedPointerTo<parser::TransactionStatement>()->GetTransactionType();
      switch (txn_type) {
        case parser::TransactionStatement::CommandType::kBegin:
          return network::QueryType::QUERY_BEGIN;
        case parser::TransactionStatement::CommandType::kCommit:
          return network::QueryType::QUERY_COMMIT;
        case parser::TransactionStatement::CommandType::kRollback:
          return network::QueryType::QUERY_ROLLBACK;
      }
    }
    case parser::StatementType::SELECT:
      return network::QueryType::QUERY_SELECT;
    case parser::StatementType::INSERT:
      return network::QueryType::QUERY_INSERT;
    case parser::StatementType::UPDATE:
      return network::QueryType::QUERY_UPDATE;
    case parser::StatementType::DELETE:
      return network::QueryType::QUERY_DELETE;
    case parser::StatementType::CREATE: {
      const auto create_type = statement.CastManagedPointerTo<parser::CreateStatement>()->GetCreateType();
      switch (create_type) {
        case parser::CreateStatement::CreateType::kTable:
          return network::QueryType::QUERY_CREATE_TABLE;
        case parser::CreateStatement::CreateType::kDatabase:
          return network::QueryType::QUERY_CREATE_DB;
        case parser::CreateStatement::CreateType::kIndex:
          return network::QueryType::QUERY_CREATE_INDEX;
        case parser::CreateStatement::CreateType::kTrigger:
          return network::QueryType::QUERY_CREATE_TRIGGER;
        case parser::CreateStatement::CreateType::kSchema:
          return network::QueryType::QUERY_CREATE_SCHEMA;
        case parser::CreateStatement::CreateType::kView:
          return network::QueryType::QUERY_CREATE_VIEW;
      }
    }
    case parser::StatementType::DROP: {
      const auto drop_type = statement.CastManagedPointerTo<parser::DropStatement>()->GetDropType();
      switch (drop_type) {
        case parser::DropStatement::DropType::kDatabase:
          return network::QueryType::QUERY_DROP_DB;
        case parser::DropStatement::DropType::kTable:
          return network::QueryType::QUERY_DROP_TABLE;
        case parser::DropStatement::DropType::kSchema:
          return network::QueryType::QUERY_DROP_SCHEMA;
        case parser::DropStatement::DropType::kIndex:
          return network::QueryType::QUERY_DROP_INDEX;
        case parser::DropStatement::DropType::kView:
          return network::QueryType::QUERY_DROP_VIEW;
        case parser::DropStatement::DropType::kPreparedStatement:
          return network::QueryType::QUERY_DROP_PREPARED_STATEMENT;
        case parser::DropStatement::DropType::kTrigger:
          return network::QueryType::QUERY_DROP_TRIGGER;
      }
    }
    case parser::StatementType::VARIABLE_SET:
      return network::QueryType::QUERY_SET;
    case parser::StatementType::PREPARE:
      return network::QueryType::QUERY_PREPARE;
    case parser::StatementType::EXECUTE:
      return network::QueryType::QUERY_EXECUTE;
    case parser::StatementType::RENAME:
      return network::QueryType::QUERY_RENAME;
    case parser::StatementType::ALTER:
      return network::QueryType::QUERY_ALTER;
    case parser::StatementType::COPY:
      return network::QueryType::QUERY_COPY;
    case parser::StatementType::ANALYZE:
      return network::QueryType::QUERY_ANALYZE;
    case parser::StatementType::EXPLAIN:
      return network::QueryType::QUERY_EXPLAIN;
    default:
      return network::QueryType::QUERY_INVALID;
  }
}

}  // namespace terrier::trafficcop
