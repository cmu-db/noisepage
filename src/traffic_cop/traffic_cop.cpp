#include <sqlite3.h>
#include <util/test_harness.h>

#include "network/postgres_protocol_utils.h"
#include "traffic_cop/traffic_cop.h"
#include "type/transient_value_factory.h"

namespace terrier::traffic_cop {

void TrafficCop::ExecuteQuery(const char *query, network::PostgresPacketWriter *const out,
                              const network::SimpleQueryCallback &callback) {
  sqlite_engine.ExecuteQuery(query, out, callback);
}

Statement TrafficCop::Parse(const char *query, const std::vector<type::TypeId> &param_types) {

  Statement statement;
  statement.sqlite3_stmt_ = sqlite_engine.PrepareStatement(query);

  using namespace type;

  for(auto type_id : param_types)
  {
    // We use zero values for each of the types, as we don't know the actual values now
    if(type_id == TypeId::INTEGER)
      statement.query_params.push_back(TransientValueFactory::GetInteger(0));
    else if(type_id == TypeId::BIGINT)
      statement.query_params.push_back(TransientValueFactory::GetBigInt(0));
    else if(type_id == TypeId::BOOLEAN)
      statement.query_params.push_back(TransientValueFactory::GetBoolean(false));
    else if(type_id == TypeId::DECIMAL)
      statement.query_params.push_back(TransientValueFactory::GetDecimal(0.0));
    else if(type_id == TypeId::VARCHAR)
      statement.query_params.push_back(TransientValueFactory::GetVarChar(nullptr));
  }

  return statement;

}

}  // namespace terrier::traffic_cop
