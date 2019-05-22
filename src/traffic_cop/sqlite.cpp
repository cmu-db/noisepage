#include <sqlite3.h>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "loggers/main_logger.h"
#include "network/network_defs.h"
#include "traffic_cop/result_set.h"
#include "traffic_cop/sqlite.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/transient_value_peeker.h"

namespace terrier::traffic_cop {

SqliteEngine::SqliteEngine() {
  auto rc = sqlite3_open_v2("sqlite.db", &sqlite_db_, SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                            nullptr);
  if (rc != 0) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(sqlite_db_));
    LOG_ERROR("Can't open database %s", sqlite3_errmsg(sqlite_db_));
    exit(0);
  } else {
    fprintf(stderr, "\n");
  }
}

SqliteEngine::~SqliteEngine() { sqlite3_close(sqlite_db_); }

void SqliteEngine::ExecuteQuery(const char *query, network::PostgresPacketWriter *out,
                                const network::SimpleQueryCallback &callback) {
  ResultSet result_set;

  sqlite3_exec(sqlite_db_, query, StoreResults, &result_set, &error_msg);
  if (error_msg != nullptr) {
    LOG_ERROR("Error msg from Sqlite3: " + std::string(error_msg));
    sqlite3_free(error_msg);
  }

  callback(result_set, out);
}

int SqliteEngine::StoreResults(void *result_set_void, int elem_count, char **values, char **column_names) {
  auto result_set = reinterpret_cast<ResultSet *>(result_set_void);

  if (result_set->column_names_.empty()) {
    for (int i = 0; i < elem_count; i++) {
      result_set->column_names_.emplace_back(column_names[i]);
    }
  }

  Row current_row;
  for (int i = 0; i < elem_count; i++) {
    current_row.emplace_back(type::TransientValueFactory::GetVarChar(values[i]));
  }

  result_set->rows_.push_back(std::move(current_row));

  return 0;
}

sqlite3_stmt *SqliteEngine::PrepareStatement(std::string query) {
  // Replace "$" to "?"
  for (char &c : query)
    if (c == '$') c = '?';

  sqlite3_stmt *stmt;
  int error_code = sqlite3_prepare_v2(sqlite_db_, query.c_str(), -1, &stmt, nullptr);
  if (error_code == SQLITE_OK) return stmt;

  LOG_ERROR("Sqlite Prepare Error: Error Code = {0}, msg = {1}", error_code, sqlite3_errmsg(sqlite_db_));
  return nullptr;
}

void SqliteEngine::Bind(sqlite3_stmt *stmt, const std::shared_ptr<std::vector<type::TransientValue>> &p_params) {
  using type::TransientValuePeeker;
  using type::TypeId;

  sqlite3_reset(stmt);
  auto &params = *p_params;

  for (int i = 0; i < static_cast<int>(params.size()); i++) {
    auto type = params[i].Type();
    int res;
    if (type == TypeId::INTEGER) {
      res = sqlite3_bind_int(stmt, i + 1, TransientValuePeeker::PeekInteger(params[i]));
    } else if (type == TypeId::DECIMAL) {
      res = sqlite3_bind_double(stmt, i + 1, TransientValuePeeker::PeekDecimal(params[i]));
    } else if (type == TypeId::VARCHAR) {
      std::string_view varchar_value = TransientValuePeeker::PeekVarChar(params[i]);
      res = sqlite3_bind_text(stmt, i + 1, varchar_value.data(), -1, SQLITE_STATIC);
    } else if (type == TypeId::TIMESTAMP) {
      auto value = static_cast<int64_t>(!TransientValuePeeker::PeekTimestamp(params[i]));
      res = sqlite3_bind_int64(stmt, i + 1, value);
    } else {
      LOG_ERROR("Unsupported type: {0}", static_cast<int>(type));
      res = 0;
    }

    if (res != SQLITE_OK) {
      LOG_ERROR("Bind error: error code = {0}", res);
    }
  }
}

std::vector<std::string> SqliteEngine::DescribeColumns(sqlite3_stmt *stmt) {
  int column_cnt = sqlite3_column_count(stmt);
  std::vector<std::string> column_names;
  for (int i = 0; i < column_cnt; i++) {
    std::string col_name = std::string(sqlite3_column_name(stmt, i));
    column_names.push_back(col_name);
  }
  return column_names;
}

ResultSet SqliteEngine::Execute(sqlite3_stmt *stmt) {
  ResultSet result_set;
  result_set.column_names_ = DescribeColumns(stmt);
  auto column_cnt = static_cast<int>(result_set.column_names_.size());

  int result_code = sqlite3_step(stmt);

  while (result_code == SQLITE_ROW) {
    Row row;
    for (int i = 0; i < column_cnt; i++) {
      int type = sqlite3_column_type(stmt, i);
      if (type == SQLITE_INTEGER) {
        int value = sqlite3_column_int(stmt, i);
        row.push_back(type::TransientValueFactory::GetInteger(value));
      } else if (type == SQLITE_FLOAT) {
        double value = sqlite3_column_double(stmt, i);
        row.push_back(type::TransientValueFactory::GetDecimal(value));
      } else if (type == SQLITE_TEXT) {
        const unsigned char *result_cstring = sqlite3_column_text(stmt, i);
        auto *value = reinterpret_cast<const char *>(result_cstring);
        row.push_back(type::TransientValueFactory::GetVarChar(value));
      } else if (type == SQLITE_NULL) {
        row.push_back(type::TransientValueFactory::GetVarChar("NULL"));
      } else {
        LOG_ERROR("Unsupported Type: {0}", type);
      }
    }
    result_set.rows_.push_back(std::move(row));

    result_code = sqlite3_step(stmt);
  }

  LOG_TRACE("Execute complete, {0} rows are in the result set", result_set.rows_.size());

  return result_set;
}

}  // namespace terrier::traffic_cop
