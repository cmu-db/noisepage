#include <sqlite3.h>
#include <cstdio>
#include <tcop/sqlite.h>

#include "loggers/main_logger.h"

#include "tcop/sqlite.h"

namespace terrier {
SqliteEngine::SqliteEngine() {
  auto rc = sqlite3_open_v2(
      "sqlite.db", &sqlite_db_,
      SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
  if (rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(sqlite_db_));
    LOG_ERROR("Can't open database %s", sqlite3_errmsg(sqlite_db_));
    exit(0);
  } else {
    fprintf(stderr, "\n");
  }
}

SqliteEngine::~SqliteEngine() {
  sqlite3_close(sqlite_db_);
}
void SqliteEngine::ExecuteQuery(const char *query, SqliteCallback callback) {

  sqlite3_exec(sqlite_db_, query, callback, nullptr, &error_msg);
  if(error_msg != nullptr)
  {
    LOG_ERROR("Error msg from Sqlite3: " + std::string(error_msg));
    sqlite3_free(error_msg);
  }
}

}

