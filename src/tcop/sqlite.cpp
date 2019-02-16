#include <sqlite3.h>
#include <cstdio>
#include <tcop/sqlite.h>

#include "loggers/main_logger.h"

#include "tcop/sqlite.h"

namespace terrier {
Sqlite::Sqlite() {
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

Sqlite::~Sqlite() {
  sqlite3_close(sqlite_db_);
}

}

