#pragma once

struct sqlite3;

namespace terrier {

using SqliteCallback = int (*)(void*,int,char**,char**);

class SqliteEngine
{

 public:
  SqliteEngine();
  virtual ~SqliteEngine();

  void ExecuteQuery(const char *query, SqliteCallback callback);

 private:
  // SQLite database
  struct sqlite3 *sqlite_db_;
  char *error_msg;
};

} // namespace terrier

