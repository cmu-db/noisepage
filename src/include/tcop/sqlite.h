#pragma once

struct sqlite3;

namespace terrier {

class Sqlite
{

 public:
  Sqlite();
  virtual ~Sqlite();

 private:
  // SQLite database
  struct sqlite3 *sqlite_db_;
};

} // namespace terrier

