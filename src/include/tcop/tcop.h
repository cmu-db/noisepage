#pragma once
#include "sqlite.h"

namespace terrier{
class TrafficCop {

 public:
  void ExecuteQuery(const char *query, SqliteCallback callback);

 private:
  SqliteEngine sqlite_engine;

};
}

