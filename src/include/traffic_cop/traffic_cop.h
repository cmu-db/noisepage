#pragma once
#include "sqlite.h"

namespace terrier::traffic_cop {

/*
 * Traffic Cop of the database.
 * This is the reception of the backend execution system.
 *
 * *Should be a singleton*
 * */

class TrafficCop {

 public:
  void ExecuteQuery(const char *query, std::function<void(FakeResultSet &)> &callback);

 private:
  SqliteEngine sqlite_engine;

};

}

