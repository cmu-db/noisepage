#include "util/test_harness.h"
#include "tcop/tcop.h"


namespace terrier{
class TrafficCopTests : public TerrierTest
{

};

static int PrintRows(void *callback_param, int argc, char **values, char **col_name){
  int i;
  if(callback_param != nullptr)
    printf("callback param: %s", (const char*)callback_param);
  for(i=0; i<argc; i++){
    printf("%s = %s\n", col_name[i], values[i] ? values[i] : "NULL");
  }
  printf("\n");
  return 0;
}


//NOLINTNEXTLINE
TEST_F(TrafficCopTests, FirstTest) {
  TrafficCop traffic_cop;


  traffic_cop.ExecuteQuery("DROP TABLE IF EXISTS TableA", PrintRows);
  traffic_cop.ExecuteQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);", PrintRows);
  traffic_cop.ExecuteQuery("INSERT INTO TableA VALUES (1, 'abc');", PrintRows);
  traffic_cop.ExecuteQuery("INSERT INTO TableA VALUES (2, 'def');", PrintRows);
  traffic_cop.ExecuteQuery("SELECT * FROM TableA", PrintRows);

}

} // namespace terrier
