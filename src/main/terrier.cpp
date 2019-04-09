#include "main/db_main.h"

int main() {
  auto db_main = std::make_shared<terrier::DBMain>();
  db_main->Init();
  db_main->Start();
  db_main->Shutdown();
}
