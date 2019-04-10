#include "main/db_main.h"

int main(int argc, char **argv) {
  auto db_main = std::make_shared<terrier::DBMain>();
  db_main->Init(argc, argv);
  db_main->Start();
  db_main->Shutdown();
}
