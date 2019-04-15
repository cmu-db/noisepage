#include "main/db_main.h"

int main(int argc, char **argv) {
  terrier::DBMain db;
  db.Init(argc, argv);
  db.Run();
}
