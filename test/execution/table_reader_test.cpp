#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/tpl_test/schema_reader.h"
#include "execution/sql/execution_structures.h"
#include "catalog/catalog.h"
#include "execution/tpl_test.h"  // NOLINT


namespace tpl::compiler::test {

class TableReaderTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(TableReaderTest, SimpleSchemaTest) {
  // Get Catalog
  auto catalog = sql::ExecutionStructures::Instance()->GetCatalog();


  reader::SchemaReader schema_reader{catalog};
  auto table_info = schema_reader.ReadTableInfo("../sample_tpl/tables/test_1.schema");
  ASSERT_EQ(table_info->schema->GetColumns().size(), 4);
  ASSERT_EQ(table_info->index_schemas.size(), 1);
  ASSERT_EQ(table_info->index_maps[0].size(), 1);
}

}