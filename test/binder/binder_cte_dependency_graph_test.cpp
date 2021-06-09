#include <memory>

#include "binder/bind_node_visitor.h"
#include "binder/cte/context_sensitive_table_ref.h"
#include "binder/cte/dependency_graph.h"
#include "binder/cte/lexical_scope.h"
#include "binder/cte/structured_statement.h"
#include "catalog/catalog.h"
#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_manager.h"

using noisepage::binder::cte::ContextSensitiveTableRef;
using noisepage::binder::cte::DependencyGraph;
using noisepage::binder::cte::Edge;
using noisepage::binder::cte::RefType;
using noisepage::binder::cte::StructuredStatement;
using noisepage::binder::cte::Vertex;
using noisepage::binder::cte::VertexType;

// Alias, Type, Depth, Position
using PseudoVertex = std::tuple<std::string, VertexType, std::size_t, std::size_t>;

namespace noisepage {
class BinderCteDepdendencyGraphTest : public TerrierTest {
 public:
  void InitializeTables() {
    // Create database
    txn_ = txn_manager_->BeginTransaction();
    db_oid_ = catalog_->CreateDatabase(common::ManagedPointer{txn_}, database_name_, true);

    // Commit the transaction
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TypeId::INTEGER);

    // Create table
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);

    // CREATE TABLE TEST(x INT, y INT)
    std::vector<catalog::Schema::Column> cols{};
    cols.emplace_back("x", type::TypeId::INTEGER, true, int_default);
    cols.emplace_back("y", type::TypeId::INTEGER, true, int_default);
    catalog::Schema schema{cols};

    table_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), table_name_, schema);
    auto table = new storage::SqlTable{db_main_->GetStorageLayer()->GetBlockStore(), schema};
    EXPECT_TRUE(accessor_->SetTablePointer(table_oid_, table));

    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void SetUp() override {
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    InitializeTables();

    // Prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

 protected:
  // The test database name
  const std::string database_name_{"TestDB"};

  // The test table name
  const std::string table_name_{"TestTable"};

  // The DBMain instance
  std::unique_ptr<DBMain> db_main_;

  // The test database OID
  catalog::db_oid_t db_oid_;

  // The test table OID
  catalog::table_oid_t table_oid_;

  // The transaction manager
  transaction::TransactionContext *txn_;

  // The catalog instance
  common::ManagedPointer<catalog::Catalog> catalog_;

  // The catalog accessor
  std::unique_ptr<catalog::CatalogAccessor> accessor_;

  // The transaction manager
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
};

/**
 * Get the SELECT statement from a raw SQL query.
 * @param sql The query string
 * @return A pair of the parse tree and the SELECT statement;
 * we must return both in order to extend the parse tree's lifetime
 */
static std::pair<std::unique_ptr<parser::ParseResult>, common::ManagedPointer<parser::SelectStatement>>
ParseToSelectStatement(const std::string &sql) {
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);
  if (parse_tree->GetStatement(0)->GetType() != parser::StatementType::SELECT) {
    // Just die, don't really care how
    throw std::runtime_error{""};
  }
  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();
  return std::make_pair(std::move(parse_tree), select);
}

/**
 * Build an edge from `src` to `dst`.
 * @param src The source vertex, as PsuedoVertex (std::tuple)
 * @param dst The destination vertex, as PseudoVertex (std::tuple)
 * @return The new edge
 */
static Edge MakeEdge(const PseudoVertex &src, const PseudoVertex &dst) {
  return Edge{Vertex{std::get<0>(src), std::get<1>(src), std::get<2>(src), std::get<3>(src)},
              Vertex{std::get<0>(dst), std::get<1>(dst), std::get<2>(dst), std::get<3>(dst)}};
}

// ----------------------------------------------------------------------------
// Dependency Graph Construction: Reference Resolution Helpers
// ----------------------------------------------------------------------------

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit0) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto *ref = DependencyGraph::FindWriteReferenceInScope("testtable", statement.RootScope());
  EXPECT_EQ(DependencyGraph::NOT_FOUND, ref);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit1) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto *ref = DependencyGraph::FindWriteReferenceInScope("x", statement.RootScope());
  EXPECT_NE(DependencyGraph::NOT_FOUND, ref);
  EXPECT_EQ(ref->Table()->GetAlias(), "x");
  EXPECT_EQ(ref->Type(), RefType::WRITE);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit2) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(i) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto *x_ref = DependencyGraph::FindWriteReferenceInScope("x", statement.RootScope());
  EXPECT_NE(DependencyGraph::NOT_FOUND, x_ref);
  EXPECT_EQ(x_ref->Table()->GetAlias(), "x");
  EXPECT_EQ(x_ref->Type(), RefType::WRITE);

  const auto *y_ref = DependencyGraph::FindWriteReferenceInScope("y", statement.RootScope());
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_ref);
  EXPECT_EQ(y_ref->Table()->GetAlias(), "y");
  EXPECT_EQ(y_ref->Type(), RefType::WRITE);

  const auto *z_ref = DependencyGraph::FindWriteReferenceInScope("z", statement.RootScope());
  EXPECT_EQ(DependencyGraph::NOT_FOUND, z_ref);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit3) {
  const std::string sql =
      "WITH x(i) AS (SELECT 1), y(i) AS (SELECT * FROM x), z(k) AS (SELECT * FROM y) SELECT * FROM z;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto *x_ref = DependencyGraph::FindWriteReferenceInScope("x", statement.RootScope());
  EXPECT_NE(DependencyGraph::NOT_FOUND, x_ref);
  EXPECT_EQ(x_ref->Table()->GetAlias(), "x");
  EXPECT_EQ(x_ref->Type(), RefType::WRITE);

  const auto *y_ref = DependencyGraph::FindWriteReferenceInScope("y", statement.RootScope());
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_ref);
  EXPECT_EQ(y_ref->Table()->GetAlias(), "y");
  EXPECT_EQ(y_ref->Type(), RefType::WRITE);

  const auto *z_ref = DependencyGraph::FindWriteReferenceInScope("z", statement.RootScope());
  EXPECT_NE(DependencyGraph::NOT_FOUND, z_ref);
  EXPECT_EQ(z_ref->Table()->GetAlias(), "z");
  EXPECT_EQ(z_ref->Type(), RefType::WRITE);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit4) {
  // Forward references
  const std::string sql =
      "WITH x(i) AS (SELECT 1), y(i) AS (SELECT * FROM x), z(k) AS (SELECT * FROM y) SELECT * FROM z;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();

  const auto *x_ref = DependencyGraph::FindWriteReferenceInScope("x", root);
  const auto *y_ref = DependencyGraph::FindWriteReferenceInScope("y", root);
  const auto *z_ref = DependencyGraph::FindWriteReferenceInScope("z", root);

  EXPECT_NE(DependencyGraph::NOT_FOUND, x_ref);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_ref);
  EXPECT_NE(DependencyGraph::NOT_FOUND, z_ref);

  // Check references from `x`
  const auto &x_scope = *x_ref->Scope();

  const auto *x_forward_from_x = DependencyGraph::FindForwardWriteReferenceInScope("x", root, x_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, x_forward_from_x);
  const auto *y_forward_from_x = DependencyGraph::FindForwardWriteReferenceInScope("y", root, x_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_forward_from_x);
  const auto *z_forward_from_x = DependencyGraph::FindForwardWriteReferenceInScope("z", root, x_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, z_forward_from_x);

  const auto *x_backward_from_x = DependencyGraph::FindBackwardWriteReferenceInScope("x", root, x_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, x_backward_from_x);
  const auto *y_backward_from_x = DependencyGraph::FindBackwardWriteReferenceInScope("y", root, x_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, y_backward_from_x);
  const auto *z_backward_from_x = DependencyGraph::FindBackwardWriteReferenceInScope("z", root, x_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, z_backward_from_x);

  // Check references from `y`
  const auto &y_scope = *y_ref->Scope();

  const auto *x_forward_from_y = DependencyGraph::FindForwardWriteReferenceInScope("x", root, y_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, x_forward_from_y);
  const auto *y_forward_from_y = DependencyGraph::FindForwardWriteReferenceInScope("y", root, y_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, y_forward_from_y);
  const auto *z_forward_from_y = DependencyGraph::FindForwardWriteReferenceInScope("z", root, y_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, z_forward_from_y);

  const auto *x_backward_from_y = DependencyGraph::FindBackwardWriteReferenceInScope("x", root, y_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, x_backward_from_y);
  const auto *y_backward_from_y = DependencyGraph::FindBackwardWriteReferenceInScope("y", root, y_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, y_backward_from_y);
  const auto *z_backward_from_y = DependencyGraph::FindBackwardWriteReferenceInScope("z", root, y_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, z_backward_from_y);

  // Check references from `z`
  const auto &z_scope = *z_ref->Scope();

  const auto *x_forward_from_z = DependencyGraph::FindForwardWriteReferenceInScope("x", root, z_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, x_forward_from_z);
  const auto *y_forward_from_z = DependencyGraph::FindForwardWriteReferenceInScope("y", root, z_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, y_forward_from_z);
  const auto *z_forward_from_z = DependencyGraph::FindForwardWriteReferenceInScope("z", root, z_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, z_forward_from_z);

  const auto *x_backward_from_z = DependencyGraph::FindBackwardWriteReferenceInScope("x", root, z_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, x_backward_from_z);
  const auto *y_backward_from_z = DependencyGraph::FindBackwardWriteReferenceInScope("y", root, z_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_backward_from_z);
  const auto *z_backward_from_z = DependencyGraph::FindBackwardWriteReferenceInScope("z", root, z_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, z_backward_from_z);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit5) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (WITH y(m) AS (SELECT 1), a(n) AS (SELECT * FROM y) SELECT * FROM a), y(j) AS (SELECT 2) "
      "SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.EnclosedScopes().size(), 2UL);

  // Descend into the scope defined by `x`
  const auto &x_scope = root.EnclosedScopes().at(0);
  EXPECT_EQ(x_scope->EnclosedScopes().size(), 2UL);
  EXPECT_EQ(x_scope->RefCount(), 3UL);
  EXPECT_EQ(x_scope->ReadRefCount(), 1UL);
  EXPECT_EQ(x_scope->WriteRefCount(), 2UL);

  // Search from the scope defined by `a`, as if we are trying to resolve `SELECT * FROM y`
  const auto *a_ref = DependencyGraph::FindWriteReferenceInScope("a", *x_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, a_ref);
  const auto &a_scope = *a_ref->Scope();

  // Attempt to resolve in precedence-order
  const auto *y_backward_from_a = DependencyGraph::FindBackwardWriteReferenceInScope("y", *x_scope, a_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_backward_from_a);
  const auto *y_upward_from_a = DependencyGraph::FindWriteReferenceInAnyEnclosingScope("y", *x_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_upward_from_a);
  const auto *y_forward_from_a = DependencyGraph::FindForwardWriteReferenceInScope("y", *x_scope, a_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, y_forward_from_a);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolutionUnit6) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (WITH a(m) AS (SELECT * FROM y), y(n) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT 2) "
      "SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.EnclosedScopes().size(), 2UL);

  // Descend into the scope defined by `x`
  const auto &x_scope = root.EnclosedScopes().at(0);
  EXPECT_EQ(x_scope->EnclosedScopes().size(), 2UL);
  EXPECT_EQ(x_scope->RefCount(), 3UL);
  EXPECT_EQ(x_scope->ReadRefCount(), 1UL);
  EXPECT_EQ(x_scope->WriteRefCount(), 2UL);

  // Search from the scope defined by `a`, as if we are trying to resolve `SELECT * FROM y`
  const auto *a_ref = DependencyGraph::FindWriteReferenceInScope("a", *x_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, a_ref);
  const auto &a_scope = *a_ref->Scope();

  // Attempt to resolve in precedence-order
  const auto *y_backward_from_a = DependencyGraph::FindBackwardWriteReferenceInScope("y", *x_scope, a_scope);
  EXPECT_EQ(DependencyGraph::NOT_FOUND, y_backward_from_a);
  const auto *y_upward_from_a = DependencyGraph::FindWriteReferenceInAnyEnclosingScope("y", *x_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_upward_from_a);
  const auto *y_forward_from_a = DependencyGraph::FindForwardWriteReferenceInScope("y", *x_scope, a_scope);
  EXPECT_NE(DependencyGraph::NOT_FOUND, y_forward_from_a);
}

// ----------------------------------------------------------------------------
// Dependency Graph Construction: Reference Resolution
// ----------------------------------------------------------------------------

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolution0) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.RefCount(), 2UL);
  EXPECT_EQ(root.ReadRefCount(), 1UL);
  EXPECT_EQ(root.WriteRefCount(), 1UL);

  // Grab a reference to the read on `x`
  const auto &x_read = *root.References().at(1);
  EXPECT_EQ(RefType::READ, x_read.Type());
  EXPECT_EQ("x", x_read.Table()->GetAlias());

  // Resolve dependencies for `x` (READ references do not introduce dependencies)
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(x_read));
  const auto deps = DependencyGraph::ResolveDependenciesFor(x_read);
  EXPECT_EQ(0UL, deps.size());
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolution1) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.RefCount(), 3UL);
  EXPECT_EQ(root.ReadRefCount(), 1UL);
  EXPECT_EQ(root.WriteRefCount(), 2UL);

  // Resolve dependencies for write on `x`
  const auto &x_write = *root.References().at(0);
  EXPECT_EQ(RefType::WRITE, x_write.Type());
  EXPECT_EQ("x", x_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(x_write));
  const auto x_write_deps = DependencyGraph::ResolveDependenciesFor(x_write);
  EXPECT_EQ(0UL, x_write_deps.size());

  // Resolve dependencies for write on `y`
  const auto &y_write = *root.References().at(1);
  EXPECT_EQ(RefType::WRITE, y_write.Type());
  EXPECT_EQ("y", y_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(y_write));
  const auto y_write_deps = DependencyGraph::ResolveDependenciesFor(y_write);
  EXPECT_EQ(1UL, y_write_deps.size());

  const auto *y_write_dep = *y_write_deps.begin();
  EXPECT_EQ(*y_write_dep, x_write);

  // Resolve dependencies for read on `y`
  const auto &y_read = *root.References().at(2);
  EXPECT_EQ(RefType::READ, y_read.Type());
  EXPECT_EQ("y", y_read.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(y_read));
  const auto deps = DependencyGraph::ResolveDependenciesFor(y_read);
  EXPECT_EQ(0UL, deps.size());
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolution2) {
  const std::string sql = "WITH RECURSIVE y(j) AS (SELECT * FROM x), x(i) AS (SELECT 1) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.RefCount(), 3UL);
  EXPECT_EQ(root.ReadRefCount(), 1UL);
  EXPECT_EQ(root.WriteRefCount(), 2UL);

  const auto &y_write = *root.References().at(0);
  const auto &x_write = *root.References().at(1);
  const auto &y_read = *root.References().at(2);

  // Resolve dependencies for write on `y`
  EXPECT_EQ(RefType::WRITE, y_write.Type());
  EXPECT_EQ("y", y_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(y_write));
  const auto y_write_deps = DependencyGraph::ResolveDependenciesFor(y_write);
  EXPECT_EQ(1UL, y_write_deps.size());

  const auto *y_write_dep = *y_write_deps.begin();
  EXPECT_EQ(*y_write_dep, x_write);

  // Resolve dependencies for write on `x`
  EXPECT_EQ(RefType::WRITE, x_write.Type());
  EXPECT_EQ("x", x_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(x_write));
  const auto x_write_deps = DependencyGraph::ResolveDependenciesFor(x_write);
  EXPECT_EQ(0UL, x_write_deps.size());

  // Resolve dependencies for read on `y`
  EXPECT_EQ(RefType::READ, y_read.Type());
  EXPECT_EQ("y", y_read.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(y_read));
  const auto deps = DependencyGraph::ResolveDependenciesFor(y_read);
  EXPECT_EQ(0UL, deps.size());
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolution3) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (WITH y(m) AS (SELECT 1), a(n) AS (SELECT * FROM y) SELECT * FROM a), y(j) AS (SELECT 2) "
      "SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.RefCount(), 3UL);
  EXPECT_EQ(root.ReadRefCount(), 1UL);
  EXPECT_EQ(root.WriteRefCount(), 2UL);
  EXPECT_EQ(root.EnclosedScopes().size(), 2UL);

  // Resolve top-level write dependencies

  const auto &x_write = *root.References().at(0);
  const auto &y_write = *root.References().at(1);

  EXPECT_EQ(RefType::WRITE, x_write.Type());
  EXPECT_EQ("x", x_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(x_write));
  const auto x_write_deps = DependencyGraph::ResolveDependenciesFor(x_write);
  EXPECT_EQ(1UL, x_write_deps.size());

  EXPECT_EQ(RefType::WRITE, y_write.Type());
  EXPECT_EQ("y", y_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(y_write));
  const auto y_write_deps = DependencyGraph::ResolveDependenciesFor(y_write);
  EXPECT_EQ(0UL, y_write_deps.size());

  // Resolve nested write dependencies

  const auto &x_scope = root.EnclosedScopes().front();
  EXPECT_EQ(x_scope->EnclosedScopes().size(), 2UL);
  EXPECT_EQ(x_scope->RefCount(), 3UL);
  EXPECT_EQ(x_scope->ReadRefCount(), 1UL);
  EXPECT_EQ(x_scope->WriteRefCount(), 2UL);

  const auto &nested_y_write = *x_scope->References().at(0);
  const auto &nested_a_write = *x_scope->References().at(1);

  EXPECT_EQ(RefType::WRITE, nested_y_write.Type());
  EXPECT_EQ("y", nested_y_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(nested_y_write));
  const auto nested_y_write_deps = DependencyGraph::ResolveDependenciesFor(nested_y_write);
  EXPECT_EQ(0UL, nested_y_write_deps.size());

  EXPECT_EQ(RefType::WRITE, nested_a_write.Type());
  EXPECT_EQ("a", nested_a_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(nested_a_write));
  const auto nested_a_write_deps = DependencyGraph::ResolveDependenciesFor(nested_a_write);
  EXPECT_EQ(1UL, nested_a_write_deps.size());

  // Resolved reference nested_a -(backward)-> nested_y
  const auto *nested_a_write_dep = *nested_a_write_deps.begin();
  EXPECT_EQ(*nested_a_write_dep, nested_y_write);

  // Resolved reference x -(local)-> nested_a
  const auto *x_write_dep = *x_write_deps.begin();
  EXPECT_EQ(*x_write_dep, nested_a_write);
}

TEST_F(BinderCteDepdendencyGraphTest, ReferenceResolution4) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (WITH a(m) AS (SELECT * FROM y), y(n) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT 2) "
      "SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  const auto &root = statement.RootScope();
  EXPECT_EQ(root.RefCount(), 3UL);
  EXPECT_EQ(root.ReadRefCount(), 1UL);
  EXPECT_EQ(root.WriteRefCount(), 2UL);
  EXPECT_EQ(root.EnclosedScopes().size(), 2UL);

  // Resolve top-level write dependencies

  const auto &x_write = *root.References().at(0);
  const auto &y_write = *root.References().at(1);

  EXPECT_EQ(RefType::WRITE, x_write.Type());
  EXPECT_EQ("x", x_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(x_write));
  const auto x_write_deps = DependencyGraph::ResolveDependenciesFor(x_write);
  EXPECT_EQ(1UL, x_write_deps.size());

  EXPECT_EQ(RefType::WRITE, y_write.Type());
  EXPECT_EQ("y", y_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(y_write));
  const auto y_write_deps = DependencyGraph::ResolveDependenciesFor(y_write);
  EXPECT_EQ(0UL, y_write_deps.size());

  // Resolve nested write dependencies

  const auto &x_scope = root.EnclosedScopes().front();
  EXPECT_EQ(x_scope->EnclosedScopes().size(), 2UL);
  EXPECT_EQ(x_scope->RefCount(), 3UL);
  EXPECT_EQ(x_scope->ReadRefCount(), 1UL);
  EXPECT_EQ(x_scope->WriteRefCount(), 2UL);

  const auto &nested_a_write = *x_scope->References().at(0);
  const auto &nested_y_write = *x_scope->References().at(1);

  EXPECT_EQ(RefType::WRITE, nested_a_write.Type());
  EXPECT_EQ("a", nested_a_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(nested_a_write));
  const auto nested_a_write_deps = DependencyGraph::ResolveDependenciesFor(nested_a_write);
  EXPECT_EQ(1UL, nested_a_write_deps.size());

  EXPECT_EQ(RefType::WRITE, nested_y_write.Type());
  EXPECT_EQ("y", nested_y_write.Table()->GetAlias());
  EXPECT_NO_THROW(DependencyGraph::ResolveDependenciesFor(nested_y_write));
  const auto nested_y_write_deps = DependencyGraph::ResolveDependenciesFor(nested_y_write);
  EXPECT_EQ(0UL, nested_y_write_deps.size());

  // Resolved: nested_a -(upward)-> y
  const auto *nested_a_write_dep = *nested_a_write_deps.begin();
  EXPECT_EQ(*nested_a_write_dep, y_write);

  // Resolved reference x -(local)-> nested_a
  const auto *x_write_dep = *x_write_deps.begin();
  EXPECT_EQ(*x_write_dep, nested_a_write);
}

// ----------------------------------------------------------------------------
// Dependency Graph Construction Success (THROW/NO_THROW)
// ----------------------------------------------------------------------------

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess0) {
  // Ambuguous reference
  const std::string sql = "WITH x(i) AS (SELECT 1), x(j) AS (SELECT 2) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_THROW(DependencyGraph::Build(select), BinderException);
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess1) {
  // Ambiguous reference
  const std::string sql = "WITH x(i) AS (WITH y(j) AS (SELECT 1), y(k) AS (SELECT 2) SELECT * FROM y) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_THROW(DependencyGraph::Build(select), BinderException);
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess2) {
  // Reference of nested definition
  const std::string sql =
      "WITH x(i) AS (SELECT * FROM a), y(j) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_THROW(DependencyGraph::Build(select), BinderException);
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess3) {
  // Reference of nested definition
  const std::string sql =
      "WITH y(j) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), x(i) AS (SELECT * FROM a) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_THROW(DependencyGraph::Build(select), BinderException);
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess4) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess5) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess6) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(i) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstructionSuccess7) {
  const std::string sql =
      "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(i) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
}

// ----------------------------------------------------------------------------
// Dependency Graph Construction (Graph Structure)
// ----------------------------------------------------------------------------

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction0) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 1UL);
  EXPECT_EQ(graph->Size(), 0UL);
  EXPECT_TRUE(graph->HasVertex({"testtable", VertexType::READ, 0UL, 0UL}));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction1) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 2UL);
  EXPECT_EQ(graph->Size(), 0UL);
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::WRITE, 0UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::READ, 0UL, 1UL}));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction2) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 4UL);
  EXPECT_EQ(graph->Size(), 1UL);

  EXPECT_TRUE(graph->HasVertex({"x", VertexType::WRITE, 0UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 0UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::READ, 0UL, 2UL}));
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::READ, 1UL, 0UL}));

  EXPECT_TRUE(graph->HasEdge(MakeEdge({"y", VertexType::WRITE, 0UL, 1UL}, {"x", VertexType::WRITE, 0UL, 0UL})));
  EXPECT_FALSE(graph->HasEdge(MakeEdge({"x", VertexType::WRITE, 0UL, 0UL}, {"y", VertexType::WRITE, 0UL, 1UL})));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction3) {
  const std::string sql = "WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 4UL);
  EXPECT_EQ(graph->Size(), 1UL);

  EXPECT_TRUE(graph->HasVertex({"x", VertexType::WRITE, 0UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 0UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::READ, 0UL, 2UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::READ, 1UL, 0UL}));

  EXPECT_TRUE(graph->HasEdge(MakeEdge({"x", VertexType::WRITE, 0UL, 0UL}, {"y", VertexType::WRITE, 0UL, 1UL})));
  EXPECT_FALSE(graph->HasEdge(MakeEdge({"y", VertexType::WRITE, 0UL, 1UL}, {"x", VertexType::WRITE, 0UL, 0UL})));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction4) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (WITH y(m) AS (SELECT 1), a(n) AS (SELECT * FROM y) SELECT * FROM a), y(j) AS (SELECT 2) "
      "SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 7UL);
  EXPECT_EQ(graph->Size(), 2UL);

  EXPECT_TRUE(graph->HasVertex({"x", VertexType::WRITE, 0UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 0UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::READ, 0UL, 2UL}));

  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 1UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"a", VertexType::WRITE, 1UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"a", VertexType::READ, 1UL, 2UL}));

  EXPECT_TRUE(graph->HasVertex({"y", VertexType::READ, 2UL, 0UL}));

  EXPECT_TRUE(graph->HasEdge(MakeEdge({"x", VertexType::WRITE, 0UL, 0UL}, {"a", VertexType::WRITE, 1UL, 1UL})));
  EXPECT_TRUE(graph->HasEdge(MakeEdge({"a", VertexType::WRITE, 1UL, 1UL}, {"y", VertexType::WRITE, 1UL, 0UL})));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction5) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (WITH a(m) AS (SELECT * FROM y), y(n) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT 2) "
      "SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 7UL);
  EXPECT_EQ(graph->Size(), 2UL);

  EXPECT_TRUE(graph->HasVertex({"x", VertexType::WRITE, 0UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 0UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::READ, 0UL, 2UL}));

  EXPECT_TRUE(graph->HasVertex({"a", VertexType::WRITE, 1UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 1UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"a", VertexType::READ, 1UL, 2UL}));

  EXPECT_TRUE(graph->HasVertex({"y", VertexType::READ, 2UL, 0UL}));

  EXPECT_TRUE(graph->HasEdge(MakeEdge({"x", VertexType::WRITE, 0UL, 0UL}, {"a", VertexType::WRITE, 1UL, 0UL})));
  EXPECT_TRUE(graph->HasEdge(MakeEdge({"a", VertexType::WRITE, 1UL, 0UL}, {"y", VertexType::WRITE, 0UL, 1UL})));
}

TEST_F(BinderCteDepdendencyGraphTest, CheckGraphConstruction6) {
  const std::string sql = "WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_EQ(graph->Order(), 5UL);
  EXPECT_EQ(graph->Size(), 2UL);

  EXPECT_TRUE(graph->HasVertex({"x", VertexType::WRITE, 0UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::WRITE, 0UL, 1UL}));
  EXPECT_TRUE(graph->HasVertex({"y", VertexType::READ, 0UL, 2UL}));

  EXPECT_TRUE(graph->HasVertex({"y", VertexType::READ, 1UL, 0UL}));
  EXPECT_TRUE(graph->HasVertex({"x", VertexType::READ, 1UL, 0UL}));

  EXPECT_TRUE(graph->HasEdge(MakeEdge({"x", VertexType::WRITE, 0UL, 0UL}, {"y", VertexType::WRITE, 0UL, 1UL})));
  EXPECT_TRUE(graph->HasEdge(MakeEdge({"y", VertexType::WRITE, 0UL, 1UL}, {"x", VertexType::WRITE, 0UL, 0UL})));
}

// ----------------------------------------------------------------------------
// Dependency Graph Validation: Mutual Recursion
// ----------------------------------------------------------------------------

TEST_F(BinderCteDepdendencyGraphTest, CheckMutualRecursion0) {
  const std::string sql = "SELECT * FROM TestTable";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_FALSE(graph->ContainsInvalidMutualRecursion());
}

TEST_F(BinderCteDepdendencyGraphTest, CheckMutualRecursion1) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_FALSE(graph->ContainsInvalidMutualRecursion());
}

TEST_F(BinderCteDepdendencyGraphTest, CheckMutualRecursion2) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_FALSE(graph->ContainsInvalidMutualRecursion());
}

TEST_F(BinderCteDepdendencyGraphTest, CheckMutualRecursion3) {
  const std::string sql = "WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_TRUE(graph->ContainsInvalidMutualRecursion());
}

TEST_F(BinderCteDepdendencyGraphTest, CheckMutualRecursion4) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM z), z(k) AS (SELECT * FROM x) SELECT * FROM z;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_NO_THROW(DependencyGraph::Build(select));
  const auto graph = DependencyGraph::Build(select);
  EXPECT_TRUE(graph->ContainsInvalidMutualRecursion());
}

// ----------------------------------------------------------------------------
// Dependency Graph Validation: Forward References
// ----------------------------------------------------------------------------

}  // namespace noisepage
