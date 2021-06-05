#include "binder/cte/dependency_graph.h"

#include <algorithm>
#include <numeric>

#include "binder/cte/context_sensitive_table_ref.h"
#include "binder/cte/lexical_scope.h"
#include "binder/cte/structured_statement.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "parser/table_ref.h"

namespace noisepage::binder::cte {

std::unique_ptr<DependencyGraph> DependencyGraph::Build(common::ManagedPointer<parser::SelectStatement> root) {
  return std::make_unique<DependencyGraph>(std::make_unique<StructuredStatement>(root));
}

std::unique_ptr<DependencyGraph> DependencyGraph::Build(common::ManagedPointer<parser::InsertStatement> root) {
  return std::make_unique<DependencyGraph>(std::make_unique<StructuredStatement>(root));
}

std::unique_ptr<DependencyGraph> DependencyGraph::Build(common::ManagedPointer<parser::UpdateStatement> root) {
  return std::make_unique<DependencyGraph>(std::make_unique<StructuredStatement>(root));
}

std::unique_ptr<DependencyGraph> DependencyGraph::Build(common::ManagedPointer<parser::DeleteStatement> root) {
  return std::make_unique<DependencyGraph>(std::make_unique<StructuredStatement>(root));
}

DependencyGraph::DependencyGraph(std::unique_ptr<StructuredStatement> &&statement) {
  statement_ = std::move(statement);
  for (auto *table_ref : statement_->MutableReferences()) {
    graph_[table_ref] = ResolveDependenciesFor(*table_ref);
  }
}

std::unordered_set<const ContextSensitiveTableRef *> DependencyGraph::ResolveDependenciesFor(
    const ContextSensitiveTableRef &table_ref) const {
  if (table_ref.Type() == RefType::READ) {
    // READ references cannot introduce a dependency
    return {};
  }

  // Locate the scope defined by this table reference
  const auto *defined_scope = table_ref.Scope();
  NOISEPAGE_ASSERT(defined_scope != nullptr, "WRITE references must define a scope");

  // For each READ reference in the defined scope, resolve it to the
  // corresponding WRITE reference that defines the read table
  std::unordered_set<const ContextSensitiveTableRef *> dependencies{};
  for (const auto ref : defined_scope->References()) {
    if (ref.Type() == RefType::READ) {
      dependencies.insert(ResolveDependency(ref));
    }
  }

  return dependencies;
}

const ContextSensitiveTableRef *DependencyGraph::ResolveDependency(const ContextSensitiveTableRef &table_ref) const {
  NOISEPAGE_ASSERT(table_ref.Type() == RefType::READ, "Resolving WRITE reference dependencies is ambiguous");

  // To resolve a dependency, we need to locate the correct WRITE table reference
  // that defines the table read by the current table reference in question.
  //
  // The priority for resolution of references goes like:
  //
  //  1. WRITE table references (WITH ...) in the same scope
  //  2. WRITE table references defined in an enclosing scope
  //
  // Naturally, referring to temporary tables in "lower" scopes is an error.

  // 1. WRITE table references in the same scope

  const auto &target_alias = table_ref.Table()->GetAlias();
  const auto *enclosing_scope = table_ref.EnclosingScope();
  const std::size_t n_matches =
      std::count_if(enclosing_scope->References().cbegin(), enclosing_scope->References().cend(),
                    [&target_alias](const ContextSensitiveTableRef &r) {
                      return r.Type() == RefType::WRITE && r.Table()->GetAlias() == target_alias;
                    });
  if (n_matches > 1UL) {
    throw BINDER_EXCEPTION("Ambiguous Table Reference", common::ErrorCode::ERRCODE_DUPLICATE_TABLE);
  }

  auto it = std::find_if(enclosing_scope->References().cbegin(), enclosing_scope->References().cend(),
                         [&target_alias](const ContextSensitiveTableRef &r) {
                           return r.Type() == RefType::WRITE && r.Table()->GetAlias() == target_alias;
                         });
  if (it != enclosing_scope->References().cend()) {
    // Found it
    return std::addressof(*it);
  }

  for (const auto &ref : enclosing_scope->References()) {
    if (ref.Type() == RefType::WRITE && ref.Table()->GetAlias() == target_alias) {
      // Found it
      return &ref;
    }
  }

  // 2. WRITE table references defined in an enclosing scope
  // TODO(Kyle): This.

  throw BINDER_EXCEPTION("Table Not Found", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
}

void DependencyGraph::PopulateGraphVisit(LexicalScope &scope) {
  for (auto &enclosed_scope : scope.EnclosedScopes()) {
    PopulateGraphVisit(enclosed_scope);
  }
  for (auto &table_ref : scope.References()) {
    // Each reference is initialized with an empty set of dependencies
    graph_[&table_ref] = {};
  }
}

std::size_t DependencyGraph::Order() const { return graph_.size(); }

std::size_t DependencyGraph::Size() const {
  return std::transform_reduce(graph_.cbegin(), graph_.cend(), 0UL, std::plus{},
                               [](const EntryType &entry) { return entry.second.size(); });
}

bool DependencyGraph::HasVertex(const Vertex &vertex) const { return false; }

bool DependencyGraph::HasEdge(const Edge &edge) const { return false; }

bool DependencyGraph::CheckAll() const { return true; }

bool DependencyGraph::CheckForwardReferences() const { return true; }

bool DependencyGraph::CheckMutualRecursion() const { return true; }

}  // namespace noisepage::binder::cte
