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

// ----------------------------------------------------------------------------
// Construction
// ----------------------------------------------------------------------------

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
  // The statement maintains ownership over most of the data
  // used by the dependency graph, so we need ownership of it
  statement_ = std::move(statement);

  // Perform some basic validation prior to resolving dependencies;
  // throws if the input structured statement is invalid, attempting
  // to build a dependency graph from an invalid statement is hopeless
  ValidateStructuredStatement(*statement_);

  for (auto *table_ref : statement_->MutableReferences()) {
    graph_[table_ref] = ResolveDependenciesFor(*table_ref);
  }
}

// ----------------------------------------------------------------------------
// StructuredStatement Validation
// ----------------------------------------------------------------------------

void DependencyGraph::ValidateStructuredStatement(const StructuredStatement &statement) {
  // Ensure that none of the scopes in the structured statement contain duplicate aliases
  if (ContainsAmbiguousReferences(statement.RootScope())) {
    throw BINDER_EXCEPTION("Ambiguous Table Reference", common::ErrorCode::ERRCODE_DUPLICATE_TABLE);
  }
}

bool DependencyGraph::ContainsAmbiguousReferences(const LexicalScope &scope) {
  std::unordered_set<std::string> read_aliases{};
  std::unordered_set<std::string> write_aliases{};
  for (const auto &table_ref : scope.References()) {
    if (table_ref.Type() == RefType::READ) {
      read_aliases.insert(table_ref.Table()->GetAlias());
    } else if (table_ref.Type() == RefType::WRITE) {
      write_aliases.insert(table_ref.Table()->GetAlias());
    }
  }
  const auto contains_ambiguous_ref =
      (scope.ReadRefCount() > read_aliases.size()) || (scope.WriteRefCount() > write_aliases.size());
  return contains_ambiguous_ref || std::any_of(scope.EnclosedScopes().cbegin(), scope.EnclosedScopes().cend(),
                                               [](const LexicalScope &s) { return ContainsAmbiguousReferences(s); });
}

// ----------------------------------------------------------------------------
// Reference Resolution
// ----------------------------------------------------------------------------

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

  /**
   * To resolve a dependency, we need to locate the correct WRITE table reference
   * that defines the table read by the current table reference in question.
   *
   * The priority for resolution of references goes like:
   *
   *  1. WRITE table references (WITH ...) in the same scope
   *  2. WRITE table references (non-forward) defined in the enclosing scope
   *  3. WRITE table references (any) in scopes that enclose the enclosing scope (if present)
   *  4. WRITE table references (forward) defined in the enclosing scope
   *
   * Naturally, referring to temporary tables in "lower" scopes is an error.
   *
   * TODO(Kyle): I derived these rules from experimenting with the Postgres implementation
   * of common table expressions, recursive and non-recursive, rather than actually looking
   * at the grammar defined in the SQL standard. At the same time, I feel like there must
   * be a more elegant way to express the rules for reference resolution... For instance,
   * prioritizing dependent references in the order in which they appear in a depth-first
   * traversal of the tree represented by the structured statement ALMOST works, but breaks
   * down because of the precendence of rules 3. and 4. above.
   */

  // 1. WRITE table references in the same scope

  const auto &target_alias = table_ref.Table()->GetAlias();
  const auto *enclosing_scope = table_ref.EnclosingScope();
  auto it = std::find_if(enclosing_scope->References().cbegin(), enclosing_scope->References().cend(),
                         [&target_alias](const ContextSensitiveTableRef &r) {
                           return r.Type() == RefType::WRITE && r.Table()->GetAlias() == target_alias;
                         });
  if (it != enclosing_scope->References().cend()) {
    // Found it
    return std::addressof(*it);
  }

  // 2. WRITE table references (non-forward) defined in the enclosing scope

  if (!enclosing_scope->HasEnclosingScope()) {
    throw BINDER_EXCEPTION("Table Not Found", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
  }

  throw BINDER_EXCEPTION("Table Not Found", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
}

// ----------------------------------------------------------------------------
// Reference Resolution
// ----------------------------------------------------------------------------

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
