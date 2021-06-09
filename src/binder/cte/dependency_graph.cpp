#include "binder/cte/dependency_graph.h"

#include <algorithm>
#include <numeric>

#include "binder/cte/context_sensitive_table_ref.h"
#include "binder/cte/lexical_scope.h"
#include "binder/cte/structured_statement.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/graph.h"
#include "common/graph_algorithm.h"
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
    if (table_ref->Type() == RefType::READ) {
      read_aliases.insert(table_ref->Table()->GetAlias());
    } else if (table_ref->Type() == RefType::WRITE) {
      write_aliases.insert(table_ref->Table()->GetAlias());
    }
  }
  const auto contains_ambiguous_ref =
      (scope.ReadRefCount() > read_aliases.size()) || (scope.WriteRefCount() > write_aliases.size());
  return contains_ambiguous_ref ||
         std::any_of(scope.EnclosedScopes().cbegin(), scope.EnclosedScopes().cend(),
                     [](const std::unique_ptr<LexicalScope> &s) { return ContainsAmbiguousReferences(*s); });
}

// ----------------------------------------------------------------------------
// Reference Resolution
// ----------------------------------------------------------------------------

std::unordered_set<const ContextSensitiveTableRef *> DependencyGraph::ResolveDependenciesFor(
    const ContextSensitiveTableRef &table_ref) {
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
  for (const auto &ref : defined_scope->References()) {
    if (ref->Type() == RefType::READ) {
      dependencies.insert(ResolveReference(*ref));
    }
  }

  return dependencies;
}

const ContextSensitiveTableRef *DependencyGraph::ResolveReference(const ContextSensitiveTableRef &table_ref) {
  NOISEPAGE_ASSERT(table_ref.Type() == RefType::READ, "Resolving WRITE reference dependencies is ambiguous");

  /**
   * To resolve a dependency, we need to locate the correct WRITE table reference
   * that defines the table read by the current table reference in question.
   *
   * The priority for resolution of references goes like:
   *
   *  1. WRITE table references (WITH ...) in the same scope
   *  2. WRITE table references (backward) defined in the enclosing scope
   *  3. WRITE table references (any) defined in scopes that enclose the enclosing scope (if present)
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

  const auto &target_alias = table_ref.Table()->GetAlias();

  // `scope` is the scope that conatins the READ reference that we want to resolve
  const auto &scope = *table_ref.EnclosingScope();

  // 1. WRITE table references defined in the same scope

  const auto *local_ref = FindWriteReferenceInScope(target_alias, scope);
  if (local_ref != NOT_FOUND) {
    return local_ref;
  }

  if (!scope.HasEnclosingScope()) {
    throw BINDER_EXCEPTION("Table Not Found", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
  }

  // 2. WRITE table references (backward) defined in the enclosing scope

  const auto *backward_ref = FindBackwardWriteReferenceInScope(target_alias, *scope.EnclosingScope(), scope);
  if (backward_ref != NOT_FOUND) {
    return backward_ref;
  }

  // 3. WRITE table reference (any) defined in scopes that enclose the enclosing scope

  const auto *upward_ref = FindWriteReferenceInAnyEnclosingScope(target_alias, *scope.EnclosingScope());
  if (upward_ref != NOT_FOUND) {
    return upward_ref;
  }

  // 4. WRITE table references (forward) defined in the enclosing scope

  const auto *forward_ref = FindForwardWriteReferenceInScope(target_alias, *scope.EnclosingScope(), scope);
  if (forward_ref != NOT_FOUND) {
    return forward_ref;
  }

  throw BINDER_EXCEPTION("Table Not Found", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
}

const ContextSensitiveTableRef *DependencyGraph::FindWriteReferenceInScope(std::string_view alias,
                                                                           const LexicalScope &scope) {
  auto it = std::find_if(scope.References().cbegin(), scope.References().cend(),
                         [&alias](const std::unique_ptr<ContextSensitiveTableRef> &r) {
                           return r->Type() == RefType::WRITE && r->Table()->GetAlias() == alias;
                         });
  return (it == scope.References().cend()) ? NOT_FOUND : (*it).get();
}

const ContextSensitiveTableRef *DependencyGraph::FindWriteReferenceInAnyEnclosingScope(std::string_view alias,
                                                                                       const LexicalScope &scope) {
  auto *upward_scope = scope.EnclosingScope();
  while (upward_scope != LexicalScope::GLOBAL_SCOPE) {
    const auto *upward_ref = FindWriteReferenceInScope(alias, *upward_scope);
    if (upward_ref != NOT_FOUND) {
      return upward_ref;
    }
    // Traverse upward to the enclosing scope
    upward_scope = upward_scope->EnclosingScope();
  }
  return NOT_FOUND;
}

const ContextSensitiveTableRef *DependencyGraph::FindForwardWriteReferenceInScope(std::string_view alias,
                                                                                  const LexicalScope &scope,
                                                                                  const LexicalScope &partition_point) {
  // Locate the partition point within the scope
  auto partition =
      std::find_if(scope.EnclosedScopes().cbegin(), scope.EnclosedScopes().cend(),
                   [&partition_point](const std::unique_ptr<LexicalScope> &s) { return *s == partition_point; });
  NOISEPAGE_ASSERT(partition != scope.EnclosedScopes().cend(), "Partition point must be present in scope");

  // Compute an iterator into the references collection
  const auto pos = std::distance(scope.EnclosedScopes().begin(), partition);
  auto begin = scope.References().cbegin();
  // Advance 1 passed the partition point so that it is not considered in the search
  std::advance(begin, pos + 1);

  // Search the appropriate range for the target alias
  auto it =
      std::find_if(begin, scope.References().cend(), [&alias](const std::unique_ptr<ContextSensitiveTableRef> &r) {
        return r->Type() == RefType::WRITE && r->Table()->GetAlias() == alias;
      });
  return (it == scope.References().cend()) ? NOT_FOUND : (*it).get();
}

const ContextSensitiveTableRef *DependencyGraph::FindBackwardWriteReferenceInScope(
    std::string_view alias, const LexicalScope &scope, const LexicalScope &partition_point) {
  // Locate the partition point within the scope
  auto partition =
      std::find_if(scope.EnclosedScopes().cbegin(), scope.EnclosedScopes().cend(),
                   [&partition_point](const std::unique_ptr<LexicalScope> &s) { return *s == partition_point; });
  NOISEPAGE_ASSERT(partition != scope.EnclosedScopes().cend(), "Partition point must be present in scope");

  // Compute an iterator into the references collection
  const auto pos = std::distance(scope.EnclosedScopes().cbegin(), partition);
  auto end = scope.References().cbegin();
  std::advance(end, pos);

  // Search the appropriate range for the target alias
  auto it =
      std::find_if(scope.References().cbegin(), end, [&alias](const std::unique_ptr<ContextSensitiveTableRef> &r) {
        return r->Type() == RefType::WRITE && r->Table()->GetAlias() == alias;
      });
  return (it == end) ? NOT_FOUND : (*it).get();
}

// ----------------------------------------------------------------------------
// Graph Queries
// ----------------------------------------------------------------------------

std::size_t DependencyGraph::Order() const { return graph_.size(); }

std::size_t DependencyGraph::Size() const {
  return std::transform_reduce(graph_.cbegin(), graph_.cend(), 0UL, std::plus{},
                               [](const EntryType &entry) { return entry.second.size(); });
}

bool DependencyGraph::HasVertex(const Vertex &vertex) const {
  // Map the vertex type to its corresponding reference type
  const auto ref_type = (vertex.Type() == VertexType::READ) ? RefType::READ : RefType::WRITE;
  for (const auto &[ref, _] : graph_) {
    if (ref->Type() == ref_type && ref->Table()->GetAlias() == vertex.Alias()) {
      const auto &scope = *ref->EnclosingScope();
      if (scope.Depth() == vertex.Depth() && scope.PositionOf(vertex.Alias(), ref_type) == vertex.Position()) {
        return true;
      }
    }
  }
  // Not found
  return false;
}

bool DependencyGraph::HasEdge(const Edge &edge) const {
  if (!HasVertex(edge.Source()) || !HasVertex(edge.Destination())) {
    return false;
  }
  const auto &src = edge.Source();
  const auto &dst = edge.Destination();

  const auto src_type = (src.Type() == VertexType::READ) ? RefType::READ : RefType::WRITE;
  const auto dst_type = (dst.Type() == VertexType::READ) ? RefType::READ : RefType::WRITE;

  auto *src_ref = FindRef(src.Alias(), src_type, src.Depth(), src.Position());
  auto *dst_ref = FindRef(dst.Alias(), dst_type, dst.Depth(), dst.Position());

  return (graph_.at(src_ref).count(dst_ref) > 0UL);
}

ContextSensitiveTableRef *DependencyGraph::FindRef(std::string_view alias, RefType type, const std::size_t depth,
                                                   const std::size_t position) const {
  for (auto &[ref, _] : graph_) {
    if (ref->Type() == type && ref->Table()->GetAlias() == alias) {
      const auto &scope = *ref->EnclosingScope();
      if (scope.Depth() == depth && scope.PositionOf(alias, type) == position) {
        return ref;
      }
    }
  }
  UNREACHABLE("Reference Does Not Exist");
}

// ----------------------------------------------------------------------------
// Graph Validation
// ----------------------------------------------------------------------------

bool DependencyGraph::Validate() const {
  const std::vector<bool> violations{ContainsInvalidForwardReference(), ContainsInvalidMutualRecursion()};
  return std::none_of(violations.cbegin(), violations.cend(), [](const bool r) { return r; });
}

bool DependencyGraph::ContainsInvalidForwardReference() const { return false; }

bool DependencyGraph::ContainsInvalidMutualRecursion() const {
  // To check for mutual recursion among CTEs in the query,
  // we construct an abstract graph from the dependency
  // graph we have constructed and check for cycles in this
  // abstract representation. If a cycle is present, we
  // report that the query contains mutual recursion.

  // In order to construct an abstract Graph representation,
  // we need to map each of the entries to a unique identifier.
  std::unordered_map<const ContextSensitiveTableRef *, std::size_t> ids{};
  std::size_t id{0};
  for (const auto &[ref, _] : graph_) {
    ids[ref] = id++;
  }

  // Now we can construct the graph itself
  common::Graph graph{};
  for (const auto &[ref, deps] : graph_) {
    const auto src_id = ids.at(ref);
    for (const auto *dep : deps) {
      const auto dst_id = ids.at(dep);
      graph.AddEdge(src_id, dst_id);
    }
  }

  NOISEPAGE_ASSERT(graph.Size() == Size(),
                   "The size of the abstract graph should be equivalent to the size of the dependency graph");

  // With the graph constructed, all that remains
  // is to determine if the graph contains a cycle
  return common::graph::HasCycle(graph);
}

}  // namespace noisepage::binder::cte
