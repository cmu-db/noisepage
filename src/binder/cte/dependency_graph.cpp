#include "binder/cte/dependency_graph.h"

#include <algorithm>

#include "binder/cte/context_sensitive_table_ref.h"
#include "binder/cte/structured_statement.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "parser/table_ref.h"

namespace noisepage::binder::cte {

DependencyGraph::DependencyGraph(const StructuredStatement &statement) {
  // Once we have the collection of raw dependencies, we perform
  // some additional processing to resolve dependencies and structure
  // them in a way that is amenable to the validation queries

  const auto &scope_to_depth = statement.Scopes();
  const auto &references = statement.References();

  std::size_t scope_id = 0;
  std::unordered_map<std::size_t, LexicalScope> scopes{};

  // Temporarily track the originating reference
  std::unordered_map<const TableReference *, const ContextSensitiveTableRef *> backpointers{};

  // Populate all of our references
  for (const auto &table_ref : references) {
    // We only care about WRITE references (table definitions)
    // for the purposes of computing the dependency graph
    if (table_ref.Type() == RefType::WRITE) {
      if (scopes.find(table_ref.Scope()) == scopes.end()) {
        scopes[table_ref.Scope()] = LexicalScope{scope_id++, scope_to_depth.at(table_ref.Scope())};
      }

      graph_.emplace_back(TableReference{scopes.at(table_ref.Scope()), table_ref.Position(), table_ref.Table()});
      backpointers[&graph_.back()] = &table_ref;
    }
  }

  // Populate dependencies for each reference
  for (auto &ref : graph_) {
    const auto &original_ref = *backpointers.at(&ref);
    for (const auto dep_id : original_ref.Dependencies()) {
      ref.AddDependency(Resolve(dep_id, references, backpointers));
    }
  }
}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::SelectStatement> root)
    : DependencyGraph{StructuredStatement{root}} {}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::InsertStatement> root)
    : DependencyGraph{StructuredStatement{root}} {}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::UpdateStatement> root)
    : DependencyGraph{StructuredStatement{root}} {}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::DeleteStatement> root)
    : DependencyGraph{StructuredStatement{root}} {}

const DependencyGraph::TableReference *DependencyGraph::Resolve(
    const std::size_t id, const std::vector<ContextSensitiveTableRef> &references,
    const std::unordered_map<const TableReference *, const ContextSensitiveTableRef *> &backpointers) const {
  // Locate the original dependency in the collection of references
  auto it = std::find_if(references.cbegin(), references.cend(),
                         [=](const ContextSensitiveTableRef &r) { return r.Id() == id; });
  NOISEPAGE_ASSERT(it != references.cend(), "Broken Invariant");

  // WRITE references (i.e. those that define a temporary table)
  // always depend on a READ reference - the SELECT (or SELECTs)
  // that define the temporary table; we now have this dependency.

  // From here, we need to locate the WRITE reference that produces
  // the table read by this READ reference; we accomplish this by
  // searching the current scope for aliases that match the READ
  // reference; if we don't find it there, we progressively search
  // enclosing scopes up to the root scope. If no match is found
  // during this search, this is an error on the part of the user.

  const auto &read_ref = *it;
  const auto target_alias = read_ref.Table()->GetAlias();

  auto current_scope = read_ref.Scope();
  while (current_scope >= 0UL) {
    const auto n_matches =
        std::count_if(references.cbegin(), references.cend(), [&](const ContextSensitiveTableRef &r) {
          return r.Scope() == current_scope && r.Type() == RefType::WRITE && r.Table()->GetAlias() == target_alias;
        });
    if (n_matches > 1) {
      // Ambiguous
      throw BINDER_EXCEPTION("Ambiguous Table Reference", common::ErrorCode::ERRCODE_DUPLICATE_TABLE);
    }

    auto it = std::find_if(references.cbegin(), references.cend(), [&](const ContextSensitiveTableRef &r) {
      return r.Scope() == current_scope && r.Type() == RefType::WRITE && r.Table()->GetAlias() == target_alias;
    });
    if (it != references.cend()) {
      // Successfully resolved the reference
      // TODO(Kyle): A more efficient way to do this...
      for (auto &t : graph_) {
        if (backpointers.at(&t) == std::addressof(*it)) {
          return std::addressof(t);
        }
      }
      UNREACHABLE("Broken Invariant");
    }

    // Not found; continue in the enclosing scope
    --current_scope;
  }

  throw BINDER_EXCEPTION("Failed to Resolve Table Reference", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
}

std::size_t DependencyGraph::Order() const { return 0UL; }

std::size_t DependencyGraph::Size() const { return 0UL; }

bool DependencyGraph::HasVertex(const Vertex &vertex) const {
  // return std::any_of(graph_.cbegin(), graph_.cend(), [&vertex](const detail::ContextSensitiveTableRef &r) {
  //   return r.Table()->GetAlias() == vertex.Alias() && r.Depth() == vertex.Depth() && r.Position() ==
  //   vertex.Position();
  // });
  return false;
}

bool DependencyGraph::HasEdge(const Edge &edge) const {
  // if (!HasVertex(edge.Source()) || !HasVertex(edge.Destination())) {
  //   return false;
  // }
  // auto *src = GetVertex(edge.Source());
  // auto *dst = GetVertex(edge.Destination());
  // return src->Dependencies().count(dst->Id()) > 0;
  return false;
}

bool DependencyGraph::CheckAll() const { return true; }

bool DependencyGraph::CheckForwardReferences() const { return true; }

bool DependencyGraph::CheckMutualRecursion() const { return true; }

bool DependencyGraph::CheckNestedScopes() const {
  // Checking for nested-scope dependencies boils down to
  // checking each edge in the graph and verifying that it
  // is NEVER the case that the source vertex has a depth
  // that is strictly less than the destination
  return true;
}

const ContextSensitiveTableRef *DependencyGraph::GetVertex(const Vertex &vertex) const {
  // auto it = std::find_if(graph_.begin(), graph_.end(), [&vertex](const detail::ContextSensitiveTableRef &r) {
  //   return r.Table()->GetAlias() == vertex.Alias() && r.Depth() == vertex.Depth() && r.Position() ==
  //   vertex.Position();
  // });
  // return (it == graph_.end()) ? nullptr : std::addressof(*it);
  return nullptr;
}

std::pair<const ContextSensitiveTableRef *, const ContextSensitiveTableRef *> DependencyGraph::GetEdge(
    const Edge &edge) const {
  // return std::make_pair(GetVertex(edge.Source()), GetVertex(edge.Destination()));
  return std::make_pair(nullptr, nullptr);
}
}  // namespace noisepage::binder::cte
