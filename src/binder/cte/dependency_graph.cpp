#include "binder/cte/dependency_graph.h"

#include <algorithm>

namespace noisepage::binder::cte {

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::SelectStatement> root) {
  // Once we have the collection of raw dependencies, we perform
  // some additional processing to resolve dependencies and structure
  // them in a way that is amenable to the validation queries

  // for (const auto &table_ref : context.Refs()) {
  //   // We only care about WRITE references (i.e. those that "define" a temporary table)
  //   if (table_ref.Type() == detail::RefType::WRITE) {
  //     graph_.emplace_back(table_ref.Id(), table_ref.Type(), table_ref.Depth(), table_ref.Position(),
  //     table_ref.Table()); auto &new_vertex = graph_.back();
  //     // Resolve the dependencies for this table reference
  //     for (const auto id : table_ref.Dependencies()) {
  //       ResolveReference(id, context);
  //       new_vertex.AddDependency(0);
  //     }
  //   }
  // }
}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::InsertStatement> root) {}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::UpdateStatement> root) {}

DependencyGraph::DependencyGraph(common::ManagedPointer<parser::DeleteStatement> root) {}

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
