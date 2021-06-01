#include "binder/table_ref_dependency_graph.h"

#include "parser/select_statement.h"
#include "parser/table_ref.h"

namespace noisepage::binder {

// ----------------------------------------------------------------------------
// AliasAdjacencyList
// ----------------------------------------------------------------------------

void AliasAdjacencyList::AddEdge(const std::string &src, const std::string &dst) { adjacency_list_[src].insert(dst); }

std::vector<std::string> AliasAdjacencyList::Nodes() const {
  std::vector<std::string> nodes{};
  nodes.reserve(adjacency_list_.size());
  for (auto &[k, _] : adjacency_list_) {
    nodes.push_back(k);
  }
  return nodes;
}

const std::set<std::string> &AliasAdjacencyList::AdjacenciesFor(const std::string &alias) const {
  NOISEPAGE_ASSERT(adjacency_list_.find(alias) != adjacency_list_.cend(), "Alias not present in adjacency list");
  return adjacency_list_.at(alias);
}

bool AliasAdjacencyList::Equals(const AliasAdjacencyList &lhs, const AliasAdjacencyList &rhs) {
  auto lhs_nodes = lhs.Nodes();
  auto rhs_nodes = rhs.Nodes();
  std::sort(lhs_nodes.begin(), lhs_nodes.end());
  std::sort(rhs_nodes.begin(), rhs_nodes.end());

  // Ensure that the vertex set of each adjacency list is equivalent
  std::vector<std::string> difference{};
  std::set_symmetric_difference(lhs_nodes.cbegin(), lhs_nodes.cend(), rhs_nodes.cbegin(), rhs_nodes.cend(),
                                std::back_inserter(difference));
  if (!difference.empty()) {
    return false;
  }

  // Ensure the adjacencies for each node are equivalent
  for (const auto &alias : lhs_nodes) {
    const auto &lhs_edges = lhs.AdjacenciesFor(alias);
    const auto &rhs_edges = rhs.AdjacenciesFor(alias);
    std::set_symmetric_difference(lhs_edges.cbegin(), lhs_edges.cend(), rhs_edges.cbegin(), rhs_edges.cend(),
                                  std::back_inserter(difference));
    if (!difference.empty()) {
      return false;
    }
  }

  return true;
}

// ----------------------------------------------------------------------------
// TableDependencyGraph
// ----------------------------------------------------------------------------

TableRefDependencyGraph::TableRefDependencyGraph(
    const std::vector<common::ManagedPointer<parser::TableRef>> &table_refs,
    common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor)
    : catalog_accessor_{catalog_accessor} {
  // Visit each of the top-level references
  TableRefDependencyGraphVisitor visitor{&graph_};
  for (const auto &ref : table_refs) {
    visitor.Visit(ref);
  }

  // Filter out the dependencies on existing (non-temporary) tables
  (void) catalog_accessor_;
}

AliasAdjacencyList TableRefDependencyGraph::AdjacencyList() const {
  AliasAdjacencyList adjacency_list{};
  for (const auto &[table_ref, dependencies] : graph_) {
    for (const auto &dep : dependencies) {
      adjacency_list.AddEdge(table_ref->GetAlias(), dep->GetAlias());
    }
  }
  return adjacency_list;
}

// ----------------------------------------------------------------------------
// TableDependencyGraphVisitor
// ----------------------------------------------------------------------------

TableRefDependencyGraphVisitor::TableRefDependencyGraphVisitor(TableRefDependencyGraphVisitor::GraphType *graph)
    : graph_{graph} {}

void TableRefDependencyGraphVisitor::Visit(common::ManagedPointer<parser::TableRef> table_ref) {
  NOISEPAGE_ASSERT(table_ref->HasSelect(),
                   "Attempt to compute table reference dependencies for non-CTE table reference");
  // Update the context for this table reference
  current_ref_ = table_ref;
  // Populate dependencies for the "default" SELECT
  Visit(table_ref->GetSelect());
  if (table_ref->GetSelect()->HasUnionSelect()) {
    // Populate dependencies for UNION SELECT, if present (e.g. recursive CTEs)
    Visit(table_ref->GetSelect()->GetUnionSelect());
  }
}

void TableRefDependencyGraphVisitor::Visit(common::ManagedPointer<parser::SelectStatement> select) {
  // Recursively consider all nested table references
  for (const auto &nested_ref : select->GetSelectWith()) {
    Visit(nested_ref);
  }

  // Populate the dependencies for the SELECT at this level;
  // here, we always add the SELECT table to the collection of
  // dependencies for this particular table reference, regardless
  // of the "type" of table reference that it is, because at this
  // point in processing some table references produced by CTEs
  // may not be bound as CTEs yet, so we can't discriminate by type
  if (select->HasSelectTable()) {
    (*graph_)[current_ref_].insert(select->GetSelectTable());
  }
}

}  // namespace noisepage::binder
