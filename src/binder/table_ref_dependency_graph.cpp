#include "binder/table_ref_dependency_graph.h"

#include "common/graph.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"

namespace noisepage::binder {

// ----------------------------------------------------------------------------
// TableDependencyGraph
// ----------------------------------------------------------------------------

TableRefDependencyGraph::TableRefDependencyGraph(common::ManagedPointer<parser::SelectStatement> root_select,
                                                 common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor)
    : catalog_accessor_{catalog_accessor} {
  // TODO(Kyle): Are there cases in which the root SELECT does not have a table?
  if (root_select->HasSelectTable()) {
    // The root SELECT statement cannot have an dependencies
    graph_[root_select->GetSelectTable()] = {};
  }

  // Visit each of the top-level references
  TableRefDependencyGraphVisitor visitor{&graph_};
  for (const auto &ref : root_select->GetSelectWith()) {
    visitor.Visit(ref);
  }

  // Filter out the dependencies on existing (non-temporary) tables
  (void)catalog_accessor_;
}

common::Graph TableRefDependencyGraph::ToGraph(
    std::unordered_map<std::size_t, common::ManagedPointer<parser::TableRef>> *metadata) const {
  // Construct an (arbitrary) mapping from vertex identifiers to table references;
  // this is necesary because after we use the generic Graph representation to
  // run various graph algorithms, we want to be able to come back to table references
  return common::Graph{};
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
