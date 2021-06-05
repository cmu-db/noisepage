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
// TableReference
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
  statement_ = std::move(statement);
  // Populate the graph with all of the WRITE references in the statement
  PopulateGraphVisit(statement_->RootScope());
  // Populate the dependencies for each of the references in the graph
}

void DependencyGraph::PopulateGraphVisit(const LexicalScope &scope) {
  for (const auto &enclosed_scope : scope.EnclosedScopes()) {
    PopulateGraphVisit(enclosed_scope);
  }
}

std::size_t DependencyGraph::Order() const { return graph_.size(); }

std::size_t DependencyGraph::Size() const {
  using EntryType = std::pair<ContextSensitiveTableRef *, std::vector<ContextSensitiveTableRef *>>;
  return std::transform_reduce(graph_.cbegin(), graph_.cend(), 0UL, std::plus{},
                               [](const EntryType &entry) { return entry.second.size(); });
}

bool DependencyGraph::HasVertex(const Vertex &vertex) const { return false; }

bool DependencyGraph::HasEdge(const Edge &edge) const { return false; }

bool DependencyGraph::CheckAll() const { return true; }

bool DependencyGraph::CheckForwardReferences() const { return true; }

bool DependencyGraph::CheckMutualRecursion() const { return true; }

}  // namespace noisepage::binder::cte
