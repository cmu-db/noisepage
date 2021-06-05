#include "binder/cte/dependency_graph.h"

#include <algorithm>

#include "binder/cte/context_sensitive_table_ref.h"
#include "binder/cte/structured_statement.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "parser/table_ref.h"

namespace noisepage::binder::cte {

DependencyGraph::DependencyGraph(const StructuredStatement &statement) {}

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
  return nullptr;
}

std::size_t DependencyGraph::Order() const { return 0UL; }

std::size_t DependencyGraph::Size() const { return 0UL; }

bool DependencyGraph::HasVertex(const Vertex &vertex) const { return false; }

bool DependencyGraph::HasEdge(const Edge &edge) const { return false; }

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

const ContextSensitiveTableRef *DependencyGraph::GetVertex(const Vertex &vertex) const { return nullptr; }

std::pair<const ContextSensitiveTableRef *, const ContextSensitiveTableRef *> DependencyGraph::GetEdge(
    const Edge &edge) const {
  return std::make_pair(nullptr, nullptr);
}
}  // namespace noisepage::binder::cte
