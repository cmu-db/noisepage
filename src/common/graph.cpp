#include "common/graph.h"

#include <numeric>

#include "common/graph_algorithm.h"
#include "common/macros.h"

namespace noisepage::common {

void Graph::AddVertex(const std::size_t id) {
  if (graph_.find(id) == graph_.cend()) {
    graph_[id] = {};
  }
}

void Graph::AddEdge(const std::size_t src_id, const std::size_t dst_id) {
  // TODO(Kyle): Should we allow self-edges?
  graph_[src_id].insert(dst_id);
  if (graph_.find(dst_id) == graph_.cend()) {
    graph_[dst_id] = {};
  }
}

std::size_t Graph::Order() const { return graph_.size(); }

std::size_t Graph::Size() const {
  return std::transform_reduce(
      graph_.cbegin(), graph_.cend(), 0UL, std::plus{},
      [](const std::pair<std::size_t, std::unordered_set<std::size_t>> &kv) { return kv.second.size(); });
}

std::vector<std::size_t> Graph::VertexSet() const {
  std::vector<std::size_t> vertex_set{};
  vertex_set.reserve(graph_.size());
  for (const auto &[id, _] : graph_) {
    vertex_set.push_back(id);
  }
  return vertex_set;
}

std::vector<std::pair<std::size_t, std::size_t>> Graph::EdgeSet() const {
  std::vector<std::pair<std::size_t, std::size_t>> edge_set{};
  for (const auto &[src, adjacencies] : graph_) {
    for (const auto &adjacent : adjacencies) {
      edge_set.emplace_back(src, adjacent);
    }
  }
  return edge_set;
}

const std::unordered_set<std::size_t> &Graph::AdjacenciesFor(const std::size_t id) const {
  NOISEPAGE_ASSERT(graph_.find(id) != graph_.cend(), "Request for adjacencies of non-existent vertex");
  return graph_.at(id);
}

bool Graph::operator==(const Graph &rhs) const { return graph::Isomorphic(*this, rhs); }

bool Graph::operator!=(const Graph &rhs) const { return !graph::Isomorphic(*this, rhs); }

Graph Graph::FromEdgeSet(const std::vector<std::pair<std::size_t, std::size_t>> &edge_set) {
  Graph g{};
  for (const auto &[src, dst] : edge_set) {
    g.AddEdge(src, dst);
  }
  return g;
}

}  // namespace noisepage::common
