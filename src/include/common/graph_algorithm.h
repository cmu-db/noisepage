#pragma once

#include <vector>

namespace noisepage::common {
class Graph;
}  // namespace noisepage::common

namespace noisepage::common::graph {

/**
 * Determine if graph `lhs` is isomorphic to graph `rhs`.
 * @param lhs Input graph
 * @param rhs Input graph
 * @return `true` if the graphs are isomorphic, `false` otherwise
 */
bool Isomorphic(const Graph &lhs, const Graph &rhs);

/**
 * Determine if graph `graph` contains a cycle.
 * @param graph The graph of interest
 */
bool HasCycle(const Graph &graph);

/**
 * Compute a topological sort of graph `graph`.
 * @pre The graph must be acyclic
 * @param graph The input graph
 * @return A topological sort of `graph`
 */
std::vector<std::size_t> TopologicalSort(const Graph &graph);

}  // namespace noisepage::common::graph
