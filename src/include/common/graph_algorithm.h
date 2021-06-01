#pragma once

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

}  // namespace noisepage::common::graph
