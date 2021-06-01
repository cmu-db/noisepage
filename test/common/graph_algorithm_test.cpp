#include "common/graph_algorithm.h"

#include "common/graph.h"
#include "test_util/test_harness.h"

namespace noisepage {

using common::Graph;
namespace graph = common::graph;

class GraphAlgorithmTest : public TerrierTest {};

// ----------------------------------------------------------------------------
// graph::Isomorphic
// ----------------------------------------------------------------------------

TEST_F(GraphAlgorithmTest, Isomorphic0) {
  Graph a{};
  Graph b{};
  EXPECT_TRUE(graph::Isomorphic(a, b));
}

TEST_F(GraphAlgorithmTest, Isomorphic1) {
  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddEdge(1, 2);

  EXPECT_TRUE(graph::Isomorphic(a, b));
}

TEST_F(GraphAlgorithmTest, Isomorphic2) {
  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddVertex(2);

  EXPECT_FALSE(graph::Isomorphic(a, b));
}

// ----------------------------------------------------------------------------
// graph::HasCycle
// ----------------------------------------------------------------------------

}  // namespace noisepage
