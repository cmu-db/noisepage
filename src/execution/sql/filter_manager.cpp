#include "execution/sql/filter_manager.h"

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "execution/bandit/agent.h"
#include "execution/bandit/multi_armed_bandit.h"
#include "execution/bandit/policy.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/timer.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::sql {

namespace {

// TODO(pmenon): Move to some Policy Factory
std::unique_ptr<bandit::Policy> CreatePolicy(bandit::Policy::Kind policy_kind) {
  switch (policy_kind) {
    case bandit::Policy::Kind::EpsilonGreedy: {
      return std::make_unique<bandit::EpsilonGreedyPolicy>(bandit::EpsilonGreedyPolicy::K_DEFAULT_EPSILON);
    }
    case bandit::Policy::Kind::Greedy: {
      return std::make_unique<bandit::GreedyPolicy>();
    }
    case bandit::Policy::Kind::Random: {
      return std::make_unique<bandit::RandomPolicy>();
    }
    case bandit::Policy::Kind::UCB: {
      return std::make_unique<bandit::UCBPolicy>(bandit::UCBPolicy::K_DEFAULT_UCB_HYPER_PARAM);
    }
    case bandit::Policy::Kind::FixedAction: {
      return std::make_unique<bandit::FixedActionPolicy>(0);
    }
    default: {
      UNREACHABLE("Impossible bandit policy kind");
    }
  }
}

}  // namespace

FilterManager::FilterManager(const bandit::Policy::Kind policy_kind) : policy_(CreatePolicy(policy_kind)) {}

FilterManager::~FilterManager() = default;

void FilterManager::StartNewClause() {
  TERRIER_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  clauses_.emplace_back();
}

void FilterManager::InsertClauseFlavor(const FilterManager::MatchFn flavor) {
  TERRIER_ASSERT(!finalized_, "Cannot modify filter manager after finalization");
  TERRIER_ASSERT(!clauses_.empty(), "Inserting flavor without clause");
  clauses_.back().flavors_.push_back(flavor);
}

void FilterManager::Finalize() {
  if (finalized_) {
    return;
  }

  optimal_clause_order_.resize(clauses_.size());

  // Initialize optimal orderings, initially in the order they appear
  std::iota(optimal_clause_order_.begin(), optimal_clause_order_.end(), 0);

  // Setup the agents, once per clause
  for (uint32_t idx = 0; idx < clauses_.size(); idx++) {
    agents_.emplace_back(policy_.get(), ClauseAt(idx)->NumFlavors());
  }

  finalized_ = true;
}

void FilterManager::RunFilters(ProjectedColumnsIterator *const pci) {
  TERRIER_ASSERT(finalized_, "Must finalize the filter before it can be used");

  // Execute the clauses in what we currently believe to be the optimal order
  for (const uint32_t opt_clause_idx : optimal_clause_order_) {
    RunFilterClause(pci, opt_clause_idx);
  }
}

void FilterManager::RunFilterClause(ProjectedColumnsIterator *const pci, const uint32_t clause_index) {
  //
  // This function will execute the clause at the given clause index. But, we'll
  // be smart about it. We'll use our multi-armed bandit agent to predict the
  // implementation flavor to execute so as to optimize the reward: the smallest
  // execution time.
  //
  // Each clause has an agent tracking the execution history of the flavors.
  // In this round, select one using the agent's configured policy, execute it,
  // convert it to a reward and update the agent's state.
  //

  // Select the apparent optimal flavor of the clause to execute
  bandit::Agent *agent = GetAgentFor(clause_index);
  const uint32_t opt_flavor_idx = agent->NextAction();
  const auto opt_match_func = ClauseAt(clause_index)->flavors_[opt_flavor_idx];

  // Run the filter
  // NOLINTNEXTLINE
  auto [_, exec_ms] = RunFilterClauseImpl(pci, opt_match_func);
  (void)_;

  // Update the agent's state
  double reward = bandit::MultiArmedBandit::ExecutionTimeToReward(exec_ms);
  agent->Observe(reward);
  EXECUTION_LOG_DEBUG("Clause {} observed reward {}", clause_index, reward);
}

std::pair<uint32_t, double> FilterManager::RunFilterClauseImpl(ProjectedColumnsIterator *const pci,
                                                               const FilterManager::MatchFn func) {
  // Time and execute the match function, returning the number of selected
  // tuples and the execution time in milliseconds
  util::Timer<> timer;
  timer.Start();
  const uint32_t num_selected = func(pci);
  timer.Stop();
  return std::make_pair(num_selected, timer.Elapsed());
}

uint32_t FilterManager::GetOptimalFlavorForClause(const uint32_t clause_index) const {
  const bandit::Agent *agent = GetAgentFor(clause_index);
  return agent->GetCurrentOptimalAction();
}

bandit::Agent *FilterManager::GetAgentFor(const uint32_t clause_index) { return &agents_[clause_index]; }

const bandit::Agent *FilterManager::GetAgentFor(const uint32_t clause_index) const { return &agents_[clause_index]; }

}  // namespace terrier::execution::sql
