#pragma once

#include <random>
#include <vector>

#include "execution/util/common.h"

namespace tpl::bandit {

class Agent;

/**
 * An enumeration capturing different policies for choosing actions.
 */
enum class Kind : u8 { EpsilonGreedy, Greedy, Random, UCB, FixedAction };

/**
 * A policy prescribes an action to be taken based on the memory of an agent.
 */
class Policy {
 public:
  /**
   * Cosntructor
   * @param kind the kind of policy
   */
  explicit Policy(Kind kind) : kind_(kind), generator_(time(nullptr)) {}

  /**
   * Returns the next action according to the policy
   */
  virtual u32 NextAction(Agent *agent) = 0;

  /**
   * @return the kind of policy
   */
  auto kind() { return kind_; }

 protected:
  /// The kind of policy
  Kind kind_;
  /// The random number generator
  std::mt19937 generator_;
};

/**
 * The Epsilon-Greedy policy will choose a random action with probability
 * epsilon and take the best apparent approach with probability 1-epsilon. If
 * multiple actions are tied for best choice, then a random action from that
 * subset is selected.
 */
class EpsilonGreedyPolicy : public Policy {
 public:
  /**
   * Construct
   * @param epsilon epsilon value
   * @param kind kind of policy
   */
  explicit EpsilonGreedyPolicy(double epsilon, Kind kind = Kind::EpsilonGreedy)
      : Policy(kind), epsilon_(epsilon), real_(std::uniform_real_distribution<double>(0, 1)) {}

  u32 NextAction(Agent *agent) override;

 private:
  double epsilon_;
  std::uniform_real_distribution<double> real_;
};

/**
 * The Greedy policy only takes the best apparent action, with ties broken by
 * random selection. This can be seen as a special case of EpsilonGreedy where
 * epsilon = 0 i.e. always exploit.
 */
class GreedyPolicy : public EpsilonGreedyPolicy {
 public:
  /// Constructor
  GreedyPolicy() : EpsilonGreedyPolicy(0, Kind::Greedy) {}
};

/**
 * The Random policy randomly selects from all available actions with no
 * consideration to which is apparently best. This can be seen as a special
 * case of EpsilonGreedy where epsilon = 1 i.e. always explore.
 */
class RandomPolicy : public EpsilonGreedyPolicy {
 public:
  /// Constructor
  RandomPolicy() : EpsilonGreedyPolicy(1, Kind::Random) {}
};

/**
 * The Upper Confidence Bound algorithm (UCB). It applies an exploration
 * factor to the expected value of each arm which can influence a greedy
 * selection strategy to more intelligently explore less confident options.
 */
class UCBPolicy : public Policy {
 public:
  /**
   * Constructor
   * @param c hyperparameter value
   */
  explicit UCBPolicy(double c) : Policy(Kind::UCB), c_(c) {}

  u32 NextAction(Agent *agent) override;

 private:
  // Hyperparameter that decides the weight of the penalty term.
  double c_;
};

/**
 * Fixed Action Policy. It returns a fixed action irrespective of the rewards
 * obtained.
 */
class FixedActionPolicy : public Policy {
 public:
  /**
   * Constructor
   * @param action action to take
   */
  explicit FixedActionPolicy(u32 action) : Policy(Kind::FixedAction), action_(action) {}

  u32 NextAction(Agent *agent) override { return action_; }

 private:
  u32 action_;
};

}  // namespace tpl::bandit
