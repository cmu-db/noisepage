#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"

// From test
#include "execution/vm/module_compiler.h"

#include "execution/bandit/agent.h"
#include "execution/bandit/environment.h"
#include "execution/bandit/multi_armed_bandit.h"
#include "execution/bandit/policy.h"

#define NUM_EXPERIMENTS 10

namespace terrier::execution::vm::test {

struct TestConf {
  std::string pred1;
  std::string pred2;
  std::string out_file;
};

class BanditTest : public TplTest, public ::testing::WithParamInterface<TestConf> {
 public:
  BanditTest() : region_("test") {}

  util::Region *region() { return &region_; }

  auto CreateSampleTPLFunction(int index, std::vector<int> permuataion) {
    // Assumes that size of permuataion is 5.

    std::vector<std::string> predicates = {pred1_, pred2_, "row.colB >= 2", "row.colB < 5", "row.colC <= 6048"};

    auto predicate = predicates[permuataion[0]] + " and " + predicates[permuataion[1]] + " and " +
                     predicates[permuataion[2]] + " and " + predicates[permuataion[3]] + " and " +
                     predicates[permuataion[4]];

    auto function_name = "f" + std::to_string(index);

    auto src = "fun " + function_name +
               "() -> int32 {"
               "  var count : int32 = 0"
               "  for (row in test_1) {"
               "    if (" +
               predicate +
               ") {"
               "      count = count + 1"
               "    }"
               "  }"
               "  return count"
               "}\n";

    return std::tuple(src, function_name);
  }

  auto CreateSampleTPLCode() {
    std::vector<std::vector<int>> permuataions = {{0, 1, 2, 3, 4}, {2, 1, 4, 3, 0}, {2, 4, 3, 1, 0}, {3, 2, 1, 0, 4},
                                                  {3, 4, 0, 1, 2}, {4, 0, 1, 3, 2}, {4, 1, 0, 3, 2}, {4, 3, 1, 2, 0},
                                                  {4, 3, 2, 0, 1}, {4, 3, 2, 1, 0}};

    std::string tpl_code;
    std::vector<std::string> function_names;

    for (u32 i = 0; i < permuataions.size(); ++i) {
      // NOLINTNEXTLINE
      auto [src, function_name] = CreateSampleTPLFunction(i, permuataions[i]);
      tpl_code += src;
      function_names.push_back(function_name);
    }

    return std::tuple(tpl_code, function_names);
  }

  void SetPred1(std::string pred) { pred1_ = std::move(pred); }
  void SetPred2(std::string pred) { pred2_ = std::move(pred); }

 private:
  std::string pred1_;
  std::string pred2_;
  util::Region region_;
};

void RunExperiment(bandit::MultiArmedBandit *bandit, bandit::Agent *agent, bool shuffle, int num_trials,
                   u32 optimal_action, std::vector<double> *avg_rewards, std::vector<double> *optimal,
                   double *avg_exec_time, double *avg_total_time) {
  auto environment = bandit::Environment(bandit, agent);

  std::vector<double> rewards;
  std::vector<u32> actions;

  double total_time = 0;
  for (u32 exp = 0; exp < NUM_EXPERIMENTS; exp++) {
    environment.Reset();
    {
      util::ScopedTimer<std::milli> timer(&total_time);
      environment.Run(num_trials, &rewards, &actions, shuffle);
    }

    for (int i = 0; i < num_trials; ++i) {
      auto exec_time_i = bandit::MultiArmedBandit::RewardToExecutionTime(rewards[i]);
      (*avg_rewards)[i] += exec_time_i;
      (*optimal)[i] += (actions[i] == optimal_action) ? 1 : 0;
      *avg_exec_time += exec_time_i;
    }
    *avg_total_time += total_time;
  }

  for (int i = 0; i < num_trials; ++i) {
    (*avg_rewards)[i] /= NUM_EXPERIMENTS;
    (*optimal)[i] /= NUM_EXPERIMENTS;
    (*optimal)[i] *= 100;
  }

  *avg_exec_time /= NUM_EXPERIMENTS;
  *avg_total_time /= NUM_EXPERIMENTS;
}

// NOLINTNEXTLINE
TEST_P(BanditTest, DISABLED_SimpleTest) {
  auto conf = GetParam();

  EXECUTION_LOG_INFO("Configuration {}", conf.out_file);

  this->SetPred1(conf.pred1);
  this->SetPred2(conf.pred2);

  // NOLINTNEXTLINE
  auto [src, action_names] = CreateSampleTPLCode();

  ModuleCompiler compiler;
  auto *ast = compiler.CompileToAst(src);

  // Try generating bytecode for this declaration
  auto module = std::make_unique<Module>(BytecodeGenerator::Compile(ast, nullptr, "bandit"));

  auto bandit = bandit::MultiArmedBandit(module.get(), action_names);

  auto num_actions = static_cast<u32>(action_names.size());
  u32 num_trials = 200;

  std::vector<double> exec_time_individual(num_actions, 0.0);
  std::vector<double> total_time_individual(num_actions, 0.0);
  std::vector<std::vector<double>> rewards_individual(num_actions, std::vector<double>(num_trials, 0.0));
  std::vector<std::vector<double>> optimal_individual(num_actions, std::vector<double>(num_trials, 0.0));

  double min_time_so_far = DBL_MAX;
  u32 optimal_action = 0;

  // Run using FixedAction policy for each action
  for (u32 action = 0; action < num_actions; action++) {
    auto policy = bandit::FixedActionPolicy(action);
    auto agent = bandit::Agent(&policy, 10);

    RunExperiment(&bandit, &agent, /*shuffle=*/false, num_trials, optimal_action, &rewards_individual[action],
                  &optimal_individual[action], &exec_time_individual[action], &total_time_individual[action]);

    if (exec_time_individual[action] < min_time_so_far) {
      min_time_so_far = exec_time_individual[action];
      optimal_action = action;
    }

    EXECUTION_LOG_INFO("Completed FixedActionPolicy for action: {}", action);
  }

  EXECUTION_LOG_INFO("Best Action is action: {}", optimal_action);

  double exec_time_ucb = 0.0;
  double total_time_ucb = 0.0;
  std::vector<double> rewards_ucb(num_trials, 0.0);
  std::vector<double> optimal_ucb(num_trials, 0.0);

  // Run using UCB policy
  {
    auto policy = bandit::UCBPolicy(1);
    auto agent = bandit::Agent(&policy, num_actions);

    RunExperiment(&bandit, &agent, /*shuffle=*/true, num_trials, optimal_action, &rewards_ucb, &optimal_ucb,
                  &exec_time_ucb, &total_time_ucb);

    EXECUTION_LOG_INFO("Completed UCBPolicy");
  }

  double exec_time_epsgreedy = 0.0;
  double total_time_epsgreedy = 0.0;
  std::vector<double> rewards_epsgreedy(num_trials, 0.0);
  std::vector<double> optimal_epsgreedy(num_trials, 0.0);

  // Run using EpsilonGreedy policy
  {
    auto policy = bandit::EpsilonGreedyPolicy(0.1);
    auto agent = bandit::Agent(&policy, num_actions);

    RunExperiment(&bandit, &agent, /*shuffle=*/true, num_trials, optimal_action, &rewards_epsgreedy, &optimal_epsgreedy,
                  &exec_time_epsgreedy, &total_time_epsgreedy);

    EXECUTION_LOG_INFO("Completed EpsilonGreedyPolicy");
  }

  EXECUTION_LOG_INFO("Writing to out_file");

  std::ofstream out_file;
  out_file.open(conf.out_file.c_str());

  out_file << "Timestep/Partition, ";

  for (u32 action = 0; action < num_actions; action++) {
    out_file << "Action" << action << ", ";
  }

  out_file << "UCB: exec_time, UCB: optimal, ";
  out_file << "EpsGreedy: exec_time, EpsGreedy: optimal, " << std::endl;

  // print execution time on each timestep.
  for (u32 i = 0; i < num_trials; ++i) {
    out_file << i << ", ";

    for (u32 action = 0; action < num_actions; action++) {
      out_file << rewards_individual[action][i] << ", ";
    }

    out_file << rewards_ucb[i] << ", " << optimal_ucb[i] << ", " << rewards_epsgreedy[i] << ", " << optimal_epsgreedy[i]
             << std::endl;
  }

  // print overall execution time.=
  out_file << "exec_time, ";

  for (u32 action = 0; action < num_actions; action++) {
    out_file << exec_time_individual[action] << ", ";
  }

  out_file << exec_time_ucb << ", , " << exec_time_epsgreedy << ", , " << std::endl;

  // print overall execution time with overhead.
  out_file << "total_time, ";

  for (u32 action = 0; action < num_actions; action++) {
    out_file << total_time_individual[action] << ", ";
  }

  out_file << total_time_ucb << ", , " << total_time_epsgreedy << ", , " << std::endl;

  out_file.close();
}

std::vector<TestConf> confs = {{"row.colA >= 10000000", "row.colA < 10000000", "output_0.csv"},
                               {"row.colA >= 9000000", "row.colA < 11000000", "output_10.csv"},
                               {"row.colA >= 8000000", "row.colA < 12000000", "output_20.csv"},
                               {"row.colA >= 7000000", "row.colA < 13000000", "output_30.csv"},
                               {"row.colA >= 6000000", "row.colA < 14000000", "output_40.csv"},
                               {"row.colA >= 5000000", "row.colA < 15000000", "output_50.csv"},
                               {"row.colA >= 4000000", "row.colA < 16000000", "output_60.csv"},
                               {"row.colA >= 3000000", "row.colA < 17000000", "output_70.csv"},
                               {"row.colA >= 2000000", "row.colA < 18000000", "output_80.csv"},
                               {"row.colA >= 1000000", "row.colA < 19000000", "output_90.csv"},
                               {"row.colA >= 0", "row.colA < 20000000", "output_100.csv"}};

INSTANTIATE_TEST_CASE_P(SimpleTestInstance, BanditTest, ::testing::ValuesIn(confs));

}  // namespace terrier::execution::vm::test
