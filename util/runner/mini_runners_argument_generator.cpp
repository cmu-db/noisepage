#include "runner/mini_runners_argument_generator.h"

#include <cmath>
#include <unordered_set>

#include "self_driving/modeling/operating_unit_defs.h"

namespace noisepage::runner {

void MiniRunnersArgumentGenerator::GenArithArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                     const MiniRunnersDataConfig &config) {
  auto operators = {selfdriving::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
                    selfdriving::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY,
                    selfdriving::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE,
                    selfdriving::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_PLUS_OR_MINUS,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_MULTIPLY,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_DIVIDE,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_COMPARE,
                    selfdriving::ExecutionOperatingUnitType::OP_VARCHAR_COMPARE};

  std::vector<size_t> counts;
  for (size_t i = 10000; i < 100000; i += 10000) counts.push_back(i);
  for (size_t i = 100000; i < 1000000; i += 100000) counts.push_back(i);

  for (auto op : operators) {
    for (auto count : counts) {
      b->push_back({static_cast<int64_t>(op), static_cast<int64_t>(count)});
    }
  }
}

void MiniRunnersArgumentGenerator::GenOutputArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                      const MiniRunnersDataConfig &config) {
  auto &num_cols = config.sweep_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        if (type == type::TypeId::INTEGER)
          b->push_back({col, 0, row});
        else if (type == type::TypeId::REAL)
          b->push_back({0, col, row});
      }
    }
  }

  // Generate special Output feature [1 0 0 1 1]
  b->push_back({0, 0, 1});
}

void MiniRunnersArgumentGenerator::GenScanArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                    const MiniRunnersDataConfig &config) {
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL, type::TypeId::VARCHAR};
  const std::vector<uint32_t> *num_cols;
  for (auto type : types) {
    if (type == type::TypeId::VARCHAR)
      num_cols = &config.sweep_varchar_col_nums_;
    else
      num_cols = &config.sweep_col_nums_;

    for (auto col : *num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (type == type::TypeId::INTEGER)
            b->push_back({col, 0, 15, 0, row, car, 0});
          else if (type == type::TypeId::REAL)
            b->push_back({0, col, 0, 15, row, car, 0});
          else if (type == type::TypeId::VARCHAR)
            b->push_back({0, col, 0, 5, row, car, 1});
          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->push_back({col, 0, 15, 0, row, row, 0});
        else if (type == type::TypeId::REAL)
          b->push_back({0, col, 0, 15, row, row, 0});
        else if (type == type::TypeId::VARCHAR)
          b->push_back({0, col, 0, 5, row, row, 1});
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenScanMixedArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                         const MiniRunnersDataConfig &config) {
  const auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  std::vector<std::vector<int64_t>> args;
  GenerateMixedArguments(&args, settings, config, row_nums, 0);
  GenerateMixedArguments(&args, settings, config, row_nums, 1);
  for (const auto &arg : args) {
    b->push_back(arg);
  }
}

void MiniRunnersArgumentGenerator::GenSortArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                    const MiniRunnersDataConfig &config) {
  auto is_topks = {0, 1};
  auto &num_cols = config.sweep_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto is_topk : is_topks) {
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          int64_t car = 1;
          while (car < row) {
            if (type == type::TypeId::INTEGER)
              b->push_back({col, 0, 15, 0, row, car, is_topk});
            else if (type == type::TypeId::REAL)
              b->push_back({0, col, 0, 15, row, car, is_topk});
            car *= 2;
          }

          if (type == type::TypeId::INTEGER)
            b->push_back({col, 0, 15, 0, row, row, is_topk});
          else if (type == type::TypeId::REAL)
            b->push_back({0, col, 0, 15, row, row, is_topk});
        }
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenAggregateArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                         const MiniRunnersDataConfig &config) {
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::VARCHAR};
  const std::vector<uint32_t> *num_cols;
  for (auto type : types) {
    if (type == type::TypeId::VARCHAR)
      num_cols = &config.sweep_varchar_col_nums_;
    else
      num_cols = &config.sweep_col_nums_;

    for (auto col : *num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (type != type::TypeId::VARCHAR)
            b->push_back({col, 0, 15, 0, row, car});
          else
            b->push_back({0, col, 0, 5, row, car});
          car *= 2;
        }

        if (type != type::TypeId::VARCHAR)
          b->push_back({col, 0, 15, 0, row, row});
        else
          b->push_back({0, col, 0, 5, row, row});
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenAggregateKeylessArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                                const MiniRunnersDataConfig &config) {
  auto &num_cols = config.sweep_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  for (auto col : num_cols) {
    for (auto row : row_nums) {
      int64_t car = 1;
      while (car < row) {
        b->push_back({col, 15, row, car});
        car *= 2;
      }

      b->push_back({col, 15, row, row});
    }
  }
}

void MiniRunnersArgumentGenerator::GenJoinSelfArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                        const MiniRunnersDataConfig &config) {
  auto &num_cols = config.sweep_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        std::vector<int64_t> cars;
        while (car < row) {
          if (row * row / car <= settings.data_rows_limit_) {
            if (type == type::TypeId::INTEGER)
              b->push_back({col, 0, 15, 0, row, car});
            else if (type == type::TypeId::BIGINT)
              b->push_back({0, col, 0, 15, row, car});
          }

          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->push_back({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::BIGINT)
          b->push_back({0, col, 0, 15, row, row});
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenJoinNonSelfArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                           const MiniRunnersDataConfig &config) {
  auto &num_cols = config.sweep_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (size_t i = 0; i < row_nums.size(); i++) {
        auto build_rows = row_nums[i];
        auto build_car = row_nums[i];
        for (size_t j = i + 1; j < row_nums.size(); j++) {
          auto probe_rows = row_nums[j];
          auto probe_car = row_nums[j];

          auto matched_car = row_nums[i];
          if (type == type::TypeId::INTEGER)
            b->push_back({col, 0, 15, 0, build_rows, build_car, probe_rows, probe_car, matched_car});
          else if (type == type::TypeId::BIGINT)
            b->push_back({0, col, 0, 15, build_rows, build_car, probe_rows, probe_car, matched_car});
        }
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenIdxScanArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                       const MiniRunnersDataConfig &config) {
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT, type::TypeId::VARCHAR};
  auto idx_sizes = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto &lookup_sizes = config.sweep_index_lookup_sizes_;
  const std::vector<uint32_t> *key_sizes;
  for (auto type : types) {
    int64_t tbl_cols = (type == type::TypeId::VARCHAR) ? 5 : 15;
    if (type == type::TypeId::VARCHAR)
      key_sizes = &config.sweep_varchar_index_col_nums_;
    else
      key_sizes = &config.sweep_index_col_nums_;

    for (auto key_size : *key_sizes) {
      for (auto idx_size : idx_sizes) {
        b->push_back({static_cast<int64_t>(type), tbl_cols, key_size, idx_size, 0, 1});
        for (auto lookup_size : lookup_sizes) {
          if (lookup_size <= idx_size) {
            b->push_back({static_cast<int64_t>(type), tbl_cols, key_size, idx_size, lookup_size, -1});
          }
        }

        b->push_back({static_cast<int64_t>(type), tbl_cols, key_size, idx_size, 0, 0});
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenIdxJoinArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                       const MiniRunnersDataConfig &config) {
  auto &key_sizes = config.sweep_col_nums_;
  auto idx_sizes = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  for (auto key_size : key_sizes) {
    for (size_t j = 0; j < idx_sizes.size(); j++) {
      // Build the inner index
      b->push_back({key_size, 0, idx_sizes[j], 1});

      for (size_t i = 0; i < idx_sizes.size(); i++) {
        b->push_back({key_size, idx_sizes[i], idx_sizes[j], -1});
      }

      // Drop the inner index
      b->push_back({key_size, 0, idx_sizes[j], 0});
    }
  }
}

void MiniRunnersArgumentGenerator::GenInsertArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                      const MiniRunnersDataConfig &config) {
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL};
  auto &num_rows = config.sweep_insert_row_nums_;
  auto &num_cols = config.sweep_col_nums_;
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : num_rows) {
        if (type == type::TypeId::INTEGER)
          b->push_back({col, 0, col, row});
        else if (type == type::TypeId::REAL)
          b->push_back({0, col, col, row});
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenInsertMixedArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                           const MiniRunnersDataConfig &config) {
  auto &mixed_dist = config.sweep_insert_mixed_dist_;
  auto &num_rows = config.sweep_insert_row_nums_;
  for (auto mixed : mixed_dist) {
    for (auto row : num_rows) {
      b->push_back({mixed.first, mixed.second, mixed.first + mixed.second, row});
    }
  }
}

void MiniRunnersArgumentGenerator::GenUpdateIndexArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                           const MiniRunnersDataConfig &config) {
  auto &idx_key = config.sweep_update_index_col_nums_;
  auto &update_keys = config.sweep_update_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  for (auto type : types) {
    for (auto idx_key_size : idx_key) {
      for (auto update_key : update_keys) {
        if (idx_key_size + update_key >= 15) continue;

        for (auto row_num : row_nums) {
          if (row_num > settings.updel_limit_) continue;

          // Special argument used to indicate a build index
          // We need to do this to prevent update/delete from unintentionally
          // updating multiple indexes. This way, there will only be 1 index
          // on the table at a given time.
          if (type == type::TypeId::INTEGER)
            b->push_back({idx_key_size, 0, update_key, 15, 0, row_num, 0, 1});
          else if (type == type::TypeId::BIGINT)
            b->push_back({0, idx_key_size, update_key, 0, 15, row_num, 0, 1});

          int64_t lookup_size = 1;
          std::vector<int64_t> lookups;
          while (lookup_size <= row_num) {
            lookups.push_back(lookup_size);
            lookup_size *= 2;
          }

          for (auto lookup : lookups) {
            if (type == type::TypeId::INTEGER)
              b->push_back({idx_key_size, 0, update_key, 15, 0, row_num, lookup, -1});
            else if (type == type::TypeId::BIGINT)
              b->push_back({0, idx_key_size, update_key, 0, 15, row_num, lookup, -1});
          }

          // Special argument used to indicate a drop index
          if (type == type::TypeId::INTEGER)
            b->push_back({idx_key_size, 0, update_key, 15, 0, row_num, 0, 0});
          else if (type == type::TypeId::BIGINT)
            b->push_back({0, idx_key_size, update_key, 0, 15, row_num, 0, 0});
        }
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenDeleteIndexArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                           const MiniRunnersDataConfig &config) {
  auto &idx_key = config.sweep_index_col_nums_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  for (auto type : types) {
    for (auto idx_key_size : idx_key) {
      for (auto row_num : row_nums) {
        if (row_num > settings.updel_limit_) continue;

        // Special argument used to indicate a build index
        // We need to do this to prevent update/delete from unintentionally
        // updating multiple indexes. This way, there will only be 1 index
        // on the table at a given time.
        if (type == type::TypeId::INTEGER)
          b->push_back({idx_key_size, 0, 15, 0, row_num, 0, 1});
        else if (type == type::TypeId::BIGINT)
          b->push_back({0, idx_key_size, 0, 15, row_num, 0, 1});

        int64_t lookup_size = 1;
        std::vector<int64_t> lookups;
        while (lookup_size <= row_num) {
          lookups.push_back(lookup_size);
          lookup_size *= 2;
        }

        for (auto lookup : lookups) {
          if (type == type::TypeId::INTEGER)
            b->push_back({idx_key_size, 0, 15, 0, row_num, lookup, -1});
          else if (type == type::TypeId::BIGINT)
            b->push_back({0, idx_key_size, 0, 15, row_num, lookup, -1});
        }

        // Special argument used to indicate a drop index
        if (type == type::TypeId::INTEGER)
          b->push_back({idx_key_size, 0, 15, 0, row_num, 0, 0});
        else if (type == type::TypeId::BIGINT)
          b->push_back({0, idx_key_size, 0, 15, row_num, 0, 0});
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenCreateIndexArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                           const MiniRunnersDataConfig &config) {
  auto &num_threads = config.sweep_index_create_threads_;
  auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  std::vector<uint32_t> num_cols;
  {
    std::unordered_set<uint32_t> col_set;
    col_set.insert(config.sweep_col_nums_.begin(), config.sweep_col_nums_.end());
    col_set.insert(config.sweep_index_col_nums_.begin(), config.sweep_index_col_nums_.end());

    num_cols.reserve(col_set.size());
    for (auto &col : col_set) {
      num_cols.push_back(col);
    }
  }
  std::sort(num_cols.begin(), num_cols.end(), std::less<>());

  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  for (auto thread : num_threads) {
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          int64_t car = 1;
          // Only use one cardinality for now since we can't track any cardinality anyway
          // TODO(lin): Augment the create index runner with different cardinalities when we're able to
          //  estimate/track the cardinality.
          if (row > settings.create_index_small_limit_) {
            // For these, we get a memory explosion if the cardinality is too low.
            while (car < row) {
              car *= 2;
            }
            car = car / (pow(2, settings.create_index_large_cardinality_num_));
          }

          if (type == type::TypeId::INTEGER)
            b->push_back({col, 0, 15, 0, row, car, 0, thread});
          else if (type == type::TypeId::BIGINT)
            b->push_back({0, col, 0, 15, row, car, 0, thread});
        }
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenCreateIndexMixedArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                                const MiniRunnersDataConfig &config) {
  auto &num_threads = config.sweep_index_create_threads_;
  const auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);

  // Generates INTEGER + VARCHAR
  std::vector<std::vector<int64_t>> args;
  GenerateMixedArguments(&args, settings, config, row_nums, 1);

  for (auto thread : num_threads) {
    for (auto arg : args) {
      auto row = arg[4];
      auto arg_car = arg[5];
      if (row > settings.create_index_small_limit_) {
        // For these, we get a memory explosion if the cardinality is too low.
        int64_t car = 1;
        while (car < row) {
          car *= 2;
        }

        // This car is the ceiling
        car = car / (pow(2, settings.create_index_large_cardinality_num_));
        // Only use one cardinality for now since we can't track any cardinality anyway
        if (arg_car != car) {
          continue;
        }
      }

      arg.push_back(thread);
      b->push_back(arg);
    }
  }
}

void MiniRunnersArgumentGenerator::GenIndexInsertDeleteArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                                                 const MiniRunnersDataConfig &config) {
  auto num_indexes = {settings.index_model_batch_size_};
  const auto row_nums = config.GetRowNumbersWithLimit(settings.data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  auto num_cols = config.sweep_index_col_nums_;

  for (auto num_index : num_indexes) {
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          b->push_back({col, 15, row, static_cast<int64_t>(type), num_index});
        }
      }
    }
  }
}

void MiniRunnersArgumentGenerator::GenerateMixedArguments(std::vector<std::vector<int64_t>> *args,
                                                          const MiniRunnersSettings &settings,
                                                          const MiniRunnersDataConfig &config,
                                                          const std::vector<uint32_t> &row_nums, uint32_t varchar_mix) {
  std::vector<std::pair<uint32_t, uint32_t>> mixed_dist;
  uint32_t step_size;
  if (varchar_mix == 0) {
    /* Vector of table distributions <INTEGER, DECIMALS> */
    mixed_dist = config.sweep_scan_mixed_dist_;
    step_size = 2;
  } else {
    /* Vector of table distributions <INTEGER, VARCHAR> */
    mixed_dist = config.sweep_scan_mixed_varchar_dist_;
    step_size = 1;
  } /* Always generate full table scans for all row_num and cardinalities. */
  for (auto col_dist : mixed_dist) {
    std::pair<uint32_t, uint32_t> start = {col_dist.first - step_size, step_size};
    while (true) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          args->push_back({start.first, start.second, col_dist.first, col_dist.second, row, car, varchar_mix});
          car *= 2;
        }
        args->push_back({start.first, start.second, col_dist.first, col_dist.second, row, row, varchar_mix});
      }
      if (start.second < col_dist.second) {
        start.second += step_size;
      } else if (start.first < col_dist.first) {
        start.first += step_size;
        start.second = step_size;
      } else {
        break;
      }
    }
  }
}

};  // namespace noisepage::runner
