#include "brain/pilot/pilot.h"

#include <unordered_map>
#include <map>
#include <vector>
#include <string>
#include <utility>
#include <iostream>
#include <fstream>

#include "common/macros.h"
#include "parser/expression/constant_value_expression.h"
#include "brain/forecast/workload_forecast.h"
#include "execution/exec_defs.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::brain {

Pilot::Pilot(uint64_t forecast_interval) : forecast_interval_(forecast_interval) {
  LoadQueryTrace();
  LoadQueryText();
  forecastor_ = std::make_unique<WorkloadForecast>(query_timestamp_to_id_, num_executions_, query_id_to_text_, 
                                                   query_text_to_id_, query_id_to_params_, forecast_interval_);
}

void Pilot::LoadQueryTrace() {
  uint8_t NUM_COLS = 4;
  std::ifstream myFile("./query_trace.csv");
  // Make sure the file is open
  if(!myFile.is_open()) throw std::runtime_error("Could not open file");
  
  // Helper vars
  std::string line, val_string, type_string;
  if(!myFile.good()) throw std::runtime_error("File stream is not good");

  // ignore header
  std::getline(myFile, line);

  // Read data, line by line
  execution::query_id_t query_id;
  size_t pos, pos2, colnum, curr_size;
  bool null_detected;
  std::vector<std::string> val_vec (NUM_COLS, "");
  

  while(std::getline(myFile, line))
  {
    colnum = 0;
    val_vec.assign(NUM_COLS, "");
    // std::cout << line << "\n" << std::flush;
    while ((pos = line.find(",")) != std::string::npos && colnum < NUM_COLS) {
      if (pos > 0) {
        val_vec[colnum] = line.substr(0, pos);
      }
      line.erase(0, pos + 2);
      colnum ++;
    }

    if (val_vec[0] == "") {
      // query_id not found
      continue;
    }

    query_id = static_cast<execution::query_id_t>(std::stoi(val_vec[0]));
    val_string = val_vec[2];
    type_string = val_vec[3];

    std::vector<parser::ConstantValueExpression> param_vec;
    while ((pos = val_string.find(";")) != std::string::npos 
           && (pos2 = type_string.find(";")) != std::string::npos) {
      if (pos > 0) {
        auto cve = parser::ConstantValueExpression::FromString(val_string.substr(0, pos), 
                                                               std::stoi(type_string.substr(0, pos2)));
          
        // if (cve.ToString() != val_string.substr(0, pos)) {
        //   std::cout << cve.ToString() << " " << val_string.substr(0, pos) << std::endl;
        // }
        param_vec.push_back(cve);
        // std::cout << cve.ToString() << "," << std::flush;
        // std::cout << val_vec.at(counter) << " " << val_string << ",\n" << std::flush;
      } else {
        std::cout << "null value detected in query params recorded\n" << std::flush;
        null_detected = true;
        break;
      }
      val_string.erase(0, pos + 1);
      type_string.erase(0, pos2 + 1);
    }

    if (!null_detected) {
      if ((curr_size = query_id_to_params_[query_id].size()) < num_sample_) {
        query_id_to_params_[query_id].push_back(param_vec);
        query_timestamp_to_id_[std::stoull(val_vec[1])] = std::make_pair(query_id, curr_size);
        num_executions_[query_id].push_back(1);
      } else {
        num_executions_[query_id][rand() % num_sample_] ++;
      }
    }
    // std::cout << "\n" << std::flush;
    // std::cout << "\nnewline\n" << std::endl;
  }
  // Close file
  myFile.close();
}

void Pilot::LoadQueryText() {
  uint8_t NUM_COLS = 3;
  // Create an input filestream
  std::ifstream myFile("./query_text.csv");
  // Make sure the file is open
  if(!myFile.is_open()) throw std::runtime_error("Could not open file");
  
  // Helper vars
  std::string line;
  if(!myFile.good()) throw std::runtime_error("File stream is not good");

  // ignore header
  std::getline(myFile, line);

  // Read data, line by line
  execution::query_id_t query_id;
  size_t pos, colnum;
  std::vector<std::string> val_vec (NUM_COLS, "");

  while(std::getline(myFile, line))
  {
    colnum = 0;
    val_vec.assign(NUM_COLS, "");
    // std::cout << line << "\n" << std::flush;
    while ((pos = line.find(",")) != std::string::npos && colnum < NUM_COLS) {
      if (pos > 0) {
        val_vec[colnum] = line.substr(0, pos);
      }
      line.erase(0, pos + 2);
      colnum ++;
    }

    if (val_vec[0] == "") {
      // query_id not found
      continue;
    }
    query_id = static_cast<execution::query_id_t>(std::stoi(val_vec[0]));
    query_id_to_text_[query_id] = val_vec[1];
    query_text_to_id_[val_vec[1]] = query_id;
    // std::cout << "\n" << std::flush;
    // std::cout << "\nnewline\n" << std::endl;
  }
  // Close file
  myFile.close();

  // Initialize queries_, query_params_ here, by loading from CSV files
}

}  // namespace terrier::brain
