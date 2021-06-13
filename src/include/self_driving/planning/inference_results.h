#pragma once

#include <map>
#include <utility>
#include <vector>

namespace noisepage::selfdriving::pilot {

/** Bundles the inference results to compute cost during the Pilot's planning */
struct InferenceResults {
  // query id, <num_param of this query executed, total number of collected ous for this query>
  std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> query_info_;

  // This is to record the start index of ou records belonging to a segment in input to the interference model
  std::map<uint32_t, uint64_t> segment_to_offset_;

  // pipeline_to_prediction maps each pipeline to a vector of ou inference results for all ous of this pipeline
  // (where each entry corresponds to a different query param)
  // Each element of the outermost vector is a vector of ou prediction (each being a double vector) for one set of
  // parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction_;

  // OU Inference results for queries across each segments (one-to-one mapping to query_interference_results_)
  // In theory this can be index references to pipeline_to_prediction_ to save space. But it would be more complex
  std::vector<std::vector<double>> query_ou_inference_results_;

  // Inference results for queries after (interference model applied). Ordered by the OUs across each forecast segment
  std::vector<std::vector<double>> query_inference_results_;

  // Inference result for an optional action (interference model applied)
  std::vector<double> action_inference_result_;
};

}  // namespace noisepage::selfdriving::pilot
