// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <random>
#include <utility>
#include <vector>
#include <mutex>

#ifdef VELOX_ENABLE_QPL
#include "qpl/qpl.h"

// namespace facebook {
// namespace velox {
// namespace qpl {  

namespace facebook::velox::dwio::common {

/// QplJobHWPool is resource pool to provide the job objects, which is
/// used for storing context information during.
/// Memory for QPL job will be allocated when the QPLJobHWPool instance is created
///
//  QPL job can offload RLE-decoding/Filter/(De)compression works to hardware accelerator.
class QplJobHWPool {
 public:
  static QplJobHWPool& GetInstance();

  /// Acquire QPL job
  ///
  /// @param job_id QPL job id, used when release QPL job
  /// \return Pointer to the QPL job. If acquire job failed, return nullptr.
  qpl_job* AcquireJob(uint32_t& job_id);

  /// \brief Release QPL job by the job_id.
  void ReleaseJob(uint32_t job_id);

  /// \brief Return if the QPL job is allocated sucessfully.
  const bool& job_ready() { return iaa_job_ready; }

  bool job_status(uint32_t job_id) {
    return job_ptr_locks[job_id];
  }

  qpl_job* GetJobById(uint32_t job_id) {
    return hw_job_ptr_pool[job_id];
  }

 private:
  QplJobHWPool();
  ~QplJobHWPool();
  bool tryLockJob(uint32_t index);
  // void unLockJob(uint32_t index);
  bool AllocateQPLJob();

  /// Max jobs in QPL_JOB_POOL
  static constexpr auto MAX_JOB_NUMBER = 256;
  /// Entire buffer for storing all job objects
  static std::unique_ptr<uint8_t[]> hw_jobs_buffer;
  /// Job pool for storing all job object pointers
  static std::array<qpl_job*, MAX_JOB_NUMBER> hw_job_ptr_pool;
  /// Locks for accessing each job object pointers
  static std::vector<bool> job_ptr_locks;
  static constexpr qpl_path_t qpl_path = qpl_path_hardware;
  // static std::array<uint32_t, MAX_JOB_NUMBER> job_ptr_locks;
  static bool iaa_job_ready;
  std::mt19937 random_engine;
  std::uniform_int_distribution<int> distribution;
  std::mutex job_lock;
};

}
// }  //  namespace qpl
// }  //  namespace velox
// }  //  namespace facebook
#endif
