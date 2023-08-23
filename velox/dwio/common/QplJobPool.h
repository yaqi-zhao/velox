/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <mutex>
#include <random>
#include <vector>
#include "qpl/qpl.h"

namespace facebook::velox::dwio::common {

// QplJobHWPool is resource pool to provide the job objects, which is
// used for storing context information.
// Memory for QPL job will be allocated when the QPLJobHWPool instance is
// created
class QplJobHWPool {
 public:
  static QplJobHWPool& GetInstance();
  QplJobHWPool();
  ~QplJobHWPool();

  // Release QPL job by the job_id.
  void ReleaseJob(int job_id);

  // Return if the QPL job is allocated sucessfully.
  const bool& job_ready() {
    return iaa_job_ready;
  }

  qpl_job* AcquireDeflateJob(int& job_id);
  qpl_job* GetJobById(int job_id) {
    if (job_id >= MAX_JOB_NUMBER || job_id <= 0) {
      return nullptr;
    }
    return hw_job_ptr_pool[job_id];
  }

  // Max jobs in QPL_JOB_POOL
  static constexpr auto MAX_JOB_NUMBER = 1024;

 private:
  bool tryLockJob(uint32_t index);
  bool AllocateQPLJob();
  static constexpr qpl_path_t qpl_path = qpl_path_hardware;
  // Job pool for storing all job object pointers
  static std::array<qpl_job*, MAX_JOB_NUMBER> hw_job_ptr_pool;

  // Entire buffer for storing all job objects
  static std::unique_ptr<uint8_t[]> hw_jobs_buffer;

  // Locks for accessing each job object pointers
  static std::array<std::atomic<bool>, MAX_JOB_NUMBER> hw_job_ptr_locks;
  static bool iaa_job_ready;
};

} // namespace facebook::velox::dwio::common
