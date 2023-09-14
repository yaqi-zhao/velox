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

#include "velox/dwio/common/QplJobPool.h"
#include "velox/common/base/Exceptions.h"
#include <folly/Random.h>
#include <iostream>

#ifdef VELOX_ENABLE_QPL

namespace facebook::velox::dwio::common {

std::array<qpl_job*, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::hw_job_ptr_pool;
std::array<std::atomic<bool>, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::hw_job_ptr_locks;
bool QplJobHWPool::iaa_job_ready = false;
std::unique_ptr<uint8_t[]> QplJobHWPool::hw_jobs_buffer;
std::queue<uint32_t> QplJobHWPool::hw_job_ptr_queue;
std::array<uint32_t, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::hw_job_status_array; // 0 unsubmitted, 1 submitted
std::mutex QplJobHWPool::mtx;
std::mutex QplJobHWPool::job_queue_mtx;

QplJobHWPool& QplJobHWPool::GetInstance() {
  static QplJobHWPool pool;
  return pool;
}

QplJobHWPool::QplJobHWPool()
{
  (void)AllocateQPLJob();
}

QplJobHWPool::~QplJobHWPool() {
  for (uint32_t i = 0; i < MAX_JOB_NUMBER; ++i) {
    if (hw_job_ptr_pool[i]) {
      qpl_fini_job(hw_job_ptr_pool[i]);
      hw_job_ptr_pool[i] = nullptr;
    }
  }
  iaa_job_ready = false;
}

bool QplJobHWPool::AllocateQPLJob() {
  uint32_t job_size = 0;

  /// Get size required for saving a single qpl job object
  qpl_get_job_size(qpl_path, &job_size);
  /// Allocate entire buffer for storing all job objects
  hw_jobs_buffer = std::make_unique<uint8_t[]>(job_size * MAX_JOB_NUMBER);
  /// Initialize pool for storing all job object pointers
  /// Reallocate buffer by shifting address offset for each job object.
  for (uint32_t index = 0; index < MAX_JOB_NUMBER; ++index) {
    hw_job_status_array[index] = 0;
    qpl_job* qpl_job_ptr =
        reinterpret_cast<qpl_job*>(hw_jobs_buffer.get() + index * job_size);
    if (qpl_init_job(qpl_path, qpl_job_ptr) != QPL_STS_OK) {
      iaa_job_ready = false;
      VELOX_DCHECK_EQ(
      1,
      0,
      "Initialization of hardware IAA failed."
          " Please check if Intel In-Memory Analytics Accelerator (IAA) "
          "is properly set up!");
        return false;
    }
    this->hw_job_ptr_pool[index] = qpl_job_ptr;
    hw_job_ptr_locks[index].store(false);    
  }

  iaa_job_ready = true;
  return true;
}

qpl_job* QplJobHWPool::AcquireDeflateJob(uint32_t& job_id) {
  if (!job_ready()) {
    return nullptr;
  }
  uint32_t retry = 0;
  auto index = folly::Random::rand32(1, MAX_JOB_NUMBER - 1);
  while (!tryLockJob(index)) {
    index = folly::Random::rand32(1, MAX_JOB_NUMBER - 1);
    retry++;
    if (retry > MAX_JOB_NUMBER) {
      return nullptr;
    }
  }
  job_id = index;
  if (index >= MAX_JOB_NUMBER) {
    return nullptr;
  }

  return hw_job_ptr_pool[index];
}

void QplJobHWPool::ReleaseJob(uint32_t job_id) {
    if (job_id >= MAX_JOB_NUMBER) {
      return;
    }
    assert(job_id < MAX_JOB_NUMBER);
    hw_job_ptr_locks[job_id].store(false);  
    return;
}

bool QplJobHWPool::tryLockJob(uint32_t index) {
    bool expected = false;
    assert(index < MAX_JOB_NUMBER);
    return hw_job_ptr_locks[index].compare_exchange_strong(expected, true);
}

bool QplJobHWPool::push_unsubmitted_job(uint32_t job_id) {
  if (hw_job_ptr_queue.size() >= 256) {
    LOG(WARNING) << "hw_job_ptr_queue size: " << hw_job_ptr_queue.size();
    return false;
  }
  mtx.lock();
  hw_job_ptr_queue.push(job_id);
  hw_job_status_array[job_id] = 1;
  // LOG(WARNING) << "hw_job_ptr_queue push to queue current size: " << hw_job_ptr_queue.size();
  mtx.unlock();
  return true;
}

void QplJobHWPool::submit_job_queue() {
  if (hw_job_ptr_queue.empty()) {
    return;
  }
  if (!mtx.try_lock()) {
    return;
  }

  while (!hw_job_ptr_queue.empty()) {
    qpl_job* job =  hw_job_ptr_pool[hw_job_ptr_queue.front()];
    uint8_t* input = job->next_in_ptr;
    uint32_t avl_in = job->available_in;
    uint32_t total_in = job->total_in;
    auto status = qpl_submit_job(hw_job_ptr_pool[hw_job_ptr_queue.front()]);
    if (status == QPL_STS_OK) {
      hw_job_status_array[hw_job_ptr_queue.front()] = 0;
      hw_job_ptr_queue.pop();
      // LOG(WARNING) << "hw_job_ptr_queue pop out of queue current size: " << hw_job_ptr_queue.size();
    } else if (status == QPL_STS_QUEUES_ARE_BUSY_ERR) {
      // LOG(WARNING) << "hw_job_ptr_queue QPL_STS_QUEUES_ARE_BUSY_ERR ";
      job->next_in_ptr = input;
      job->available_in = avl_in;
      job->total_in = total_in;
      break;
    } else {
      hw_job_status_array[hw_job_ptr_queue.front()] = 0;
      hw_job_ptr_queue.pop();
      // LOG(WARNING) << "hw_job_ptr_queue job " << hw_job_ptr_queue.front() << " status not ok: " << status << ", " << hw_job_ptr_queue.size();
    }
  }
  submitting_job = false;
  mtx.unlock();
}

bool QplJobHWPool::is_job_submitted(uint32_t job_id) {
  std::lock_guard<std::mutex> lock(mtx);
  // LOG(WARNING) << "is_job_submitted: " << hw_job_status_array[job_id];
  return hw_job_status_array[job_id] == 0;
}

}
#endif
