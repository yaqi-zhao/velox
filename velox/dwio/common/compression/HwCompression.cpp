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

#include "velox/dwio/common/compression/HwCompression.h"
#include "velox/dwio/common/compression/PagedInputStream.h"

#include <folly/logging/xlog.h>

namespace facebook::velox::dwio::common::compression {

using dwio::common::encryption::Decrypter;
using dwio::common::encryption::Encrypter;
using facebook::velox::common::CompressionKind;
using memory::MemoryPool;


class GzipIAADecompressor : public AsyncDecompressor {
 public:
  explicit GzipIAADecompressor() {}

  explicit GzipIAADecompressor(
      uint64_t blockSize,
      const std::string& streamDebugInfo)
      : AsyncDecompressor{blockSize, streamDebugInfo} {}

  int decompress(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override;

  bool waitResult(int job_id) override;

  void releaseJob(int job_id) override;
};

int GzipIAADecompressor::decompress(
    const char* src,
    uint64_t srcLength,
    char* dest,
    uint64_t destLength) {
#ifdef VELOX_ENABLE_INTEL_IAA
  dwio::common::QplJobHWPool& qpl_job_pool =
      dwio::common::QplJobHWPool::getInstance();
  // int job_id = 0;
  auto deflate_job = qpl_job_pool.acquireDeflateJob();
  // qpl_job* job = qpl_job_pool.AcquireDeflateJob(job_id);
  auto job = deflate_job.second;
  if (job == nullptr) {
    LOG(WARNING) << "cannot AcquireDeflateJob ";
    return -1; // Invalid job id to illustrate the
               // failed decompress job.
  }
  job->op = qpl_op_decompress;
  job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(src));
  job->next_out_ptr = reinterpret_cast<uint8_t*>(dest);
  job->available_in = static_cast<uint32_t>(srcLength);
  job->available_out = static_cast<uint32_t>(destLength);
  job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_ZLIB_MODE;

  qpl_status status = qpl_submit_job(job);
  if (status == QPL_STS_QUEUES_ARE_BUSY_ERR) {
    qpl_job_pool.releaseJob(deflate_job.first);
    deflate_job = qpl_job_pool.acquireDeflateJob();
    job = deflate_job.second;
    if (job == nullptr) {
      LOG(WARNING)
          << "cannot acqure deflate job after QPL_STS_QUEUES_ARE_BUSY_ERR ";
      return -1; // Invalid job id to illustrate the
                 // failed decompress job.
    }
    job->op = qpl_op_decompress;
    job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(src));
    job->next_out_ptr = reinterpret_cast<uint8_t*>(dest);
    job->available_in = static_cast<uint32_t>(srcLength);
    job->available_out = static_cast<uint32_t>(destLength);
    job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_ZLIB_MODE;

    status = qpl_submit_job(job);
  }
  if (status != QPL_STS_OK) {
    qpl_job_pool.releaseJob(deflate_job.first);
    LOG(WARNING) << "cannot submit job, error status: " << status;
    return -1; // Invalid job id to illustrate the
               // failed decompress job.
  } else {
    return deflate_job.first;
  }
#else
  return -1;
#endif
}

bool GzipIAADecompressor::waitResult(int job_id) {
#ifdef VELOX_ENABLE_INTEL_IAA
  dwio::common::QplJobHWPool& qpl_job_pool =
      dwio::common::QplJobHWPool::getInstance();
  if (job_id <= 0 || job_id >= qpl_job_pool.MAX_JOB_NUMBER) {
    return true;
  }
  qpl_job* job = qpl_job_pool.getJobById(job_id);

  auto status = qpl_wait_job(job);
  qpl_job_pool.releaseJob(job_id);
  if (status == QPL_STS_OK) {
    return true;
  }
  LOG(WARNING) << "Decompress w/IAA error, status: " << status;
#endif
  return false;
}

void GzipIAADecompressor::releaseJob(int job_id) {
#ifdef VELOX_ENABLE_INTEL_IAA
  dwio::common::QplJobHWPool& qpl_job_pool =
      dwio::common::QplJobHWPool::getInstance();
  if (job_id <= 0 || job_id >= qpl_job_pool.MAX_JOB_NUMBER) {
    return;
  }
  return qpl_job_pool.releaseJob(job_id);
#endif
}

std::unique_ptr<dwio::common::compression::AsyncDecompressor>
createAsyncDecompressor(
    facebook::velox::common::CompressionKind kind,
    uint64_t bufferSize,
    const std::string& streamDebugInfo) {
  std::unique_ptr<AsyncDecompressor> decompressor;
  switch (static_cast<int64_t>(kind)) {
    case CompressionKind::CompressionKind_GZIP:
      return std::make_unique<GzipIAADecompressor>(bufferSize, streamDebugInfo);
    default:
      LOG(WARNING) << "Asynchronous mode not support for compression codec  "
                   << kind;
      return nullptr;
  }
  return nullptr;
}

}