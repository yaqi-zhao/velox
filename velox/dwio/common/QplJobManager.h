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

#include <queue>
#include <memory>

#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"

namespace facebook::velox::dwio::common {

enum TaskStatus {
  SUBMITTED = 0,
  PROCESSING = 1,
  FINISHED = 2,
  Error = 3,
};

class DecompressTask {
public:
    DecompressTask(TaskStatus status, const char* compressedData, const char* uncompressedData,
        uint32_t compressedSize, uint32_t uncompressedSize,
        parquet::thrift::CompressionCodec::type codec) :
        status_(status),
        codec_(codec),
        compressedData_(compressedData),
        uncompressedData_(uncompressedData),
        compressedSize_(compressedSize),
        decompressedSize_(uncompressedSize) {}
    ~DecompressTask() {}

    TaskStatus status() {
        return status_;
    }
    void setStatus(TaskStatus status) {
        status_ = status;
    }

private:
    TaskStatus status_;
    const parquet::thrift::CompressionCodec::type codec_;
    const char* compressedData_;
    const char* uncompressedData_;
    uint32_t compressedSize_;
    uint32_t decompressedSize_;

};

class QplJobManager
{
private:
    /* data */
    // task queue
    std::queue<std::unique_ptr<DecompressTask>> taskQueue_;
    static constexpr auto MAX_QUEUE_NUMBER = 1024; 
    std::mutex mutex_;
public:
    static QplJobManager& GetInstance();
    QplJobManager();
    ~QplJobManager();
    /**
     * Push a decompress task to the queue. If the queue number exceed 
     * MAX_QUEUE_NUMBER, return -1.
     * @param task the decompress task
     * @return 0 if OK, -1 if failed
     */
    int pushTask(std::unique_ptr<DecompressTask> task);

    std::unique_ptr<DecompressTask> popTask();
    int32_t taskNum();
    bool empty();
};

QplJobManager::QplJobManager(/* args */)
{
}

QplJobManager::~QplJobManager()
{
}


void executeDecompressJob();
} // namespace facebook::velox::dwio::common