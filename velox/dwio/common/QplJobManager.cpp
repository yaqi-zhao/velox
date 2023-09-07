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

#include "dwio/common/QplJobManager.h"

namespace facebook::velox::dwio::common {

QplJobManager& QplJobManager::GetInstance() {
    static QplJobManager manager;
    return manager;
}

QplJobManager::QplJobManager() {

}
QplJobManager::~QplJobManager() {

}

int QplJobManager::pushTask(std::unique_ptr<DecompressTask> task) {
    // mutex
    std::lock_guard<std::mutex> lk(mutex_);
    taskQueue_.push(task);
}

std::unique_ptr<DecompressTask> QplJobManager::popTask() {
    std::lock_guard<std::mutex> lk(mutex_);
    std::unique_ptr<DecompressTask> task = std::move(taskQueue_.front());
    taskQueue_.pop();
    return task;
} 


void executeDecompressJob() {
    while (!QplJobManager::GetInstance().empty()) {
        // TaskManager::getTask();
        // doDecompressData();
        // TaskManager::setStatus  set task status to finished

    }
    return;

}

} // namespace facebook::velox::dwio::common