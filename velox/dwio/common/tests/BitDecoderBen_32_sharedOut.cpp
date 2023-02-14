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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/BitPackDecoder.h"

#ifdef __BMI2__
#include "velox/dwio/common/tests/Lemire/bmipacking32.h"
#endif

#include "velox/external/duckdb/duckdb-fastpforlib.hpp"
#include "velox/external/duckdb/parquet-amalgamation.hpp"
#include "velox/vector/TypeAliases.h"

#include <arrow/util/rle_encoding.h> // @manual
#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>
#include <iostream>

#include "velox/dwio/common/QplJobPool.h"

using namespace folly;
using namespace facebook::velox;
using std::chrono::system_clock;

using RowSet = folly::Range<const facebook::velox::vector_size_t*>;

// static const uint64_t kNumValues = 1024768 * 8;
uint64_t kNumValues = 1024768 * 64 * 4; //1G

// Array of bit packed representations of randomInts_u32. The array at index i
// is packed i bits wide and the values come from the low bits of
std::vector<std::vector<uint64_t>> bitPackedData;

std::vector<uint32_t> result32;
std::vector<std::vector<uint32_t>> results;

std::vector<int32_t> allRowNumbers;
RowSet allRows;

static size_t len_u32 = 0;
std::vector<uint32_t> randomInts_u32;
std::vector<uint64_t> randomInts_u32_result;

#define BYTES(numValues, bitWidth) (numValues * bitWidth + 7) / 8

  template <typename T, typename U>
  void checkDecodeResult(
      const T* reference,
      RowSet rows,
      int8_t bitWidth,
      const U* result) {
    uint32_t mask = bits::lowMask(bitWidth);
    for (auto i = 0; i < kNumValues; ++i) {
      uint64_t original = reference[rows[i]] & mask;
      if (original != result[i]) {
        std::cout << "check fail at " << i << " with bitwidth " << bitWidth << ", origin: " << reference[i] << ", actual: " << result[i] << std::endl;
        return;
      }
    }
  }


template <typename T>
void veloxBitUnpack(uint8_t bitWidth, T* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());
  auto startTime = system_clock::now();
  facebook::velox::dwio::common::unpack<T>(
      inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
#ifdef VELOX_QPL_ASYNC_MODE  
  facebook::velox::QplJobHWPool& qpl_job_pool = facebook::velox::QplJobHWPool::GetInstance();
  for (int i = 0; i < 1024; i++) {
    if (qpl_job_pool.job_status(i)) {
      auto status = qpl_wait_job(qpl_job_pool.GetJobById(i));
      if (status != QPL_STS_OK) {
        std::cout << "qpl execution error: " << status << std::endl;
      }
      qpl_fini_job(qpl_job_pool.GetJobById(i));
      qpl_job_pool.ReleaseJob(i);
      
    }
  }
#endif    

  auto curTime = system_clock::now();
  size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        curTime - startTime).count();
  
  printf("unpack_%d_%d    time:%dus\n", int(bitWidth), int(sizeof(T) * 8), (int)(msElapsed));
}

void run_simple_benchmark() {
  printf("=======================================\n");
  printf("Benchmark Name      time:us  \n");
  for(int i = 1; i < 33; i++) {
    veloxBitUnpack<uint32_t>(i, result32.data());
    // checkDecodeResult(randomInts_u32.data(), allRows, i, result32.data());
  }
  printf("======================================\n");
}

template <typename T>
void veloxBitUnpack_1(uint8_t bitWidth, T* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());

  facebook::velox::dwio::common::unpack<T>(
      inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
  return;
}

void bitUnpack(uint8_t bitWidth, int index) {
  veloxBitUnpack_1<uint32_t>(bitWidth, results[index].data());
}


void parallelBitUnpack(uint8_t thread_num, uint8_t bitWidth) {
  auto startTime = system_clock::now();
  // std::vector<std::thread> thread_pool(thread_num);
  // for (int i = 0; i < thread_num; i++) {
  //   thread_pool[i] = std::thread(bitUnpack, bitWidth, i);
  // }

  for (int i = 0; i < thread_num; i++) {
    bitUnpack(bitWidth, i);
  }

  // for (auto& t: thread_pool) {
  //     t.join();
  //     // sleep(1);
  // }
  // auto curTime_0 = system_clock::now();
  // size_t msElapsed_0 = std::chrono::duration_cast<std::chrono::microseconds>(
  //       curTime_0 - startTime).count();
  // printf("unpack_%d_%d               init   time:%dus\n", int(bitWidth), int(sizeof(uint32_t) * 8), (int)(msElapsed_0));
#ifdef VELOX_QPL_ASYNC_MODE  
  facebook::velox::QplJobHWPool& qpl_job_pool = facebook::velox::QplJobHWPool::GetInstance();
  for (int i = 0; i < 1024; i++) {
    if (qpl_job_pool.job_status(i)) {
      auto status = qpl_wait_job(qpl_job_pool.GetJobById(i));
      if (status != QPL_STS_OK) {
        std::cout << "qpl execution error: " << status << std::endl;
      }
      
      qpl_fini_job(qpl_job_pool.GetJobById(i));
      qpl_job_pool.ReleaseJob(i);
    }
  }
#endif  
  auto curTime = system_clock::now();
  size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        curTime - startTime).count();
  printf("unpack_%d_%d                  time:%dus\n", int(bitWidth), int(sizeof(uint32_t) * 8), (int)(msElapsed));
  // checkDecodeResult(randomInts_u32.data(), allRows, bitWidth, results[0].data());
  // printf("check success");
  return;

}

void run_parallel_benchmark() {
  // sleep(20);
  printf("=======================================\n");
  auto startTime = system_clock::now();
  int iter_count = 10;
  int concurrency = 40;
  // set vector
  results.clear();
  results.resize(concurrency);
  for (int i = 0; i < concurrency; i++) {
    results[i].resize(kNumValues);
  }

  for (int iter = 1; iter < iter_count; iter++) {
    for (int i = concurrency; i <= concurrency; i=i*2) {
      for (int k = 0; k < concurrency; k++) {
        memset(&results[k][0], 0, results[k].size() * sizeof(results[k][0]));
      }
      
      printf("Parallel %d Benchmark      time:us  \n", i);
      for (int j = 2; j < 33; j++) {
        parallelBitUnpack(i, j);
      }
    }
  }
  auto curTime = system_clock::now();
  size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        curTime - startTime).count();
  printf("unpack      iter:%d       time:%dus\n", iter_count, (int)(msElapsed));
  printf("======================================\n");
}


void populateBitPacked() {
  bitPackedData.resize(33);
  for (auto bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    auto numWords = bits::roundUp(randomInts_u32.size() * bitWidth, 64) / 64;
    bitPackedData[bitWidth].resize(numWords);
    auto source = reinterpret_cast<uint64_t*>(randomInts_u32.data());
    auto destination =
        reinterpret_cast<uint64_t*>(bitPackedData[bitWidth].data());
    for (auto i = 0; i < randomInts_u32.size(); ++i) {
      bits::copyBits(source, i * 32, destination, i * bitWidth, bitWidth);
    }
  }

  allRowNumbers.resize(randomInts_u32.size());
  std::iota(allRowNumbers.begin(), allRowNumbers.end(), 0);

  allRows = RowSet(allRowNumbers);
}

 int32_t main(int32_t argc, char* argv[]) {
   auto startTime = system_clock::now();
    folly::init(&argc, &argv);

  // Populate uint32 buffer
  for (int total_num = 8; total_num <= 8; total_num = total_num* 2) {
    kNumValues = 1024768 * total_num;
    std::cout << "kNumValues: " << kNumValues << std::endl;
    for (int32_t i = 0; i < kNumValues; i++) {
      auto randomInt = folly::Random::rand32();
      randomInts_u32.push_back(randomInt);
    }
    randomInts_u32_result.resize(randomInts_u32.size());

    populateBitPacked();

    result32.resize(randomInts_u32.size());

    // run_simple_benchmark();
    run_parallel_benchmark();
    randomInts_u32.clear();
    auto curTime = system_clock::now();
    size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        curTime - startTime).count();
    printf("unpack      total_time:%dus\n", (int)(msElapsed));
  }



  return 0;
}
