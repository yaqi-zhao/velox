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

using namespace folly;
using namespace facebook::velox;
using std::chrono::system_clock;

using RowSet = folly::Range<const facebook::velox::vector_size_t*>;

// static const uint64_t kNumValues = 1024768 * 8;
uint64_t kNumValues = 1024768 * 16;

// Array of bit packed representations of randomInts_u32. The array at index i
// is packed i bits wide and the values come from the low bits of
std::vector<std::vector<uint64_t>> bitPackedData;

std::vector<uint16_t> result16;


static size_t len_u32 = 0;
std::vector<uint32_t> randomInts_u32;
std::vector<uint64_t> randomInts_u32_result;

static size_t len_u64 = 0;
std::vector<uint64_t> randomInts_u64;
std::vector<uint64_t> randomInts_u64_result;
std::vector<char> buffer_u64;

#define BYTES(numValues, bitWidth) (numValues * bitWidth + 7) / 8

template <typename T>
void veloxBitUnpack(uint8_t bitWidth, T* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());

  auto startTime = system_clock::now();
  facebook::velox::dwio::common::unpack<T>(
      inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
  auto curTime = system_clock::now();
  size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        curTime - startTime).count();

  printf("unpack_%d_%d    time:%dus\n", int(bitWidth), int(sizeof(T) * 8), (int)(msElapsed));
}

void run_simple_benchmark() {
  printf("=======================================\n");
  printf("Benchmark Name      time:us  \n");
  for (int i = 1; i < 17; i++) {
    veloxBitUnpack<uint16_t>(1, result16.data());
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

void bitUnpack(uint8_t bitWidth) {
  std::vector<uint16_t> result(kNumValues);
  veloxBitUnpack_1<uint16_t>(bitWidth, result.data());
}

void parallelBitUnpack(uint8_t thread_num, uint8_t bitWidth) {
  auto startTime = system_clock::now();
  std::vector<std::thread> thread_pool(thread_num);
  for (int i = 0; i < thread_num; i++) {
    thread_pool[i] = std::thread(bitUnpack, bitWidth);
  }
  for (auto& t: thread_pool) {
      t.join();
  }
  auto curTime = system_clock::now();
  size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        curTime - startTime).count();
  printf("unpack_%d_%d                  time:%dus\n", int(bitWidth), int(sizeof(uint8_t) * 8), (int)(msElapsed));
  return;

}

void run_parallel_benchmark() {
  printf("=======================================\n");
  for (int i = 1; i < 64; i=i*2) {
    printf("Parallel %d Benchmark      time:us  \n", i);
    for (int j = 1; j < 17; j++) {
      parallelBitUnpack(i, j);
    }
  }
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
}

 int32_t main(int32_t argc, char* argv[]) {
    folly::init(&argc, &argv);

  // Populate uint32 buffer
  // for (int total_num = 8; total_num < 32; total_num = total_num* 2) {
    kNumValues = 1024768 * 16;
    std::cout << "kNumValues: " << kNumValues << std::endl;
    for (int32_t i = 0; i < kNumValues; i++) {
      auto randomInt = folly::Random::rand32();
      randomInts_u32.push_back(randomInt);
    }
    randomInts_u32_result.resize(randomInts_u32.size());

    populateBitPacked();

    result16.resize(randomInts_u32.size());

    run_simple_benchmark();
    run_parallel_benchmark();
    randomInts_u32.clear();
  // }



  return 0;
}
