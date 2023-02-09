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

#include "velox/dwio/common/BitPackDecoder.h"
#include "velox/common/base/Nulls.h"
#include "velox/dwio/parquet/reader/RleBpDataDecoder.h"

#include <folly/Random.h>
#include <gtest/gtest.h>
#include <iostream>
#include <chrono>

using namespace facebook::velox::dwio::common;
using namespace facebook::velox;

class BitPackDecoderTest : public testing::Test {
 protected:
  void SetUp() {
    for (int32_t i = 0; i < 4099072; i++) {
      auto randomInt = folly::Random::rand64();
      randomInts_.push_back(randomInt);
    }
    populateBitPackedData();
    allRowNumbers_.resize(randomInts_.size());
    std::iota(allRowNumbers_.begin(), allRowNumbers_.end(), 0);
    oddRowNumbers_.resize(randomInts_.size() / 2);
    for (auto i = 0; i < oddRowNumbers_.size(); i++) {
      oddRowNumbers_[i] = i * 2 + 1;
    }
    allRows_ = RowSet(allRowNumbers_);
    oddRows_ = RowSet(oddRowNumbers_);
  }

  void populateBitPackedData() {
    bitPackedData_.resize(33);
    for (auto bitWidth = 1; bitWidth <= 32; ++bitWidth) {
      auto numWords = bits::roundUp(randomInts_.size() * bitWidth, 64) / 64;
      bitPackedData_[bitWidth].resize(numWords);
      auto source = randomInts_.data();
      auto destination =
          reinterpret_cast<uint64_t*>(bitPackedData_[bitWidth].data());
      for (auto i = 0; i < randomInts_.size(); ++i) {
        bits::copyBits(
            source,
            i * sizeof(*source) * 8,
            destination,
            i * bitWidth,
            bitWidth);
      }
    }
  }

  template <typename T, typename U>
  void checkDecodeResult(
      const T* reference,
      RowSet rows,
      int8_t bitWidth,
      const U* result) {
    uint64_t mask = bits::lowMask(bitWidth);
    for (auto i = 0; i < rows.size(); ++i) {
      uint64_t original = reference[rows[i]] & mask;
      ASSERT_EQ(original, result[i])
          << " at " << i << " with bitWidth " << bitWidth;
    }
  }

  template <typename T>
  void testUnpack(uint8_t width, RowSet rows) {
    std::vector<T> result(rows.size());
    int32_t start = 0;

    int32_t batch = 1;
    // Read the encoding in progressively larger batches, each time 3x more than
    // previous.
    auto bits = bitPackedData_[width].data();
    do {
      auto row = rows[start];
      int32_t bit = row * width;
      auto byteOffset = bit / 8;
      auto bitOffset = bit & 7;
      auto numRows = std::min<int32_t>(start + batch, rows.size()) - start;
      auto bitsPointer = reinterpret_cast<const uint64_t*>(
          reinterpret_cast<const char*>(bits) + byteOffset);

      // end is the first unaddressable address after the bit packed data. We
      // set this to be the byte of the last bit field to exercise the safe
      // path.
      auto end = reinterpret_cast<const char*>(bitsPointer) +
          (((start + rows[numRows - 1] - rows[start]) * width) / 8);
      unpack(
          bitsPointer,
          bitOffset,
          RowSet(&rows[start], numRows),
          rows[start],
          width,
          end,
          result.data() + start);
      start += batch;
      batch *= 3;
    } while (start < rows.size());
    checkDecodeResult(randomInts_.data(), rows, width, result.data());
  }

  template <typename T>
  void testUnpack_1(uint8_t width, RowSet rows) {
    std::vector<T> result(rows.size());
    int32_t start = 0;

    int32_t batch = 1;
    // Read the encoding in progressively larger batches, each time 3x more than
    // previous.
    auto bits = bitPackedData_[width].data();
      auto row = rows[start];
      int32_t bit = row * width;
      auto byteOffset = bit / 8;
      auto bitOffset = bit & 7;
      auto numBytes = bits::roundUp((rows.back() + 1) * width, 8) / 8;

      auto numRows = std::min<int32_t>(start + batch, rows.size()) - start;
      auto bitsPointer = reinterpret_cast<const uint64_t*>(
          reinterpret_cast<const char*>(bits) + byteOffset);
      auto end = reinterpret_cast<const char*>(bitsPointer) + numBytes;

      unpack(
          bitsPointer,
          0,
          rows,
          0,
          width,
          end,
          result.data());

    checkDecodeResult(randomInts_.data(), rows, width, result.data());
  }

  uint32_t bytes(uint64_t numValues, uint8_t bitWidth) {
    return (numValues * bitWidth + 7) / 8;
  }

  // Tests
  template <typename T>
  void testUnpack(uint8_t bitWidth) {
    auto numValues = randomInts_.size();
    std::vector<T> result(numValues);
    // sleep(20);
    // T* result = reinterpret_cast<T*>(std::malloc(numValues * sizeof(T)));

    const uint8_t* inputIter =
        reinterpret_cast<const uint8_t*>(bitPackedData_[bitWidth].data());
    T* outputIter = reinterpret_cast<T*>(result.data());
    facebook::velox::dwio::common::unpack<T>(
        inputIter, bytes(numValues, bitWidth), numValues, bitWidth, outputIter);

    checkDecodeResult(randomInts_.data(), allRows_, bitWidth, result.data());

    // std::free(result);
    // result = NULL;
  }

  std::vector<uint64_t> randomInts_;

  // All indices into 'randomInts_'.
  std::vector<int32_t> allRowNumbers_;

  // Indices into odd positions in 'randomInts_'.
  std::vector<int32_t> oddRowNumbers_;

  // Array of bit packed representations of randomInts_. The array at index i
  // is packed i bits wide and the values come from the low bits of
  std::vector<std::vector<uint64_t>> bitPackedData_;
  RowSet allRows_;
  RowSet oddRows_;
};

using std::chrono::system_clock;

TEST_F(BitPackDecoderTest, allWidths) {
  for (auto width = 0; width < bitPackedData_.size() - 1; ++width) {
    auto startTime = system_clock::now();
    testUnpack_1<int32_t>(width, allRows_);
    auto curTime = system_clock::now();
    size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             curTime - startTime)
                             .count();
    std::cout << "BitPackDecoderTest, allWidths int32_t width: " << width << ", allRows: " << allRows_.size() << ", time: " << msElapsed << "us" << std::endl;
    // std::cout << "BitPackDecoderTest, allWidths int64_t width: " << width << ", allRows: " << allRows_.size() << std::endl;
    // testUnpack<int64_t>(width, allRows_);
    // startTime = system_clock::now();
    // testUnpack_1<int32_t>(width, oddRows_);
    // curTime = system_clock::now();
    // msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
    //                          curTime - startTime)
    //                          .count();
    // std::cout << "BitPackDecoderTest, allWidths int32_t width: " << width << ", oddRows_: " << oddRows_.size() << ", time: " << msElapsed << "us" << std::endl;
    // std::cout << "BitPackDecoderTest, allWidths int64_t width: " << width << ", oddRows_: " << oddRows_.size() << std::endl;
    // testUnpack<int64_t>(width, oddRows_);
  }
}

TEST_F(BitPackDecoderTest, uint8AllRows) {
  for (auto width = 1; width <= 8; ++width) {
    auto startTime = system_clock::now();
    testUnpack<uint8_t>(width);
    auto curTime = system_clock::now();
    size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             curTime - startTime)
                             .count();
    std::cout << "uint8AllRows width: " << width << ", time: " << msElapsed << "us" << std::endl;     
  }
}

// TEST_F(BitPackDecoderTest, uint16AllRows) {
//   for (auto width = 1; width <= 16; ++width) {
//     auto startTime = system_clock::now();
//     // sleep(20);
//     testUnpack<uint16_t>(width);
//     auto curTime = system_clock::now();
//     size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
//                              curTime - startTime)
//                              .count();
//     std::cout << "uint16AllRows width: " << width << ", time: " << msElapsed << "us" << std::endl;    
//   }
// }

TEST_F(BitPackDecoderTest, uint32AllRows) {
  for (auto width = 1; width <= 32; ++width) {
    auto startTime = system_clock::now();
    testUnpack<uint32_t>(width);
    auto curTime = system_clock::now();
    size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             curTime - startTime)
                             .count();
    std::cout << "uint32AllRows width: " << width << ", time: " << msElapsed << "us" << std::endl;
   }
}
