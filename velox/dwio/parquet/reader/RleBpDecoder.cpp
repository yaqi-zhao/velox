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

#include "velox/dwio/parquet/reader/RleBpDecoder.h"

#include "velox/dwio/common/BitPackDecoder.h"

namespace facebook::velox::parquet {

void RleBpDecoder::skip(uint64_t numValues) {
  while (numValues > 0) {
    if (!remainingValues_) {
      readHeader();
    }
    uint64_t count = std::min<int>(numValues, remainingValues_);
    remainingValues_ -= count;
    numValues -= count;
    if (!repeating_) {
      auto numBits = bitWidth_ * count + bitOffset_;
      bufferStart_ += numBits >> 3;
      bitOffset_ = numBits & 7;
    }
  }
}

void RleBpDecoder::readBits(
    int32_t numValues,
    uint64_t* FOLLY_NONNULL outputBuffer,
    bool* FOLLY_NULLABLE allOnes) {
  VELOX_CHECK_EQ(1, bitWidth_);
  auto toRead = numValues;
  int32_t numWritten = 0;
  if (allOnes) {
    // initialize the all ones indicator to false for safety.
    *allOnes = false;
  }
  while (toRead) {
    if (!remainingValues_) {
      readHeader();
    }
    auto consumed = std::min<int32_t>(toRead, remainingValues_);

    if (repeating_) {
      if (allOnes && value_ && toRead == numValues &&
          remainingValues_ >= numValues) {
        // The whole read is covered by a RLE of ones and 'allOnes' is
        // provided, so we can shortcut the read.
        remainingValues_ -= toRead;
        *allOnes = true;
        return;
      }

      bits::fillBits(
          outputBuffer, numWritten, numWritten + consumed, value_ != 0);
    } else {
      bits::copyBits(
          reinterpret_cast<const uint64_t*>(bufferStart_),
          bitOffset_,
          outputBuffer,
          numWritten,
          consumed);
      int64_t offset = bitOffset_ + consumed;
      bufferStart_ += offset >> 3;
      bitOffset_ = offset & 7;
    }
    numWritten += consumed;
    toRead -= consumed;
    remainingValues_ -= consumed;
  }
}


// void RleBpDecoder::readBits(
//     int32_t numValues,
//     uint64_t* FOLLY_NONNULL outputBuffer,
//     bool* FOLLY_NULLABLE allOnes) {
//   VELOX_CHECK_EQ(1, bitWidth_);
//   auto toRead = numValues;
//   int32_t numWritten = 0;
//   if (allOnes) {
//     // initialize the all ones indicator to false for safety.
//     *allOnes = false;
//   }
//   std::vector<uint32_t> qpl_job_ids;
//   while (toRead > 0) {
//     if (numRemainingUnpackedValues_ > 0) {
//       auto numValuesToRead =
//           std::min<uint64_t>(numValues, numRemainingUnpackedValues_);
//       copyRemainingUnpackedValues(outputBuffer, numValuesToRead);

//       numValues -= numValuesToRead;
//     } else {
//       if (remainingValues_ == 0) {
//         readHeader();
//       }

//       auto numValuesToRead = std::min<uint32_t>(toRead, remainingValues_);
//       if (repeating_) {

//         if (allOnes && value_ && toRead == numValues &&
//             remainingValues_ >= numValues) {
//           // The whole read is covered by a RLE of ones and 'allOnes' is
//           // provided, so we can shortcut the read.
//           remainingValues_ -= toRead;
//           *allOnes = true;
//           return;
//         }

//         std::fill(outputBuffer, outputBuffer + numValuesToRead, value_);
//         outputBuffer += numValuesToRead;
//         remainingValues_ -= numValuesToRead;
//       } else {
//         remainingUnpackedValuesOffset_ = 0;
//         // The parquet standard requires the bit packed values are always a
//         // multiple of 8. So we read a multiple of 8 values each time
        
//         dwio::common::unpack<T>(
//             reinterpret_cast<const uint8_t*&>(bufferStart_),
//             bufferEnd_ - bufferStart_,
// #ifndef VELOX_ENABLE_QPL
//             numValuesToRead & 0xfffffff8,
// #else
//             numValuesToRead,
// #endif
//             bitWidth_,
//             reinterpret_cast<T * FOLLY_NONNULL&>(outputBuffer),
//             qpl_job_ids);

// #ifndef VELOX_ENABLE_QPL
//         remainingValues_ -= (numValuesToRead & 0xfffffff8);
// #else
//         remainingValues_ -= numValuesToRead;
// #endif

// #ifndef VELOX_ENABLE_QPL          
//         // Unpack the next 8 values to remainingUnpackedValues_ if necessary
//         if ((numValuesToRead & 7) != 0) {
//           T* output = reinterpret_cast<T*>(remainingUnpackedValues_);
//           dwio::common::unpack<T>(
//               reinterpret_cast<const uint8_t*&>(bufferStart_),
//               bufferEnd_ - bufferStart_,
//               8,
//               bitWidth_,
//               output);
//           numRemainingUnpackedValues_ = 8;
//           remainingUnpackedValuesOffset_ = 0;

//           copyRemainingUnpackedValues(outputBuffer, numValuesToRead & 7);
//           remainingValues_ -= 8;
//         }
// #endif          
//       }

//       toRead -= numValuesToRead;
//     }
//   }

// #ifdef VELOX_ENABLE_QPL
//   facebook::velox::QplJobHWPool& qpl_job_pool = facebook::velox::QplJobHWPool::GetInstance();
//   for (int i = 0; i < qpl_job_ids.size(); i++) {
//     if (qpl_job_pool.job_status(qpl_job_ids[i])) {
//       auto status = qpl_wait_job(qpl_job_pool.GetJobById(qpl_job_ids[i]));
//       if (status != QPL_STS_OK) {
//         std::cout << "qpl execution error: " << status << std::endl;
//       }
      
//       qpl_fini_job(qpl_job_pool.GetJobById(qpl_job_ids[i]));
//       qpl_job_pool.ReleaseJob(qpl_job_ids[i]);
//     }
//   }
// #endif    
// }

void RleBpDecoder::readHeader() {
  bitOffset_ = 0;
  auto maxVarIntLen = std::min<uint64_t>(
      (uint64_t)folly::kMaxVarintLength64, bufferEnd_ - bufferStart_);
  folly::ByteRange headerRange(
      reinterpret_cast<const unsigned char*>(bufferStart_),
      reinterpret_cast<const unsigned char*>(bufferStart_ + maxVarIntLen));
  // decodeVarint() would advance headerRange's begin pointer.
  auto indicator = folly::decodeVarint(headerRange);
  // Advance bufferStart_ to the position after where the varint is read
  bufferStart_ = reinterpret_cast<const char*>(headerRange.begin());

  // 0 in low bit means repeating.
  repeating_ = (indicator & 1) == 0;
  uint32_t count = indicator >> 1;
  if (repeating_) {
    remainingValues_ = count;
    // Do not load past buffer end. Reports error in valgrind and could in
    // principle run into unmapped addresses.
    if (bufferStart_ <= lastSafeWord_) {
      value_ = *reinterpret_cast<const int64_t*>(bufferStart_) & bitMask_;
    } else {
      value_ = bits::loadPartialWord(
          reinterpret_cast<const uint8_t*>(bufferStart_), byteWidth_);
    }
    bufferStart_ += byteWidth_;
  } else {
    VELOX_CHECK_LT(0, count);
    VELOX_CHECK_LT(count, std::numeric_limits<int32_t>::max() / 8);
    remainingValues_ = count * 8;
  }
}

} // namespace facebook::velox::parquet
