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

#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/BitPackDecoder.h"
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/dwio/common/TypeUtil.h"
#include "velox/dwio/parquet/qpl_reader/RleBpDecoder.h"

using std::chrono::system_clock;

namespace facebook::velox::parquet::qpl_reader {

// This class will be used for dictionary Ids or other data that is RLE/BP
// encded.o
class RleBpDataDecoder : public facebook::velox::parquet::qpl_reader::RleBpDecoder {
 public:
  using super = facebook::velox::parquet::qpl_reader::RleBpDecoder;

  RleBpDataDecoder(
      const char* FOLLY_NONNULL start,
      const char* FOLLY_NONNULL end,
      uint8_t bitWidth)
      : super::RleBpDecoder{start, end, bitWidth} {}

  // RleBpDataDecoder(
  //     const char* FOLLY_NONNULL pageData,
  //     thrift::PageHeader pageHeader)
  //     : super::RleBpDecoder{pageData, pageHeader, 0} {}

  template <bool hasNulls>
  inline void skip(
      int32_t numValues,
      int32_t current,
      const uint64_t* FOLLY_NULLABLE nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }

    super::skip(numValues);
  }

  void skip(uint64_t numValues) {
    super::skip(numValues);
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* FOLLY_NULLABLE nulls, Visitor visitor) {
    if (dwio::common::useFastPath<Visitor, hasNulls>(visitor)) {
      fastPath<hasNulls>(nulls, visitor);
      return;
    }
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    int32_t toSkip;
    bool atEnd = false;
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
        toSkip = visitor.processNull(atEnd);
      } else {
        if (hasNulls && !allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }

        // We are at a non-null value on a row to visit.
        if (!remainingValues_) {
          readHeader();
        }
        if (repeating_) {
          toSkip = visitor.process(value_, atEnd);
        } else {
          value_ = readBitField();
          toSkip = visitor.process(value_, atEnd);
        }
        --remainingValues_;
      }
      ++current;
      if (toSkip) {
        skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 private:
  template <bool hasNulls, typename Visitor>
  void fastPath(const uint64_t* FOLLY_NULLABLE nulls, Visitor& visitor) {
    constexpr bool hasFilter =
        !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
    constexpr bool hasHook =
        !std::is_same_v<typename Visitor::HookType, dwio::common::NoHook>;
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);
    if (hasNulls) {
      raw_vector<int32_t>* innerVector = nullptr;
      auto outerVector = &visitor.outerNonNullRows();
      if (Visitor::dense || rowsAsRange.back() == rowsAsRange.size() - 1) {
        dwio::common::nonNullRowsFromDense(nulls, numRows, *outerVector);
        if (outerVector->empty()) {
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
        bulkScan<hasFilter, hasHook, true>(
            folly::Range<const int32_t*>(rows, outerVector->size()),
            outerVector->data(),
            visitor);
      } else {
        innerVector = &visitor.innerNonNullRows();
        int32_t tailSkip = -1;
        auto anyNulls = dwio::common::nonNullRowsFromSparse < hasFilter,
             !hasFilter &&
            !hasHook >
                (nulls,
                 rowsAsRange,
                 *innerVector,
                 *outerVector,
                 (hasFilter || hasHook) ? nullptr : visitor.rawNulls(numRows),
                 tailSkip);
        if (anyNulls) {
          visitor.setHasNulls();
        }
        if (innerVector->empty()) {
          skip<false>(tailSkip, 0, nullptr);
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
        bulkScan<hasFilter, hasHook, true>(
            *innerVector, outerVector->data(), visitor);
        skip<false>(tailSkip, 0, nullptr);
      }
    } else {
      bulkScan<hasFilter, hasHook, false>(rowsAsRange, nullptr, visitor);
    }
  }


  template <bool hasFilter, bool hasHook, bool scatter, typename Visitor>
  void processRun(
      const int32_t* FOLLY_NONNULL rows,
      int32_t rowIndex,
      int32_t currentRow,
      int32_t numRows,
      const int32_t* FOLLY_NULLABLE scatterRows,
      int32_t* FOLLY_NULLABLE filterHits,
      typename Visitor::DataType* FOLLY_NONNULL values,
      int32_t& numValues,
      std::vector<uint32_t>& qpl_job_ids,
      Visitor& visitor) {
    auto numBits = bitOffset_ +
        (rows[rowIndex + numRows - 1] + 1 - currentRow) * bitWidth_;

    uint32_t values_0 = 0;
    using TValues = typename std::remove_reference<decltype(values_0)>::type;
    // using TValues = typename std::remove_reference<decltype(values[0])>::type;
    using TIndex = typename std::make_signed_t<
        typename dwio::common::make_index<TValues>::type>;
#ifndef VELOX_ENABLE_QPL        
    if (sizeof(TIndex) == 4) {
      // std::cout << "TINndex is 4" << std::endl;
      facebook::velox::dwio::common::unpack_uint<uint32_t>(reinterpret_cast<const uint8_t*&>(super::bufferStart_),
        super::bufferEnd_ - super::bufferStart_,
        numRows,
        bitWidth_,
        reinterpret_cast<uint32_t*>(values) + numValues, 
        qpl_job_ids);
    } else {
      facebook::velox::dwio::common::unpack(
          reinterpret_cast<const uint64_t*>(super::bufferStart_),
          bitOffset_,
          folly::Range<const int32_t*>(rows + rowIndex, numRows),
          currentRow,
          bitWidth_,
          super::bufferEnd_,
          reinterpret_cast<TIndex*>(values) + numValues);
    }
#else
    facebook::velox::dwio::common::unpack(
        reinterpret_cast<const uint64_t*>(super::bufferStart_),
        bitOffset_,
        folly::Range<const int32_t*>(rows + rowIndex, numRows),
        currentRow,
        bitWidth_,
        super::bufferEnd_,
        reinterpret_cast<TIndex*>(values) + numValues);
#endif

    super::bufferStart_ += numBits >> 3;
    bitOffset_ = numBits & 7;
    visitor.template processRun<hasFilter, hasHook, scatter>(
        values + numValues,
        numRows,
        scatterRows,
        filterHits,
        values,
        numValues);
  }

  // Returns 1. how many of 'rows' are in the current run 2. the
  // distance in rows from the current row to the first row after the
  // last in rows that falls in the current run.
  template <bool dense>
  std::pair<int32_t, std::int32_t> findNumInRun(
      const int32_t* FOLLY_NONNULL rows,
      int32_t rowIndex,
      int32_t numRows,
      int32_t currentRow) {
    DCHECK_LT(rowIndex, numRows);
    if (dense) {
      auto left = std::min<int32_t>(remainingValues_, numRows - rowIndex);
      return std::make_pair(left, left);
    }
    if (rows[rowIndex] - currentRow >= remainingValues_) {
      return std::make_pair(0, 0);
    }
    if (rows[numRows - 1] - currentRow < remainingValues_) {
      return std::pair(numRows - rowIndex, rows[numRows - 1] - currentRow + 1);
    }
    auto range = folly::Range<const int32_t*>(
        rows + rowIndex,
        std::min<int32_t>(remainingValues_, numRows - rowIndex));
    auto endOfRun = currentRow + remainingValues_;
    auto bound = std::lower_bound(range.begin(), range.end(), endOfRun);
    return std::make_pair(bound - range.begin(), bound[-1] - currentRow + 1);
  }


#ifdef VELOX_ENABLE_QPL
template <bool hasFilter, bool hasHook, bool scatter, typename Visitor>
  void bulkScan_1(
      folly::Range<const int32_t*> nonNullRows,
      const int32_t* FOLLY_NULLABLE scatterRows,
      Visitor& visitor) {
    auto startTime = system_clock::now();
    auto numAllRows = visitor.numRows();
    visitor.setRows(nonNullRows);
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    // std::cout << "numAllRows" << numAllRows << ", numRows: " << numRows << std::endl;
    auto values = visitor.rawValues(numRows);
    int32_t numValues = 0;
    auto filterHits = hasFilter ? visitor.outputRows(numRows) : nullptr;

    using TValues = typename std::remove_reference<decltype(values[0])>::type;
    using TIndex = typename std::make_signed_t<typename dwio::common::make_index<TValues>::type>;
    if (sizeof(TIndex) == sizeof(uint32_t)) {
      dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
      uint32_t job_id = 0;
      qpl_job* job = qpl_job_pool.AcquireJob(job_id);
      job->op = qpl_op_extract;
      job->param_low = 0;
      job->param_high = numAllRows;
      // job->op = qpl_op_scan_range;
      // job->param_low = 0;
      // job->param_high = 20;
      job->out_bit_width = qpl_ow_32;
      job->src1_bit_width = bitWidth_;
      job->parser = qpl_p_parquet_rle;
      job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(super::bufferStart_ - 1));
      job->available_in = super::bufferEnd_ - super::bufferStart_ + 1;
      job->next_out_ptr = reinterpret_cast<uint8_t*>(values);
      job->available_out = static_cast<uint32_t>(numRows * sizeof(TIndex)); 
      job->num_input_elements = numAllRows;

      auto status = qpl_execute_job(job);
      VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
      qpl_fini_job(qpl_job_pool.GetJobById(job_id));
      qpl_job_pool.ReleaseJob(job_id);
    }

    visitor.template processRun<hasFilter, hasHook, scatter>(
        values,
        numRows,
        scatterRows,
        filterHits,
        values,
        numValues);
    auto curTime = system_clock::now();
    size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
          curTime - startTime).count();
    
    // printf("Rle IAA decoder_%d    time:%dus\n", int(numAllRows),  (int)(msElapsed));         
    if (visitor.atEnd()) {
      visitor.setNumValues(hasFilter ? numValues : numAllRows);
      return;
    }        
    return;   
  }
#endif


  template <bool hasFilter, bool hasHook, bool scatter, typename Visitor>
  void bulkScan(
      folly::Range<const int32_t*> nonNullRows,
      const int32_t* FOLLY_NULLABLE scatterRows,
      Visitor& visitor) {
    auto startTime = system_clock::now();
    auto numAllRows = visitor.numRows();
    visitor.setRows(nonNullRows);
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    // std::cout << "numAllRows" << numAllRows << ", numRows: " << numRows << std::endl;
    auto rowIndex = 0;
    int32_t currentRow = 0;
    auto values = visitor.rawValues(numRows);
    auto filterHits = hasFilter ? visitor.outputRows(numRows) : nullptr;
    int32_t numValues = 0;
#ifdef VELOX_ENABLE_QPL
    using TValues = typename std::remove_reference<decltype(values[0])>::type;
    using TIndex = typename std::make_signed_t<typename dwio::common::make_index<TValues>::type>;
    if (sizeof(TIndex) == sizeof(uint32_t) && bitWidth_ > 1) {
      using TFilter = typename std::remove_reference<decltype(visitor.filter())>::type;
      if (std::is_same_v<TFilter, velox::common::BigintRange>) {
        return bulkScan_1<hasFilter, hasHook, scatter>(
              nonNullRows, scatterRows, visitor);
      }
    }
#endif    

    std::vector<uint32_t> qpl_job_ids;
    for (;;) {
      if (remainingValues_) {
        auto [numInRun, numAdvanced] =
            findNumInRun<Visitor::dense>(rows, rowIndex, numRows, currentRow);
        if (!numInRun) {
          // We are not at end and the next row of interest is after this run.
          VELOX_CHECK(!numAdvanced, "Would advance past end of RLEv1 run");
        } else if (repeating_) {
          visitor.template processRle<hasFilter, hasHook, scatter>(
              value_,
              0,
              numInRun,
              currentRow,
              scatterRows,
              filterHits,
              values,
              numValues);
        } else {
          processRun<hasFilter, hasHook, scatter>(
              rows,
              rowIndex,
              currentRow,
              numInRun,
              scatterRows,
              filterHits,
              values,
              numValues,
              qpl_job_ids,
              visitor);
        }
        remainingValues_ -= numAdvanced;
        currentRow += numAdvanced;
        rowIndex += numInRun;
        if (visitor.atEnd()) {
          visitor.setNumValues(hasFilter ? numValues : numAllRows);
          auto curTime = system_clock::now();
          size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                curTime - startTime).count();
          
          // printf("Rle decoder_%d    time:%dus\n", int(numAllRows),  (int)(msElapsed));   
          return;
        }
        if (remainingValues_) {
          currentRow += remainingValues_;
          skip<false>(remainingValues_, -1, nullptr);
        }
      }
      readHeader();
    }
#ifdef  VELOX_QPL_ASYNC_MODE
    dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
    for (int i = 0; i < qpl_job_ids.size(); i++) {
      if (qpl_job_pool.job_status(qpl_job_ids[i])) {
        auto status = qpl_wait_job(qpl_job_pool.GetJobById(qpl_job_ids[i]));
        if (status != QPL_STS_OK) {
          std::cout << "qpl execution error: " << status << std::endl;
        }
        
        qpl_fini_job(qpl_job_pool.GetJobById(qpl_job_ids[i]));
        qpl_job_pool.ReleaseJob(qpl_job_ids[i]);
      }
    }
#endif   
  }

  // Loads a bit field from 'ptr' + bitOffset for up to 'bitWidth' bits. makes
  // sure not to access bytes past lastSafeWord + 7.
  static inline uint64_t safeLoadBits(
      const char* FOLLY_NONNULL ptr,
      int32_t bitOffset,
      uint8_t bitWidth,
      const char* FOLLY_NONNULL lastSafeWord) {
    VELOX_DCHECK_GE(7, bitOffset);
    VELOX_DCHECK_GE(56, bitWidth);
    if (ptr < lastSafeWord) {
      return *reinterpret_cast<const uint64_t*>(ptr) >> bitOffset;
    }
    int32_t byteWidth = bits::roundUp(bitOffset + bitWidth, 8) / 8;
    return bits::loadPartialWord(
               reinterpret_cast<const uint8_t*>(ptr), byteWidth) >>
        bitOffset;
  }

  // Reads one value of 'bitWithd_' bits and advances the position.
  int64_t readBitField() {
    auto value =
        safeLoadBits(
            super::bufferStart_, bitOffset_, bitWidth_, lastSafeWord_) &
        bitMask_;
    bitOffset_ += bitWidth_;
    super::bufferStart_ += bitOffset_ >> 3;
    bitOffset_ &= 7;
    return value;
  }
};

} // namespace facebook::velox::parquet
