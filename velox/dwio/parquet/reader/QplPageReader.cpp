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

#include "velox/dwio/parquet/reader/QplPageReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include "velox/vector/FlatVector.h"
#include <snappy.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <zlib.h>
#include <zstd.h>
// #include <immintrin.h>

#ifdef VELOX_ENABLE_QPL
namespace facebook::velox::parquet {

using thrift::Encoding;
using thrift::PageHeader;

void QplPageReader::preDecompressPage(int64_t numValues) {
  for (;;) {
    auto dataStart = pageStart_;
    if (chunkSize_ <= pageStart_) {
      // This may happen if seeking to exactly end of row group.
      numRepDefsInPage_ = 0;
      numRowsInPage_ = 0;
      break;
    }
    PageHeader pageHeader = readPageHeader();
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;
    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        prefetchDataPageV1(pageHeader);
        break; 
      case thrift::PageType::DATA_PAGE_V2:
        prefetchDataPageV2(pageHeader);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        prefetchDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions   
    }
    break;
  }
  rowGroupPageInfo_.numValues = numValues;
  rowGroupPageInfo_.visitedRows = 0;
}

void QplPageReader::prefetchNextPage() {
  if (rowGroupPageInfo_.visitedRows + numRowsInPage_ >= rowGroupPageInfo_.numValues) {
    return;
  }

  if (chunkSize_ <= pageStart_) {
    // std::cout << "Not support error: chunkSize_: " << chunkSize_ << " pageStart_" << pageStart_ << std::endl;
    // This may happen if seeking to exactly end of row group.
    return;
  }
  PageHeader pageHeader;
  bool res = readPageHeader_1(pageHeader);
  if (!res) {
    return;
  }
  rowGroupPageInfo_.pageStart = pageDataStart_ + pageHeader.compressed_page_size;
    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE: {
        dataPageHeader_ = pageHeader;
        VELOX_CHECK(
            pageHeader.type == thrift::PageType::DATA_PAGE &&
            pageHeader.__isset.data_page_header);
        
        rowGroupPageInfo_.dataPageData = readBytes(pageHeader.compressed_page_size, pageBuffer_);   
        pre_decompress_data = uncompressQplData(
              rowGroupPageInfo_.dataPageData,
              pageHeader.compressed_page_size,
              pageHeader.uncompressed_page_size,
              rowGroupPageInfo_.uncompressedDataV1Data,
              data_qpl_job_id);
        break;
      }
      case thrift::PageType::DATA_PAGE_V2:
        std::cout << "error" << std::endl;
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        std::cout << "error" << std::endl;
        break;
      default:
        break; // ignore INDEX page type and any other custom extensions   
    }

}

void QplPageReader::seekToPage(int64_t row) {
  defineDecoder_.reset();
  repeatDecoder_.reset();
  // 'rowOfPage_' is the row number of the first row of the next page.
  rowOfPage_ += numRowsInPage_;
  bool has_qpl = false;
  if (dict_qpl_job_id > 0) {
    bool job_success = waitQplJob(dict_qpl_job_id);
    prepareDict(dictPageHeader_, job_success);
    dict_qpl_job_id = 0;
    has_qpl = true;
  }
  if (data_qpl_job_id > 0) {
    bool job_success = waitQplJob(data_qpl_job_id);
    bool result = prepareData(dataPageHeader_, row, job_success);
    if (!result) {
      LOG(WARNING) << "Decompress w/IAA error, try again with software.";
      pre_decompress_data = false;
      result = prepareData(dataPageHeader_, row, job_success);
      if (!result) {
        VELOX_FAIL("Decomrpess fail!");
      }
    }
    data_qpl_job_id = 0;
    has_qpl = true;
  }

  if (has_qpl) {
    if (row == kRepDefOnly || row < rowOfPage_ + numRowsInPage_) {
      return;
    }
    updateRowInfoAfterPageSkipped();
  }
  
  for (;;) {
    auto dataStart = pageStart_;
    if (chunkSize_ <= pageStart_) {
      // This may happen if seeking to exactly end of row group.
      // std::cout << "1 Not support error: chunkSize_: " << chunkSize_ << " pageStart_" << pageStart_ << std::endl;
      numRepDefsInPage_ = 0;
      numRowsInPage_ = 0;
      break;
    }
    PageHeader pageHeader = readPageHeader();
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;

    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        prepareDataPageV1(pageHeader, row);
        break;
      case thrift::PageType::DATA_PAGE_V2:
        prepareDataPageV2(pageHeader, row);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        if (row == kRepDefOnly) {
          skipBytes(
              pageHeader.compressed_page_size,
              inputStream_.get(),
              bufferStart_,
              bufferEnd_);
          continue;
        }
        prepareDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions
    }
    if (row == kRepDefOnly || row < rowOfPage_ + numRowsInPage_) {
      break;
    }
    updateRowInfoAfterPageSkipped();
  }
}

bool QplPageReader::readPageHeader_1(PageHeader& pageHeader) {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }
  if (bufferEnd_ == bufferStart_) {
    return false;
  }

  std::shared_ptr<thrift::ThriftTransport> transport =
      std::make_shared<thrift::ThriftStreamingTransport>(
          inputStream_.get(), bufferStart_, bufferEnd_);
  apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport> protocol(
      transport);
  uint64_t readBytes;
  readBytes = pageHeader.read(&protocol);

  pageDataStart_ = pageStart_ + readBytes;
  return true;
}

PageHeader QplPageReader::readPageHeader() {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }
  PageHeader pageHeader;
  std::shared_ptr<thrift::ThriftTransport> transport =
      std::make_shared<thrift::ThriftStreamingTransport>(
          inputStream_.get(), bufferStart_, bufferEnd_);
  apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport> protocol(
      transport);
  uint64_t readBytes;
  readBytes = pageHeader.read(&protocol);

  pageDataStart_ = pageStart_ + readBytes;
  return pageHeader;
}

const char* QplPageReader::readBytes(int32_t size, BufferPtr& copy) {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer = nullptr;
    int32_t bufferSize = 0;
    if (!inputStream_->Next(&buffer, &bufferSize)) {
      VELOX_FAIL("Read past end");
    }
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + bufferSize;
  }
  if (bufferEnd_ - bufferStart_ >= size) {
    bufferStart_ += size;
    return bufferStart_ - size;
  }
  dwio::common::ensureCapacity<char>(copy, size, &pool_);
  dwio::common::readBytes(
      size,
      inputStream_.get(),
      copy->asMutable<char>(),
      bufferStart_,
      bufferEnd_);
  return copy->as<char>();
}

const bool FOLLY_NONNULL QplPageReader::uncompressQplData(
    const char* pageData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    BufferPtr& uncompressedData,
    uint32_t& qpl_job_id) {
    dwio::common::ensureCapacity<char>(
    uncompressedData, uncompressedSize, &pool_);

    bool isGzip = codec_ == thrift::CompressionCodec::GZIP;

    qpl_job_id = this->DecompressAsync(
        compressedSize,
        (const uint8_t*)pageData,
        uncompressedSize,
        (uint8_t *)uncompressedData->asMutable<char>(),
        isGzip);
    if (qpl_job_id >= dwio::common::QplJobHWPool::GetInstance().MAX_JOB_NUMBER) {
      return false;
    }
    return true;
}

uint32_t QplPageReader::DecompressAsync(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output, bool isGzip)  {
    // Reset the stream for this block
    dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
    uint32_t job_id = 0;
    qpl_job* job = qpl_job_pool.AcquireDeflateJob(job_id);
    if (job == nullptr) {
        return qpl_job_pool.MAX_JOB_NUMBER; // Invalid job id to illustrate the failed decompress job.
    }
    job->op = qpl_op_decompress;
    job->next_in_ptr = const_cast<uint8_t*>(input);
    job->next_out_ptr = output;
    job->available_in = input_length;
    job->available_out = output_buffer_length;
    job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
    if (isGzip) {
      job->flags |= QPL_FLAG_GZIP_MODE;
    }
    
    qpl_status status = qpl_submit_job(job);
    if (status == QPL_STS_QUEUES_ARE_BUSY_ERR) {
      qpl_job_pool.ReleaseJob(job_id);
      job = qpl_job_pool.AcquireDeflateJob(job_id);
      if (job == nullptr) {
          return qpl_job_pool.MAX_JOB_NUMBER; // Invalid job id to illustrate the failed decompress job.
      }
      job->op = qpl_op_decompress;
      job->next_in_ptr = const_cast<uint8_t*>(input);
      job->next_out_ptr = output;
      job->available_in = input_length;
      job->available_out = output_buffer_length;
      job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
      if (isGzip) {
        job->flags |= QPL_FLAG_GZIP_MODE;
      }

      _umwait(1, __rdtsc() + 1000);
      status = qpl_submit_job(job);
    }
    if (status != QPL_STS_OK) {
        qpl_job_pool.ReleaseJob(job_id);
        LOG(WARNING) << "qpl_submit_job false: " << status;
        return qpl_job_pool.MAX_JOB_NUMBER; // Invalid job id to illustrate the failed decompress job.
    } else {
      return job_id;
    }
}

const char* FOLLY_NONNULL QplPageReader::uncompressData(
    const char* pageData,
    uint32_t compressedSize,
    uint32_t uncompressedSize) {
  switch (codec_) {
    case thrift::CompressionCodec::UNCOMPRESSED:
      return pageData;
    case thrift::CompressionCodec::SNAPPY: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);

      size_t sizeFromSnappy;
      if (!snappy::GetUncompressedLength(
              pageData, compressedSize, &sizeFromSnappy)) {
        VELOX_FAIL("Snappy uncompressed size not available");
      }
      VELOX_CHECK_EQ(uncompressedSize, sizeFromSnappy);
      snappy::RawUncompress(
          pageData, compressedSize, uncompressedData_->asMutable<char>());
      return uncompressedData_->as<char>();
    }
    case thrift::CompressionCodec::ZSTD: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);

      auto ret = ZSTD_decompress(
          uncompressedData_->asMutable<char>(),
          uncompressedSize,
          pageData,
          compressedSize);
      VELOX_CHECK(
          !ZSTD_isError(ret),
          "ZSTD returned an error: ",
          ZSTD_getErrorName(ret));
      return uncompressedData_->as<char>();
    }
    case thrift::CompressionCodec::GZIP: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);
      auto qpl_job_id = this->DecompressAsync(
        compressedSize,
        (const uint8_t*)pageData,
        uncompressedSize,
        (uint8_t *)uncompressedData_->asMutable<char>(),
        true);
      if (qpl_job_id < dwio::common::QplJobHWPool::GetInstance().MAX_JOB_NUMBER) {
        dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
        if (qpl_wait_job(qpl_job_pool.GetJobById(qpl_job_id)) == QPL_STS_OK) {
          qpl_job_pool.ReleaseJob(qpl_job_id);
          return uncompressedData_->as<char>();
        }
        qpl_job_pool.ReleaseJob(qpl_job_id);
      }
      z_stream stream;
      memset(&stream, 0, sizeof(stream));
      constexpr int WINDOW_BITS = 15;
      // Determine if this is libz or gzip from header.
      constexpr int DETECT_CODEC = 32;
      // Initialize decompressor.
      auto ret = inflateInit2(&stream, WINDOW_BITS | DETECT_CODEC);
      VELOX_CHECK(
          (ret == Z_OK),
          "zlib inflateInit failed: {}",
          stream.msg ? stream.msg : "");
      auto inflateEndGuard = folly::makeGuard([&] {
        if (inflateEnd(&stream) != Z_OK) {
          LOG(WARNING) << "inflateEnd: " << (stream.msg ? stream.msg : "");
        }
      });
      // Decompress.
      stream.next_in =
          const_cast<Bytef*>(reinterpret_cast<const Bytef*>(pageData));
      stream.avail_in = static_cast<uInt>(compressedSize);
      stream.next_out =
          reinterpret_cast<Bytef*>(uncompressedData_->asMutable<char>());
      stream.avail_out = static_cast<uInt>(uncompressedSize);
      ret = inflate(&stream, Z_FINISH);
      VELOX_CHECK(
          ret == Z_STREAM_END,
          "GZipCodec failed: {}",
          stream.msg ? stream.msg : "");
      return uncompressedData_->as<char>();
    }   
    default:
      VELOX_FAIL("Unsupported Parquet compression type '{}'", codec_);
  }
}

void QplPageReader::setPageRowInfo(bool forRepDef) {
  if (isTopLevel_ || forRepDef || maxRepeat_ == 0) {
    numRowsInPage_ = numRepDefsInPage_;
  } else if (hasChunkRepDefs_) {
    ++pageIndex_;
    VELOX_CHECK_LT(
        pageIndex_,
        numLeavesInPage_.size(),
        "Seeking past known repdefs for non top level column page {}",
        pageIndex_);
    numRowsInPage_ = numLeavesInPage_[pageIndex_];
  } else {
    numRowsInPage_ = kRowsUnknown;
  }
}

void QplPageReader::readPageDefLevels() {
  VELOX_CHECK(kRowsUnknown == numRowsInPage_ || maxDefine_ > 1);
  definitionLevels_.resize(numRepDefsInPage_);
  wideDefineDecoder_->GetBatch(definitionLevels_.data(), numRepDefsInPage_);
  leafNulls_.resize(bits::nwords(numRepDefsInPage_));
  leafNullsSize_ = getLengthsAndNulls(
      LevelMode::kNulls,
      leafInfo_,

      0,
      numRepDefsInPage_,
      numRepDefsInPage_,
      nullptr,
      leafNulls_.data(),
      0);
  numRowsInPage_ = leafNullsSize_;
  numLeafNullsConsumed_ = 0;
}

void QplPageReader::updateRowInfoAfterPageSkipped() {
  rowOfPage_ += numRowsInPage_;
  if (hasChunkRepDefs_) {
    numLeafNullsConsumed_ = rowOfPage_;
  }
}

void QplPageReader::prefetchDataPageV1(const thrift::PageHeader& pageHeader) {
  dataPageHeader_ = pageHeader;
  VELOX_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  
  dataPageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);   
  pre_decompress_data = uncompressQplData(
        dataPageData_,
        pageHeader.compressed_page_size,
        pageHeader.uncompressed_page_size,
        uncompressedDataV1Data_,
        data_qpl_job_id);
  return;
}

void QplPageReader::prefetchDataPageV2(const thrift::PageHeader& pageHeader) {
  return;
}

void QplPageReader::prefetchDictionary(const thrift::PageHeader& pageHeader) {
  dictionary_.numValues = pageHeader.dictionary_page_header.num_values;
  dictionaryEncoding_ = pageHeader.dictionary_page_header.encoding;
  dictionary_.sorted = pageHeader.dictionary_page_header.__isset.is_sorted &&
      pageHeader.dictionary_page_header.is_sorted;
  dictPageHeader_ = pageHeader;
  VELOX_CHECK(
      dictionaryEncoding_ == Encoding::PLAIN_DICTIONARY ||
      dictionaryEncoding_ == Encoding::PLAIN);  
  dictPageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);

  pre_decompress_dict = uncompressQplData(
        dictPageData_,
        pageHeader.compressed_page_size,
        pageHeader.uncompressed_page_size,
        uncompressedDictData_,
        dict_qpl_job_id);
  
  return;
}

void QplPageReader::prepareDict(const thrift::PageHeader& pageHeader, bool job_success) {
  if (!pre_decompress_dict || !job_success) {
      dictPageData_ = uncompressData(
      dictPageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);
  } else{
    dictPageData_ = uncompressedDictData_->as<char>();
  }

  auto parquetType = type_->parquetType_.value();
  switch (parquetType) {
    case thrift::Type::INT32:
    case thrift::Type::INT64:
    case thrift::Type::FLOAT:
    case thrift::Type::DOUBLE: {
      int32_t typeSize = (parquetType == thrift::Type::INT32 ||
                          parquetType == thrift::Type::FLOAT)
          ? sizeof(float)
          : sizeof(double);
      auto numBytes = dictionary_.numValues * typeSize;
      if (type_->type->isShortDecimal() && parquetType == thrift::Type::INT32) {
        auto veloxTypeLength = type_->type->cppSizeInBytes();
        auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
        dictionary_.values =
            AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      } else {
        dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      }
      if (dictPageData_) {
        memcpy(dictionary_.values->asMutable<char>(), dictPageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes,
            inputStream_.get(),
            dictionary_.values->asMutable<char>(),
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type->isShortDecimal() && parquetType == thrift::Type::INT32) {
        auto values = dictionary_.values->asMutable<int64_t>();
        auto parquetValues = dictionary_.values->asMutable<int32_t>();
        for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
          // Expand the Parquet type length values to Velox type length.
          // We start from the end to allow in-place expansion.
          values[i] = parquetValues[i];
        }
      }
      break;
    }
    case thrift::Type::BYTE_ARRAY: {
      dictionary_.values =
          AlignedBuffer::allocate<StringView>(dictionary_.numValues, &pool_);
      auto numBytes = pageHeader.uncompressed_page_size;
      auto values = dictionary_.values->asMutable<StringView>();
      dictionary_.strings = AlignedBuffer::allocate<char>(numBytes, &pool_);
      auto strings = dictionary_.strings->asMutable<char>();
      if (dictPageData_) {
        memcpy(strings, dictPageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes, inputStream_.get(), strings, bufferStart_, bufferEnd_);
      }
      auto header = strings;
      for (auto i = 0; i < dictionary_.numValues; ++i) {
        auto length = *reinterpret_cast<const int32_t*>(header);
        values[i] = StringView(header + sizeof(int32_t), length);
        header += length + sizeof(int32_t);
      }
      VELOX_CHECK_EQ(header, strings + numBytes);
      break;
    }
    case thrift::Type::FIXED_LEN_BYTE_ARRAY: {
      auto parquetTypeLength = type_->typeLength_;
      auto numParquetBytes = dictionary_.numValues * parquetTypeLength;
      auto veloxTypeLength = type_->type->cppSizeInBytes();
      auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
      dictionary_.values = AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      auto data = dictionary_.values->asMutable<char>();
      // Read the data bytes.
      if (dictPageData_) {
        memcpy(data, dictPageData_, numParquetBytes);
      } else {
        dwio::common::readBytes(
            numParquetBytes,
            inputStream_.get(),
            data,
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type->isShortDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int64_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int64_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int64_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = __builtin_bswap64(values[i]);
        }
        break;
      } else if (type_->type->isLongDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int128_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int128_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int128_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = dwio::common::builtin_bswap128(values[i]);
        }
        break;
      }
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
    }
    case thrift::Type::INT96:
    default:
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
  }    
}

bool QplPageReader::prepareData(const thrift::PageHeader& pageHeader, int64_t row, bool job_success) {
  if (!pre_decompress_data || !job_success) {
      if (rowGroupPageInfo_.visitedRows > 0)  {
        dataPageData_ = rowGroupPageInfo_.dataPageData;
      }
      pageData_ = uncompressData(
      dataPageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);    
  } else {
    if (rowGroupPageInfo_.visitedRows > 0)  {
      BufferPtr tmp = uncompressedDataV1Data_;
      uncompressedDataV1Data_ = rowGroupPageInfo_.uncompressedDataV1Data;
      rowGroupPageInfo_.uncompressedDataV1Data = tmp;
      // dwio::common::ensureCapacity<char>(uncompressedDataV1Data_, pageHeader.uncompressed_page_size, &pool_);
      // // uncompressedDataV1Data_->copyFrom(rowGroupPageInfo_.uncompressedDataV1Data.get(), rowGroupPageInfo_.uncompressedDataV1Data->size());
      // memcpy(uncompressedDataV1Data_->asMutable<char>(), rowGroupPageInfo_.uncompressedDataV1Data->asMutable<char>(), pageHeader.uncompressed_page_size);
      
    }
    pageData_ = uncompressedDataV1Data_->as<char>();
  }
  numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  setPageRowInfo(row == kRepDefOnly);


  auto pageEnd = pageData_ + pageHeader.uncompressed_page_size;
  if (maxRepeat_ > 0) {
    uint32_t repeatLength = readField<int32_t>(pageData_);
    repeatDecoder_ = std::make_unique<arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));

    pageData_ += repeatLength;
  }

  if (maxDefine_ > 0) {
    auto defineLength = readField<uint32_t>(pageData_);
    if (maxDefine_ == 1) {
      defineDecoder_ = std::make_unique<RleBpDecoder>(
          pageData_,
          pageData_ + defineLength,
          arrow::bit_util::NumRequiredBits(maxDefine_));
    } else {
      wideDefineDecoder_ = std::make_unique<arrow::util::RleDecoder>(
          reinterpret_cast<const uint8_t*>(pageData_),
          defineLength,
          arrow::bit_util::NumRequiredBits(maxDefine_));
    }
    pageData_ += defineLength;
  }
  encodedDataSize_ = pageEnd - pageData_;
  if (encodedDataSize_ > pageHeader.uncompressed_page_size) {
    return false;
  }

  encoding_ = pageHeader.data_page_header.encoding;
  if (!hasChunkRepDefs_ && (numRowsInPage_ == kRowsUnknown || maxDefine_ > 1)) {
    readPageDefLevels();
  }
  if (row != kRepDefOnly) {
    makeDecoder();
  }
  return true;
}
void QplPageReader::prepareDataPageV1(const PageHeader& pageHeader, int64_t row) {
  VELOX_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  setPageRowInfo(row == kRepDefOnly);
  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
      numRowsInPage_ + rowOfPage_ <= row) {
    dwio::common::skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);

    return;
  }
  pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
  pageData_ = uncompressData(
      pageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);
  auto pageEnd = pageData_ + pageHeader.uncompressed_page_size;
  if (maxRepeat_ > 0) {
    uint32_t repeatLength = readField<int32_t>(pageData_);
    repeatDecoder_ = std::make_unique<arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));

    pageData_ += repeatLength;
  }

  if (maxDefine_ > 0) {
    auto defineLength = readField<uint32_t>(pageData_);
    if (maxDefine_ == 1) {
      defineDecoder_ = std::make_unique<RleBpDecoder>(
          pageData_,
          pageData_ + defineLength,
          arrow::bit_util::NumRequiredBits(maxDefine_));
    } else {
      wideDefineDecoder_ = std::make_unique<arrow::util::RleDecoder>(
          reinterpret_cast<const uint8_t*>(pageData_),
          defineLength,
          arrow::bit_util::NumRequiredBits(maxDefine_));
    }
    pageData_ += defineLength;
  }
  encodedDataSize_ = pageEnd - pageData_;

  encoding_ = pageHeader.data_page_header.encoding;
  if (!hasChunkRepDefs_ && (numRowsInPage_ == kRowsUnknown || maxDefine_ > 1)) {
    readPageDefLevels();
  }

  if (row != kRepDefOnly) {
    makeDecoder();
  }
}

void QplPageReader::prepareDataPageV2(const PageHeader& pageHeader, int64_t row) {
  VELOX_CHECK(pageHeader.__isset.data_page_header_v2);
  numRepDefsInPage_ = pageHeader.data_page_header_v2.num_values;
  setPageRowInfo(row == kRepDefOnly);
  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
      numRowsInPage_ + rowOfPage_ <= row) {
    skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);
    return;
  }

  uint32_t defineLength = maxDefine_ > 0
      ? pageHeader.data_page_header_v2.definition_levels_byte_length
      : 0;
  uint32_t repeatLength = maxRepeat_ > 0
      ? pageHeader.data_page_header_v2.repetition_levels_byte_length
      : 0;
  auto bytes = pageHeader.compressed_page_size;
  pageData_ = readBytes(bytes, pageBuffer_);

  if (repeatLength) {
    repeatDecoder_ = std::make_unique<arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    defineDecoder_ = std::make_unique<RleBpDecoder>(
        pageData_ + repeatLength,
        pageData_ + repeatLength + defineLength,
        arrow::bit_util::NumRequiredBits(maxDefine_));
  }
  auto levelsSize = repeatLength + defineLength;
  pageData_ += levelsSize;
  if (pageHeader.data_page_header_v2.__isset.is_compressed ||
      pageHeader.data_page_header_v2.is_compressed) {
    pageData_ = uncompressData(
        pageData_,
        pageHeader.compressed_page_size - levelsSize,
        pageHeader.uncompressed_page_size - levelsSize);
  }
  if (row == kRepDefOnly) {
    skipBytes(bytes, inputStream_.get(), bufferStart_, bufferEnd_);
    return;
  }

  encodedDataSize_ = pageHeader.uncompressed_page_size - levelsSize;
  encoding_ = pageHeader.data_page_header_v2.encoding;
  if (numRowsInPage_ == kRowsUnknown) {
    readPageDefLevels();
  }
  if (row != kRepDefOnly) {
    makeDecoder();
  }
}

void QplPageReader::prepareDictionary(const PageHeader& pageHeader) {
  dictionary_.numValues = pageHeader.dictionary_page_header.num_values;
  dictionaryEncoding_ = pageHeader.dictionary_page_header.encoding;
  dictionary_.sorted = pageHeader.dictionary_page_header.__isset.is_sorted &&
      pageHeader.dictionary_page_header.is_sorted;
  VELOX_CHECK(
      dictionaryEncoding_ == Encoding::PLAIN_DICTIONARY ||
      dictionaryEncoding_ == Encoding::PLAIN);

  if (codec_ != thrift::CompressionCodec::UNCOMPRESSED) {
    pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
    pageData_ = uncompressData(
        pageData_,
        pageHeader.compressed_page_size,
        pageHeader.uncompressed_page_size);
  }

  auto parquetType = type_->parquetType_.value();
  switch (parquetType) {
    case thrift::Type::INT32:
    case thrift::Type::INT64:
    case thrift::Type::FLOAT:
    case thrift::Type::DOUBLE: {
      int32_t typeSize = (parquetType == thrift::Type::INT32 ||
                          parquetType == thrift::Type::FLOAT)
          ? sizeof(float)
          : sizeof(double);
      auto numBytes = dictionary_.numValues * typeSize;
      if (type_->type->isShortDecimal() && parquetType == thrift::Type::INT32) {
        auto veloxTypeLength = type_->type->cppSizeInBytes();
        auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
        dictionary_.values =
            AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      } else {
        dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      }
      if (pageData_) {
        memcpy(dictionary_.values->asMutable<char>(), pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes,
            inputStream_.get(),
            dictionary_.values->asMutable<char>(),
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type->isShortDecimal() && parquetType == thrift::Type::INT32) {
        auto values = dictionary_.values->asMutable<int64_t>();
        auto parquetValues = dictionary_.values->asMutable<int32_t>();
        for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
          // Expand the Parquet type length values to Velox type length.
          // We start from the end to allow in-place expansion.
          values[i] = parquetValues[i];
        }
      }
      break;
    }
    case thrift::Type::BYTE_ARRAY: {
      dictionary_.values =
          AlignedBuffer::allocate<StringView>(dictionary_.numValues, &pool_);
      auto numBytes = pageHeader.uncompressed_page_size;
      auto values = dictionary_.values->asMutable<StringView>();
      dictionary_.strings = AlignedBuffer::allocate<char>(numBytes, &pool_);
      auto strings = dictionary_.strings->asMutable<char>();
      if (pageData_) {
        memcpy(strings, pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes, inputStream_.get(), strings, bufferStart_, bufferEnd_);
      }
      auto header = strings;
      for (auto i = 0; i < dictionary_.numValues; ++i) {
        auto length = *reinterpret_cast<const int32_t*>(header);
        values[i] = StringView(header + sizeof(int32_t), length);
        header += length + sizeof(int32_t);
      }
      VELOX_CHECK_EQ(header, strings + numBytes);
      break;
    }
    case thrift::Type::FIXED_LEN_BYTE_ARRAY: {
      auto parquetTypeLength = type_->typeLength_;
      auto numParquetBytes = dictionary_.numValues * parquetTypeLength;
      auto veloxTypeLength = type_->type->cppSizeInBytes();
      auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
      dictionary_.values = AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      auto data = dictionary_.values->asMutable<char>();
      // Read the data bytes.
      if (pageData_) {
        memcpy(data, pageData_, numParquetBytes);
      } else {
        dwio::common::readBytes(
            numParquetBytes,
            inputStream_.get(),
            data,
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type->isShortDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int64_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int64_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int64_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = __builtin_bswap64(values[i]);
        }
        break;
      } else if (type_->type->isLongDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int128_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int128_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int128_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = dwio::common::builtin_bswap128(values[i]);
        }
        break;
      }
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
    }
    case thrift::Type::INT96:
    default:
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
  }
}

void QplPageReader::makeFilterCache(dwio::common::ScanState& state) {
  VELOX_CHECK(
      !state.dictionary2.values, "Parquet supports only one dictionary");
  state.filterCache.resize(state.dictionary.numValues);
  simd::memset(
      state.filterCache.data(),
      dwio::common::FilterResult::kUnknown,
      state.filterCache.size());
  state.rawState.filterCache = state.filterCache.data();
}

namespace {
int32_t parquetTypeBytes(thrift::Type::type type) {
  switch (type) {
    case thrift::Type::INT32:
    case thrift::Type::FLOAT:
      return 4;
    case thrift::Type::INT64:
    case thrift::Type::DOUBLE:
      return 8;
    default:
      VELOX_FAIL("Type does not have a byte width {}", type);
  }
}
} // namespace

void QplPageReader::preloadRepDefs() {
  hasChunkRepDefs_ = true;
  while (pageStart_ < chunkSize_) {
    seekToPage(kRepDefOnly);
    auto begin = definitionLevels_.size();
    auto numLevels = definitionLevels_.size() + numRepDefsInPage_;
    definitionLevels_.resize(numLevels);
    wideDefineDecoder_->GetBatch(
        definitionLevels_.data() + begin, numRepDefsInPage_);
    if (repeatDecoder_) {
      repetitionLevels_.resize(numLevels);

      repeatDecoder_->GetBatch(
          repetitionLevels_.data() + begin, numRepDefsInPage_);
    }
    leafNulls_.resize(bits::nwords(leafNullsSize_ + numRepDefsInPage_));
    auto numLeaves = getLengthsAndNulls(
        LevelMode::kNulls,
        leafInfo_,
        begin,
        begin + numRepDefsInPage_,
        numRepDefsInPage_,
        nullptr,
        leafNulls_.data(),
        leafNullsSize_);
    leafNullsSize_ += numLeaves;
    numLeavesInPage_.push_back(numLeaves);
  }

  // Reset the input to start of column chunk.
  std::vector<uint64_t> rewind = {0};
  pageStart_ = 0;
  dwio::common::PositionProvider position(rewind);
  inputStream_->seekToPosition(position);
  bufferStart_ = bufferEnd_ = nullptr;
  rowOfPage_ = 0;
  numRowsInPage_ = 0;
  pageData_ = nullptr;
  dictPageData_ = nullptr;
  dataPageData_ = nullptr;
}

void QplPageReader::decodeRepDefs(int32_t numTopLevelRows) {
  if (definitionLevels_.empty()) {
    preloadRepDefs();
  }
  repDefBegin_ = repDefEnd_;
  int32_t numLevels = definitionLevels_.size();
  int32_t topFound = 0;
  int32_t i = repDefBegin_;
  if (maxRepeat_ > 0) {
    for (; i < numLevels; ++i) {
      if (repetitionLevels_[i] == 0) {
        ++topFound;
        if (topFound == numTopLevelRows + 1) {
          break;
        }
      }
    }
    repDefEnd_ = i;
  } else {
    repDefEnd_ = i + numTopLevelRows;
  }
}

int32_t QplPageReader::getLengthsAndNulls(
    LevelMode mode,
    const ::parquet::internal::LevelInfo& info,
    int32_t begin,
    int32_t end,
    int32_t maxItems,
    int32_t* lengths,
    uint64_t* nulls,
    int32_t nullsStartIndex) const {
  ::parquet::internal::ValidityBitmapInputOutput bits;
  bits.values_read_upper_bound = maxItems;
  bits.values_read = 0;
  bits.null_count = 0;
  bits.valid_bits = reinterpret_cast<uint8_t*>(nulls);
  bits.valid_bits_offset = nullsStartIndex;

  switch (mode) {
    case LevelMode::kNulls:
      DefLevelsToBitmap(
          definitionLevels_.data() + begin, end - begin, info, &bits);
      break;
    case LevelMode::kList: {
      ::parquet::internal::DefRepLevelsToList(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits,
          lengths);
      // Convert offsets to lengths.
      for (auto i = 0; i < bits.values_read; ++i) {
        lengths[i] = lengths[i + 1] - lengths[i];
      }
      break;
    }
    case LevelMode::kStructOverLists: {
      DefRepLevelsToBitmap(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits);
      break;
    }
  }
  return bits.values_read;
}

void QplPageReader::makeDecoder() {
  auto parquetType = type_->parquetType_.value();
  switch (encoding_) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
      if (encodedDataSize_ > dataPageHeader_.uncompressed_page_size) {
        std::cout << "encodedDataSize_: " << encodedDataSize_ << ", pre_decompress_data: " << pre_decompress_data << true <<  ", dataPageHeader_: " << dataPageHeader_.uncompressed_page_size << std::endl;
      }
      dictionaryIdDecoder_ = std::make_unique<RleBpDataDecoder>(
          pageData_ + 1, pageData_ + encodedDataSize_, pageData_[0]);
      break;
    case Encoding::PLAIN:
      switch (parquetType) {
        case thrift::Type::BOOLEAN:
          booleanDecoder_ = std::make_unique<BooleanDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::BYTE_ARRAY:
          stringDecoder_ = std::make_unique<StringDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::FIXED_LEN_BYTE_ARRAY:
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              type_->typeLength_,
              true);
          break;
        default: {
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              parquetTypeBytes(type_->parquetType_.value()));
        }
      }
      break;
    case Encoding::DELTA_BINARY_PACKED:
    default:
      VELOX_UNSUPPORTED("Encoding not supported yet");
  }
}

void QplPageReader::skip(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    seekToPage(firstUnvisited_ + numRows);
    if (hasChunkRepDefs_) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
    toSkip -= rowOfPage_ - firstUnvisited_;
    prefetchNextPage();
    rowGroupPageInfo_.visitedRows += numRowsInPage_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  toSkip = skipNulls(toSkip);

  // Skip the decoder
  if (isDictionary()) {
    dictionaryIdDecoder_->skip(toSkip);
  } else if (directDecoder_) {
    directDecoder_->skip(toSkip);
  } else if (stringDecoder_) {
    stringDecoder_->skip(toSkip);
  } else if (booleanDecoder_) {
    booleanDecoder_->skip(toSkip);
  } else {
    VELOX_FAIL("No decoder to skip");
  }
}

int32_t QplPageReader::skipNulls(int32_t numValues) {
  if (!defineDecoder_ && isTopLevel_) {
    return numValues;
  }
  VELOX_CHECK(1 == maxDefine_ || !leafNulls_.empty());
  dwio::common::ensureCapacity<bool>(tempNulls_, numValues, &pool_);
  tempNulls_->setSize(0);
  if (isTopLevel_) {
    bool allOnes;
    defineDecoder_->readBits(
        numValues, tempNulls_->asMutable<uint64_t>(), &allOnes);
    if (allOnes) {
      return numValues;
    }
  } else {
    readNulls(numValues, tempNulls_);
  }
  auto words = tempNulls_->as<uint64_t>();
  return bits::countBits(words, 0, numValues);
}

void QplPageReader::skipNullsOnly(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    seekToPage(firstUnvisited_ + numRows);
    firstUnvisited_ += numRows;
    toSkip = firstUnvisited_ - rowOfPage_;
    prefetchNextPage();
    rowGroupPageInfo_.visitedRows += numRowsInPage_;
  } else {
    firstUnvisited_ += numRows;
  }

  // Skip nulls
  skipNulls(toSkip);
}

void QplPageReader::readNullsOnly(int64_t numValues, BufferPtr& buffer) {
  VELOX_CHECK(!maxRepeat_);
  auto toRead = numValues;
  if (buffer) {
    dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  }
  nullConcatenation_.reset(buffer);
  while (toRead) {
    auto availableOnPage = rowOfPage_ + numRowsInPage_ - firstUnvisited_;
    if (!availableOnPage) {
      seekToPage(firstUnvisited_);
      availableOnPage = numRowsInPage_;
      prefetchNextPage();
      rowGroupPageInfo_.visitedRows += numRowsInPage_;
    }
    auto numRead = std::min(availableOnPage, toRead);
    auto nulls = readNulls(numRead, nullsInReadRange_);
    toRead -= numRead;
    nullConcatenation_.append(nulls, 0, numRead);
    firstUnvisited_ += numRead;
  }
  buffer = nullConcatenation_.buffer();
}

const uint64_t* FOLLY_NULLABLE
QplPageReader::readNulls(int32_t numValues, BufferPtr& buffer) {
  if (maxDefine_ == 0) {
    buffer = nullptr;
    return nullptr;
  }
  dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  if (isTopLevel_) {
    VELOX_CHECK_EQ(1, maxDefine_);
    bool allOnes;
    defineDecoder_->readBits(
        numValues, buffer->asMutable<uint64_t>(), &allOnes);
    return allOnes ? nullptr : buffer->as<uint64_t>();
  }
  bits::copyBits(
      leafNulls_.data(),
      numLeafNullsConsumed_,
      buffer->asMutable<uint64_t>(),
      0,
      numValues);
  numLeafNullsConsumed_ += numValues;
  return buffer->as<uint64_t>();
}

void QplPageReader::startVisit(folly::Range<const vector_size_t*> rows) {
  visitorRows_ = rows.data();
  numVisitorRows_ = rows.size();
  currentVisitorRow_ = 0;
  initialRowOfPage_ = rowOfPage_;
  visitBase_ = firstUnvisited_;
}

bool QplPageReader::rowsForPage(
    dwio::common::SelectiveColumnReader& reader,
    bool hasFilter,
    bool mayProduceNulls,
    folly::Range<const vector_size_t*>& rows,
    const uint64_t* FOLLY_NULLABLE& nulls) {
  if (currentVisitorRow_ == numVisitorRows_) {
    return false;
  }
  int32_t numToVisit;
  // Check if the first row to go to is in the current page. If not, seek to the
  // page that contains the row.
  auto rowZero = visitBase_ + visitorRows_[currentVisitorRow_];
  if (rowZero >= rowOfPage_ + numRowsInPage_) {
    seekToPage(rowZero);
    if (hasChunkRepDefs_) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
    prefetchNextPage();
    rowGroupPageInfo_.visitedRows += numRowsInPage_;
  }
  auto& scanState = reader.scanState();
  if (isDictionary()) {
    if (scanState.dictionary.values != dictionary_.values) {
      scanState.dictionary = dictionary_;
      if (hasFilter) {
        makeFilterCache(scanState);
      }
      scanState.updateRawState();
    }
  } else {
    if (scanState.dictionary.values) {
      // If there are previous pages in the current read, nulls read
      // from them are in 'nullConcatenation_' Put this into the
      // reader for the time of dedictionarizing so we don't read
      // undefined dictionary indices.
      if (mayProduceNulls && reader.numValues()) {
        reader.setNulls(nullConcatenation_.buffer());
      }
      reader.dedictionarize();
      // The nulls across all pages are in nullConcatenation_. Clear
      // the nulls and let the prepareNulls below reserve nulls for
      // the new page.
      reader.setNulls(nullptr);
      scanState.dictionary.clear();
    }
  }

  // Then check how many of the rows to visit are on the same page as the
  // current one.
  int32_t firstOnNextPage = rowOfPage_ + numRowsInPage_ - visitBase_;
  if (firstOnNextPage > visitorRows_[numVisitorRows_ - 1]) {
    // All the remaining rows are on this page.
    numToVisit = numVisitorRows_ - currentVisitorRow_;
  } else {
    // Find the last row in the rows to visit that is on this page.
    auto rangeLeft = folly::Range<const int32_t*>(
        visitorRows_ + currentVisitorRow_,
        numVisitorRows_ - currentVisitorRow_);
    auto it =
        std::lower_bound(rangeLeft.begin(), rangeLeft.end(), firstOnNextPage);
    assert(it != rangeLeft.end());
    assert(it != rangeLeft.begin());
    numToVisit = it - (visitorRows_ + currentVisitorRow_);
  }
  // If the page did not change and this is the first call, we can return a view
  // on the original visitor rows.
  if (rowOfPage_ == initialRowOfPage_ && currentVisitorRow_ == 0) {
    nulls =
        readNulls(visitorRows_[numToVisit - 1] + 1, reader.nullsInReadRange());
    rowNumberBias_ = 0;
    rows = folly::Range<const vector_size_t*>(visitorRows_, numToVisit);
  } else {
    // We scale row numbers to be relative to first on this page.
    auto pageOffset = rowOfPage_ - visitBase_;
    rowNumberBias_ = visitorRows_[currentVisitorRow_];
    skip(rowNumberBias_ - pageOffset);
    // The decoder is positioned at 'visitorRows_[currentVisitorRow_']'
    // We copy the rows to visit with a bias, so that the first to visit has
    // offset 0.
    rowsCopy_->resize(numToVisit);
    auto copy = rowsCopy_->data();
    // Subtract 'rowNumberBias_' from the rows to visit on this page.
    // 'copy' has a writable tail of SIMD width, so no special case for end of
    // loop.
    for (auto i = 0; i < numToVisit; i += xsimd::batch<int32_t>::size) {
      auto numbers = xsimd::batch<int32_t>::load_unaligned(
                         &visitorRows_[i + currentVisitorRow_]) -
          rowNumberBias_;
      numbers.store_unaligned(copy);
      copy += xsimd::batch<int32_t>::size;
    }
    nulls = readNulls(rowsCopy_->back() + 1, reader.nullsInReadRange());
    rows = folly::Range<const vector_size_t*>(
        rowsCopy_->data(), rowsCopy_->size());
  }
  reader.prepareNulls(rows, nulls != nullptr, currentVisitorRow_);
  currentVisitorRow_ += numToVisit;
  firstUnvisited_ = visitBase_ + visitorRows_[currentVisitorRow_ - 1] + 1;
  return true;
}

const VectorPtr& QplPageReader::dictionaryValues(const TypePtr& type) {
  if (!dictionaryValues_) {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &pool_,
        type,
        nullptr,
        dictionary_.numValues,
        dictionary_.values,
        std::vector<BufferPtr>{dictionary_.strings});
  }
  return dictionaryValues_;
}

bool QplPageReader::waitQplJob(uint32_t job_id) {
  dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
  if (job_id <= 0 || job_id >= qpl_job_pool.MAX_JOB_NUMBER) {
    job_id = 0;
    return true;
  }
  qpl_job* job = qpl_job_pool.GetJobById(job_id);

  auto status = qpl_check_job(job);
  uint32_t check_time = 0;
  while (status == QPL_STS_BEING_PROCESSED && check_time < UINT32_MAX - 1)
  {
      // _umonitor(job->next_out_ptr + job->available_out - 1);
      // _umwait(1, __rdtsc() + 1000);
      _tpause(1, 1000);
      status = qpl_check_job(job);
      check_time++;
  } 
  
  qpl_fini_job(job);
  qpl_job_pool.ReleaseJob(job_id);
  if (status != QPL_STS_OK) {
    LOG(WARNING) << "Qpl job execution failed, status: " << status;
      return false;   
  }
  return true;
}

QplPageReader::~QplPageReader() {
    dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
    if (dict_qpl_job_id > 0 && dict_qpl_job_id < qpl_job_pool.MAX_JOB_NUMBER) {
        qpl_job* job = qpl_job_pool.GetJobById(dict_qpl_job_id);
        qpl_fini_job(job);
        qpl_job_pool.ReleaseJob(dict_qpl_job_id);
        dict_qpl_job_id = 0;
    }
    if (data_qpl_job_id > 0 && data_qpl_job_id < qpl_job_pool.MAX_JOB_NUMBER) {
        qpl_job* job = qpl_job_pool.GetJobById(data_qpl_job_id);
        qpl_fini_job(job);
        qpl_job_pool.ReleaseJob(data_qpl_job_id);
        data_qpl_job_id = 0;        
    }
}

} // namespace facebook::velox::parquet

#endif
