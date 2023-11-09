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

#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/common/compression/HwCompression.h"

namespace facebook::velox::parquet {

struct PreDecompPageInfo {
  int64_t numValues; // Number of values in this row group
  int64_t visitedRows; // rows already read
  uint64_t pageStart{0};
  thrift::PageHeader dataPageHeader;
  const char* FOLLY_NULLABLE dataPageData{nullptr};
  BufferPtr uncompressedDataV1Data;
};

class IAAPageReader: public PageReader {
public:
  IAAPageReader (
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      memory::MemoryPool& pool,
      ParquetTypeWithIdPtr fileType,
      thrift::CompressionCodec::type codec,
      int64_t chunkSize): PageReader(std::move(stream), pool, fileType, codec, chunkSize) {
    dict_qpl_job_id = 0;
    data_qpl_job_id = 0;
    uncompressedDictData_ = nullptr;
    uncompressedDataV1Data_ = nullptr;
    // *(this->seekToPageFn)(int64_t) = (seekToPage);
  }
  ~IAAPageReader();

  PageReaderType getType() {
    return PageReaderType::IAA;
  };

  /// Pre-decompress GZIP page with IAA
  void preDecompressPage(bool& need_pre_decompress, int64_t numValues);
  void prefetchDataPageV1(const thrift::PageHeader& pageHeader);
  void prefetchDataPageV2(const thrift::PageHeader& pageHeader);
  void prefetchDictionary(const thrift::PageHeader& pageHeader);
  const bool getDecompRes(int job_id);
  void prefetchNextPage();
  bool seekToPreDecompPage(int64_t row);

  const bool iaaDecompress(
      const char* FOLLY_NONNULL pageData,
      uint32_t compressedSize,
      uint32_t uncompressedSize,
      BufferPtr& uncompressedData,
      int& qpl_job_id);
//   override method 
virtual void seekToPage(int64_t row);
void prepareDataPageV1(
    const thrift::PageHeader& pageHeader,
    int64_t row,
    bool job_success = false);
void prepareDictionary(
      const thrift::PageHeader& pageHeader,
      bool job_success = false);


private:
  // Used for pre-decompress
  BufferPtr uncompressedDictData_;
  BufferPtr uncompressedDataV1Data_;
  thrift::PageHeader dictPageHeader_;
  const char* FOLLY_NULLABLE dictPageData_{nullptr};
  bool needUncompressDict;

  thrift::PageHeader dataPageHeader_;
  const char* FOLLY_NULLABLE dataPageData_{nullptr};

  int dict_qpl_job_id;
  int data_qpl_job_id;

  bool pre_decompress_dict = false;
  bool pre_decompress_data = false;
  bool isWinSizeFit = false;
  PreDecompPageInfo rowGroupPageInfo_;
};

} // namespace facebook::velox::parquet
