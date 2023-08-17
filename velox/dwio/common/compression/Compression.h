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

#include "velox/common/compression/Compression.h"
#include "velox/dwio/common/OutputStream.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/compression/CompressionBufferPool.h"
#include "velox/dwio/common/encryption/Encryption.h"

namespace facebook::velox::dwio::common::compression {

class Compressor {
 public:
  // zlib window bits determine the size of history buffer. If the value is
  // negative, then no zlib header/trailer or a check value will be added and
  // ABS(windowBits) will determine the buffer size.
  // https://zlib.net/manual.html
  static constexpr int DWRF_ORC_ZLIB_WINDOW_BITS = -15;
  static constexpr int PARQUET_ZLIB_WINDOW_BITS = 15;

  explicit Compressor(int32_t level) : level_{level} {}

  virtual ~Compressor() = default;

  virtual uint64_t compress(const void* src, void* dest, uint64_t length) = 0;

 protected:
  int32_t level_;
};

class Decompressor {
 public:
  explicit Decompressor(uint64_t blockSize, const std::string& streamDebugInfo)
      : blockSize_{blockSize}, streamDebugInfo_{streamDebugInfo} {}

  virtual ~Decompressor() = default;

  virtual uint64_t getUncompressedLength(
      const char* /* unused */,
      uint64_t /* unused */) const {
    return blockSize_;
  }

  virtual uint64_t decompress(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) = 0;

 protected:
  uint64_t blockSize_;
  const std::string streamDebugInfo_;
};

struct CompressionOptions {
  union Format {
    struct {
      int windowBits;
      int32_t compressionLevel;
    } zlib;

    struct {
      int32_t compressionLevel;
    } zstd;
  } format;

  uint32_t compressionThreshold;
};

static CompressionOptions getDwrfOrcCompressionOptions(
    velox::common::CompressionKind kind,
    uint32_t compressionThreshold,
    int32_t zlibCompressionLevel,
    int32_t zstdCompressionLevel) {
  CompressionOptions options;
  options.compressionThreshold = compressionThreshold;

  if (kind == velox::common::CompressionKind_ZLIB) {
    options.format.zlib.windowBits = Compressor::DWRF_ORC_ZLIB_WINDOW_BITS;
    options.format.zlib.compressionLevel = zlibCompressionLevel;
  } else if (kind == velox::common::CompressionKind_ZSTD) {
    options.format.zstd.compressionLevel = zstdCompressionLevel;
  }
  return options;
}

static CompressionOptions getDwrfOrcDecompressionOptions() {
  CompressionOptions options;
  options.format.zlib.windowBits = Compressor::DWRF_ORC_ZLIB_WINDOW_BITS;
  return options;
}

static CompressionOptions getParquetDecompressionOptions() {
  CompressionOptions options;
  options.format.zlib.windowBits = Compressor::PARQUET_ZLIB_WINDOW_BITS;
  return options;
}

/**
 * Create a decompressor for the given compression kind.
 * @param kind the compression type to implement
 * @param input the input stream that is the underlying source
 * @param bufferSize the maximum size of the buffer
 * @param pool the memory pool
 * @param useRawDecompression specify whether to perform raw decompression
 * @param compressedLength the compressed block length for raw decompression
 * @param options compression options to use
 */
std::unique_ptr<dwio::common::SeekableInputStream> createDecompressor(
    facebook::velox::common::CompressionKind kind,
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    uint64_t bufferSize,
    memory::MemoryPool& pool,
    const std::string& streamDebugInfo,
    const dwio::common::encryption::Decrypter* decryptr = nullptr,
    bool useRawDecompression = false,
    size_t compressedLength = 0,
    CompressionOptions options = getDwrfOrcDecompressionOptions());

/**
 * Create a compressor for the given compression kind.
 * @param kind the compression type to implement
 * @param bufferPool pool for compression buffer
 * @param bufferHolder buffer holder that handles buffer allocation and
 * collection
 * @param level compression level
 */
std::unique_ptr<BufferedOutputStream> createCompressor(
    facebook::velox::common::CompressionKind kind,
    CompressionBufferPool& bufferPool,
    DataBufferHolder& bufferHolder,
    CompressionOptions options,
    uint8_t pageHeaderSize,
    const dwio::common::encryption::Encrypter* encrypter = nullptr);

} // namespace facebook::velox::dwio::common::compression
