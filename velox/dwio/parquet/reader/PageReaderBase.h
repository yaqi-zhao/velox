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

#include <arrow/util/rle_encoding.h>
#include "velox/dwio/common/BitConcatenation.h"
#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/parquet/reader/BooleanDecoder.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/reader/RleBpDataDecoder.h"
#include "velox/dwio/parquet/reader/StringDecoder.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::parquet {
enum PageReaderType { COMMON = 0, IAA = 1 };
/**
 * Abstract page reader class.
 *
 * Reader object is used to process a page.
 *
 */
class PageReaderBase {
 public:
  virtual PageReaderType getType() = 0;
  virtual ~PageReaderBase() = default;

  /**
   * Skips the define decoder, if any, for 'numValues' top level
   * rows. Returns the number of non-nulls skipped. The range is the
   * current page.
   * @return the rows numbers skiped
   */
  virtual int32_t skipNulls(int32_t numRows) = 0;
  virtual void skipNullsOnly(int64_t numValues) = 0;

  /**
   * Advances 'numRows' top level rows.
   * @param numRows
   */
  virtual void skip(int64_t numRows) = 0;

  /* Applies 'visitor' to values in the ColumnChunk of 'this'. The
   * operation to perform and The operand rows are given by
   * 'visitor'. The rows are relative to the current position. The
   * current position after readWithVisitor is immediately after the
   * last accessed row.
   */
  // template <typename Visitor>
  // virtual  readWithVisitor(Visitor& visitor) = 0;

  virtual void clearDictionary() = 0;

  /* True if the current page holds dictionary indices.
   */
  virtual bool isDictionary() const = 0;

  /* Reads 'numValues' null flags into 'nulls' and advances the
   * decoders by as much. The read may span several pages. If there
   * are no nulls, buffer may be set to nullptr.
   */
  virtual void readNullsOnly(int64_t numValues, BufferPtr& buffer) = 0;
  ;

  virtual const VectorPtr& dictionaryValues(const TypePtr& type) = 0;
};
} // namespace facebook::velox::parquet