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

#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/common/QplJobPool.h"
#include "velox/dwio/parquet/qpl_reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"
#include "velox/type/Filter.h"

namespace facebook::velox::dwio::common {


template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class QplDictionaryColumnVisitor
    : public ColumnVisitor<T, TFilter, ExtractValues, isDense> {
    using super = ColumnVisitor<T, TFilter, ExtractValues, isDense>;

 public:

   QplDictionaryColumnVisitor(
      TFilter& filter,
      SelectiveColumnReader* reader,
      RowSet rows,
      ExtractValues values)
      : ColumnVisitor<T, TFilter, ExtractValues, isDense>(
            filter,
            reader,
            rows,
            values),
        type_(nullptr),
        state_(reader->scanState().rawState),
        width_(
            reader->type()->kind() == TypeKind::BIGINT        ? 8
                : reader->type()->kind() == TypeKind::INTEGER ? 4
                                                              : 2) {
        // this->dictPageHeader_ =  facebook::velox::parquet::thrift::PageHeader();
    }


  void setPageData(const char* FOLLY_NULLABLE dictPageData, facebook::velox::parquet::thrift::PageHeader& dictPageHeader,
    facebook::velox::parquet::qpl_reader::ParquetTypeWithIdPtr type) {
        this->dictPageHeader_ = dictPageHeader;
        this->type_ = type;
        this->dictPageData_ = dictPageData;
    }


//   template <bool hasFilter, bool hasHook, bool scatter>
//   void processRun(
//       const char* FOLLY_NULLABLE input,
//       facebook::velox::parquet::thrift::PageHeader& dictPageHeader,
//       facebook::velox::parquet::qpl_reader::ParquetTypeWithIdPtr type,
//       const int32_t* scatterRows,
//       std::vector<uint8_t>& mask_after_scan,
//       uint32_t& numValuesSize) {
//         if (!hasFilter) {
//             if (hasHook) {
//                 // TODO: ExtractToHook
//                 return;
//             }
//             // TODO: 
//             // if (inDict()) {
//             //     translateScatter<true, scatter>(
//             //         input, numInput, scatterRows, numValues, values);
//             // } else {
//             //     translateScatter<false, scatter>(
//             //         input, numInput, scatterRows, numValues, values);
//             // }
//             // super::rowIndex_ += numInput;
//             // numValues = scatter ? scatterRows[super::rowIndex_ - 1] + 1
//             //                     : numValues + numInput;
//             return;
//         }

//         dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
//         uint32_t job_id = 0;
//         qpl_job* job = qpl_job_pool.AcquireJob(job_id);

//         mask_after_scan.resize((dictPageHeader.dictionary_page_header.num_values + 7) / 8, 4);

//         job->flags   = QPL_FLAG_DECOMPRESS_ENABLE;
//         job->op = qpl_op_scan_range;
//         job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(input));
//         job->available_in  = dictPageHeader.compressed_page_size;
//         job->next_out_ptr = mask_after_scan.data();
//         job->available_out = static_cast<uint32_t>(mask_after_scan.size());
//         job->num_input_elements = dictPageHeader.dictionary_page_header.num_values;
//         job->out_bit_width      = qpl_ow_nom;

//         switch (super::filter_.kind()) {
//             case facebook::velox::common::FilterKind::kBigintRange: {
//                 auto rangeFilter = reinterpret_cast<facebook::velox::common::BigintRange*>(&(super::filter_));
//                 job->param_low          = rangeFilter->lower();
//                 job->param_high          = rangeFilter->upper(); 
//                 break;
//             }
//             default:
//                 std::cout << "error" << std::endl;
//                 break;
//         }

//         auto parquetType = type->parquetType_.value();
//         switch (parquetType) {
//             case facebook::velox::parquet::thrift::Type::INT32:
//             case facebook::velox::parquet::thrift::Type::INT64:
//             case facebook::velox::parquet::thrift::Type::FLOAT:
//             case facebook::velox::parquet::thrift::Type::DOUBLE: {
//                  int32_t typeSize = (parquetType == facebook::velox::parquet::thrift::Type::INT32 ||
//                                   parquetType == facebook::velox::parquet::thrift::Type::FLOAT)
//                   ? sizeof(float)
//                   : sizeof(double);
//                 job->src1_bit_width = typeSize * 8;
//                 break;
//             }
//             default:
//                 std::cout << "parquet data type unsupported" << std::endl;
//                 break;
//         }
//         auto status = qpl_execute_job(job);
//         // if (status != QPL_STS_OK) {
//         //     qpl_execute_job(job);
//         // }
//         VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
//         // std::cout << "dictionary decompress success" << std::endl;
//         numValuesSize = job->total_out;
//         qpl_fini_job(qpl_job_pool.GetJobById(job_id));
//         qpl_job_pool.ReleaseJob(job_id);

//         return;
//     }

//   template <bool hasFilter, bool hasHook, bool scatter>
//   void processRun(
//       const char* FOLLY_NULLABLE input,
//       facebook::velox::parquet::thrift::PageHeader& dictPageHeader,
//       facebook::velox::parquet::qpl_reader::ParquetTypeWithIdPtr type,
//       const int32_t* scatterRows,
//       std::vector<uint8_t>& mask_after_scan,
//       uint32_t& numValuesSize) {
//         if (!hasFilter) {
//             if (hasHook) {
//                 // TODO: ExtractToHook
//                 return;
//             }
//             return;
//         }

//         auto parquetType = type->parquetType_.value();
//         int32_t typeSize = 0;
//         switch (parquetType) {
//             case facebook::velox::parquet::thrift::Type::INT32:
//             case facebook::velox::parquet::thrift::Type::INT64:
//             case facebook::velox::parquet::thrift::Type::FLOAT:
//             case facebook::velox::parquet::thrift::Type::DOUBLE: {
//                  typeSize = (parquetType == facebook::velox::parquet::thrift::Type::INT32 ||
//                                   parquetType == facebook::velox::parquet::thrift::Type::FLOAT)
//                   ? sizeof(float)
//                   : sizeof(double);
//                   break;
//             }
//             default:
//                 std::cout << "parquet data type unsupported" << std::endl;
//                 break;
//         }

//         dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
//         uint32_t job_id = 0;
//         qpl_job* job = qpl_job_pool.AcquireJob(job_id);

//         // mask_after_scan.resize((dictPageHeader.dictionary_page_header.num_values + 7) / 8, 4);
//         mask_after_scan.resize(dictPageHeader.dictionary_page_header.num_values * typeSize, 0);

//         job->flags   = QPL_FLAG_FIRST | QPL_FLAG_LAST;
//         job->op = qpl_op_decompress;
//         job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(input));
//         job->available_in  = dictPageHeader.compressed_page_size;
//         job->next_out_ptr = mask_after_scan.data();
//         job->available_out = static_cast<uint32_t>(mask_after_scan.size());
//         job->num_input_elements = dictPageHeader.dictionary_page_header.num_values;
//         job->out_bit_width      = qpl_ow_nom;
//         job->src1_bit_width = typeSize * 8;

//         // switch (super::filter_.kind()) {
//         //     case facebook::velox::common::FilterKind::kBigintRange: {
//         //         auto rangeFilter = reinterpret_cast<facebook::velox::common::BigintRange*>(&(super::filter_));
//         //         job->param_low          = rangeFilter->lower();
//         //         job->param_high          = rangeFilter->upper(); 
//         //         break;
//         //     }
//         //     default:
//         //         std::cout << "error" << std::endl;
//         //         break;
//         // }

//         auto status = qpl_execute_job(job);
//         // if (status != QPL_STS_OK) {
//         //     qpl_execute_job(job);
//         // }
//         VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
//         // std::cout << "dictionary decompress success" << std::endl;
//         numValuesSize = job->total_out;
//         qpl_fini_job(qpl_job_pool.GetJobById(job_id));
//         qpl_job_pool.ReleaseJob(job_id);

//         return;
//     }

  template <bool hasFilter, bool hasHook, bool scatter>
  void processRun(
      const char* FOLLY_NULLABLE input,
      facebook::velox::parquet::thrift::PageHeader& dictPageHeader,
      facebook::velox::parquet::qpl_reader::ParquetTypeWithIdPtr type,
      const int32_t* scatterRows,
      uint32_t& dict_index,
      uint32_t& filter_val) {
        if (!hasFilter) {
            if (hasHook) {
                // TODO: ExtractToHook
                return;
            }
            return;
        }

        auto parquetType = type->parquetType_.value();
        int32_t typeSize = 0;
        switch (parquetType) {
            case facebook::velox::parquet::thrift::Type::INT32:
            case facebook::velox::parquet::thrift::Type::INT64:
            case facebook::velox::parquet::thrift::Type::FLOAT:
            case facebook::velox::parquet::thrift::Type::DOUBLE: {
                 typeSize = (parquetType == facebook::velox::parquet::thrift::Type::INT32 ||
                                  parquetType == facebook::velox::parquet::thrift::Type::FLOAT)
                  ? sizeof(float)
                  : sizeof(double);
                  break;
            }
            default:
                std::cout << "parquet data type unsupported" << std::endl;
                break;
        }

        dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
        uint32_t job_id = 0;
        qpl_job* job = qpl_job_pool.AcquireJob(job_id);

        // mask_after_scan.resize((dictPageHeader.dictionary_page_header.num_values + 7) / 8, 4);
        // mask_after_scan.resize(dictPageHeader.dictionary_page_header.num_values * typeSize, 0);

        job->flags   = QPL_FLAG_DECOMPRESS_ENABLE;
        job->op = qpl_op_scan_eq;
        job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(input));
        job->available_in  = dictPageHeader.compressed_page_size;
        job->next_out_ptr = (uint8_t*)(&dict_index);
        job->available_out = sizeof(dict_index);
        job->num_input_elements = dictPageHeader.dictionary_page_header.num_values;
        job->out_bit_width      = qpl_ow_32;
        job->src1_bit_width = typeSize * 8;

        switch (super::filter_.kind()) {
            case facebook::velox::common::FilterKind::kBigintRange: {
                auto rangeFilter = reinterpret_cast<facebook::velox::common::BigintRange*>(&(super::filter_));
                job->param_low          = rangeFilter->lower();
                filter_val = job->param_low;
                // job->param_high          = rangeFilter->upper(); 
                break;
            }
            default:
                std::cout << "error" << std::endl;
                break;
        }

        auto status = qpl_execute_job(job);
        // if (status != QPL_STS_OK) {
        //     qpl_execute_job(job);
        // }
        VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
        // std::cout << "dictionary decompress success" << std::endl;
        qpl_fini_job(qpl_job_pool.GetJobById(job_id));
        qpl_job_pool.ReleaseJob(job_id);

        return;
    }

    void decompressDic(const char* FOLLY_NULLABLE input,
        uint32_t input_len,
        uint32_t type_size) {
            dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
            uint32_t job_id = 0;
            qpl_job* job = qpl_job_pool.AcquireJob(job_id);

            job->op = qpl_op_decompress;
            job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(input));
            job->available_in = input_len;
            job->next_out_ptr = reinterpret_cast<const uint8_t*>(state_.dictionary.values);
            job->available_out = state_.dictionary.numValues * type_size;;
            job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;


            auto status = qpl_execute_job(job);
            VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
            qpl_fini_job(qpl_job_pool.GetJobById(job_id));
            qpl_job_pool.ReleaseJob(job_id);
            return;
    }

  template <bool hasFilter, bool hasHook, bool scatter>
  void processRun_1(
      const char* FOLLY_NULLABLE input,
      facebook::velox::parquet::thrift::PageHeader& dictPageHeader,
      facebook::velox::parquet::qpl_reader::ParquetTypeWithIdPtr type,
      const int32_t* scatterRows,
      std::vector<uint8_t>& mask_after_scan,
      uint32_t& numValuesSize) {
        if (!hasFilter) {
            if (hasHook) {
                // TODO: ExtractToHook
                return;
            }
            decompressDic(input, dictPageHeader.compressed_page_size, 4);
            return;
        }

        decompressDic(input, dictPageHeader.compressed_page_size, 4);
        // processScan()
        return;
    }


  protected:
    facebook::velox::parquet::thrift::PageHeader dictPageHeader_;
    facebook::velox::parquet::qpl_reader::ParquetTypeWithIdPtr type_;
    const char* FOLLY_NULLABLE dictPageData_{nullptr};
    const uint8_t width_;

    RawScanState state_;


};




} // namespace facebook::velox::dwio::common