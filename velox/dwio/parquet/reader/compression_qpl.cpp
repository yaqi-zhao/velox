#include "compression_qpl.h"
#include "velox/dwio/common/QplJobPool.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/BaseVector.h"

#include <mutex>
#include <atomic>
//#include <stdatomic.h>
//#include <stdbool.h>
#include <unistd.h>       // for syscall()
#include <sys/syscall.h> 
#include <thread>

#ifdef VELOX_ENABLE_QPL  

namespace facebook::velox::parquet {
#define job_size 100
static qpl_job *job_pool[job_size];
std::atomic<bool> job_status[job_size];
static bool initialized_job =false;
static int start=0;
std::mutex mtx;
bool Initjobs(qpl_path_t execution_path){
  //std::lock_guard<std::mutex> lg(mtx);
  mtx.lock();
  if(initialized_job){
    mtx.unlock();
    return true;
  }else{
    
    qpl_status status;
    uint32_t size;
    status = qpl_get_job_size(execution_path, &size);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job size getting.");
    }
    for(int i=0;i<job_size;i++){
      job_status[i]=false;
      uint8_t *job_buffer=new uint8_t[size];
      //job_buffer = std::make_unique<uint8_t[]>(size);
      job_pool[i] = reinterpret_cast<qpl_job *>(job_buffer);
      status = qpl_init_job(execution_path, job_pool[i]);
      if (status != QPL_STS_OK) {
          throw std::runtime_error("An error acquired during compression job initializing.");
          return false;
          //return Status::OK();
      }
    }
    mtx.unlock();
    initialized_job=true;
    return true;
    
  }
}
static inline size_t Getindex()
{
    size_t tsc = 0;
    unsigned lo, hi;
    __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi) : :);
    tsc = ((((uint64_t)hi) << 32) | (uint64_t)lo);
    return ((size_t)((tsc * 44485709377909ULL) >> 4)) % job_size;
}
int Qplcodec::Getjob(){
  if(!initialized_job){
    Initjobs(execute_path_);
  }
  //std::lock_guard<std::mutex> lg(mtx);
  size_t index = Getindex();
  bool expected  = false;
  while(job_status[index].compare_exchange_strong(expected,true)==false){
    expected  = false;
    index = Getindex();
  }

  job_=job_pool[index];
  return index;
}

bool Qplcodec::Compress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output){
    job_->op=qpl_op_compress;
    job_->level=(qpl_compression_levels)compression_level_;
    job_->next_in_ptr=const_cast<uint8_t*>(input);
    job_->next_out_ptr=output;
    job_->available_in=input_length;
    job_->available_out=output_buffer_length;
    job_->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;
    

    //decompression
    qpl_status status = qpl_execute_job(job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("Error while compression occurred.");
        return false;
    }else{
      return true;
    }

  }

bool Qplcodec::Decompress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output)  {
    if (output_buffer_length == 0) {
      // The zlib library does not allow *output to be NULL, even when
      // output_buffer_length is 0 (inflate() will return Z_STREAM_ERROR). We don't
      // consider this an error, so bail early if no output is expected. Note that we
      // don't signal an error if the input actually contains compressed data.
      return 0;
    }

    // Reset the stream for this block
    int job_id=Getjob();

    job_->op=qpl_op_decompress;
    job_->next_in_ptr=const_cast<uint8_t*>(input);
    job_->next_out_ptr=output;
    job_->available_in=input_length;
    job_->available_out=output_buffer_length;
    job_->flags=QPL_FLAG_FIRST | QPL_FLAG_LAST;

    //decompression
    qpl_status status = qpl_execute_job(job_);
    if (status != QPL_STS_OK) {
      std::cout << "Error while decompression occurred. status:  " << (int)status << std::endl;
        throw std::runtime_error("Error while decompression occurred.");
        std::atomic_store(&job_status[job_id],false);
    }else{
      std::atomic_store(&job_status[job_id],false);
      return true;
    }
}

uint32_t Qplcodec::DecompressSync(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output, bool isGzip)  {
    if (output_buffer_length == 0) {
      // The zlib library does not allow *output to be NULL, even when
      // output_buffer_length is 0 (inflate() will return Z_STREAM_ERROR). We don't
      // consider this an error, so bail early if no output is expected. Note that we
      // don't signal an error if the input actually contains compressed data.
      return 0;
    }

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
    job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST ;
    if (isGzip) {
      job->flags |= QPL_FLAG_GZIP_MODE;
    }    
    // job->numa_id = 1;

    //decompression
    qpl_status status = qpl_execute_job(job);
    qpl_fini_job(job);
    qpl_job_pool.ReleaseJob(job_id);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("Error while decompression sync occurred. Status: " + std::to_string(status));
    } else {
      return 0;
    }
}

uint32_t Qplcodec::DecompressAsync(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output, bool isGzip)  {
    if (output_buffer_length == 0) {
      // The zlib library does not allow *output to be NULL, even when
      // output_buffer_length is 0 (inflate() will return Z_STREAM_ERROR). We don't
      // consider this an error, so bail early if no output is expected. Note that we
      // don't signal an error if the input actually contains compressed data.
      return 0;
    }

    // Reset the stream for this block
    dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
    // return qpl_job_pool.MAX_JOB_NUMBER; 
    uint32_t job_id = 0;
    qpl_job* job = qpl_job_pool.AcquireDeflateJob(job_id);
    if (job == nullptr) {
        // std::cout << "cannot get qpl job" << std::endl;
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
    // job->numa_id = 1;

    //decompression
    // std::cout <<"id: " << std::this_thread::get_id()  << ", before submit , input: " << static_cast<const void *>(job->next_in_ptr) <<  std::endl;
    qpl_status status = qpl_submit_job(job);
    uint32_t check_time = 10;
    while (status == QPL_STS_QUEUES_ARE_BUSY_ERR && check_time < UINT32_MAX) {
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
      check_time++;
      status = qpl_submit_job(job);
        // std::cout << "id: " <<std::this_thread::get_id() << ", submit decompress job error : " << ", status: " << (int)status << ", input: " << static_cast<const void *>(job->next_in_ptr) <<  std::endl;
    }
    if (status != QPL_STS_OK) {
        qpl_job_pool.ReleaseJob(job_id);
        std::cout << "cannot submit job because of QPL_STS_QUEUES_ARE_BUSY_ERR" << std::endl;
        return qpl_job_pool.MAX_JOB_NUMBER; // Invalid job id to illustrate the failed decompress job.
    } else {
      return job_id;
    }
}

}
#endif