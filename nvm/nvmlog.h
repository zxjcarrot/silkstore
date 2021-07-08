/**
 * @ Author: Yunxiao Du
 * @ Create Time: 2021-05-27 20:34:02
 * @ Description: Allocate NVM memory to store append log 
 */

#ifndef SILKSTORE_NVMLOG
#define SILKSTORE_NVMLOG

#include<cstdio>
#include<cstdlib>
#include<cstring>
#include <cassert>
#include <atomic>
#include <cmath>
#include <unistd.h>
#include <signal.h> 
#include <malloc.h>

#include "nvm/nvm-common.h"

// comment this out for using DRAM as NVM
#define NVMPOOL_REAL   
#ifdef NVMPOOL_REAL
// use  PMEM_MMAP_HINT=desired_address
// to map to a desired address
#include <libpmem.h>
#endif



namespace leveldb{
namespace silkstore{

        
class NvmLog{
    private:
        char *data_;
        size_t counter_;
        size_t index_;
        size_t cap_;
    public:
        NvmLog();
        NvmLog(char *, size_t);
        ~NvmLog();
        void append(int64_t);
        void reset();
        void print();
};

} // namespace silkstore 
} // namespace leveldb

#endif