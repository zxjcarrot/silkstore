/**
 * @ Author: Yunxiao Du
 * @ Create Time: 2021-05-27 20:34:02
 * @ Description: Allocate NVM memory to store append log 
 */

#ifndef SILKSTORE_NVMEM
#define SILKSTORE_NVMEM

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

class NvmManager;

class Nvmem{
    private:
        char *data_;
        NvmManager* nvmem_manger_;
        size_t index_;
        size_t size_;
        size_t remain_;
    public:
        Nvmem();
        Nvmem(char *data, size_t size, NvmManager* nvmem_manger);
        ~Nvmem();
        bool UpdateCounter(size_t counters);
        bool UpdateIndex(size_t index);
        size_t GetCounter();
        uint64_t GetBeginAddress();
        uint64_t Insert(const char *, int);
        void print();
};

} // namespace silkstore 
} // namespace leveldb

#endif