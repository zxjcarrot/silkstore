#ifndef NVMMANAGER
#define NVMMANAGER
 
#include "nvm/nvmem.h"
#include <iostream>
#include <queue>

namespace leveldb{
namespace silkstore{


class NvmManager{
 private:
    const char* nvm_file_;
    size_t index_;
    size_t size_;
    char *data_;
    std::queue<size_t> memUsage;

    void init();

 public:
     NvmManager();
     NvmManager(const char * nvm_file, size_t size = GB);
     ~NvmManager();
     Nvmem* allocate(size_t size = 30*MB);
     size_t getBeginAddress();
     void free();
 };

} // namespace silkstore 
} // namespace leveldb


#endif