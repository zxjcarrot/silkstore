#ifndef NVMMANAGER
#define NVMMANAGER
 
#include "nvm/nvmem.h"
#include "nvm/nvmlog.h"
#include <iostream>
#include <queue>

#define LOGCAP 30*MB

namespace leveldb{
namespace silkstore{


class NvmManager{
 private:
    const char* nvm_file_;
    // Divide a part of the memory for logging 
    // Default value is 30*MB
    size_t logCap_;
    
    size_t index_;
    size_t cap_;
    char *data_;
    std::queue<size_t> memUsage;

    void init();

 public:
    NvmManager();
    NvmManager(const char * nvm_file, size_t size = GB);
    ~NvmManager();
    Nvmem* allocate(size_t cap = 30*MB);
    NvmLog* initLog(size_t cap = 30*MB);
    size_t getBeginAddress();
    void free();
 };

} // namespace silkstore 
} // namespace leveldb


#endif