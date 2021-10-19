#ifndef NVMMANAGER
#define NVMMANAGER
 
#include "nvm/nvmem.h"
#include <iostream>
#include <deque>
#include <mutex> 

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
    std::deque<std::pair<size_t,size_t>> memUsage;
    std::mutex mtx;
    void init();

 public:
    NvmManager();
    NvmManager(const char * nvm_file, size_t size = GB);
    ~NvmManager();
    Nvmem* allocate(size_t cap = 30*MB);
    std::string getNvmInfo();
    void free(char * address);
 };

} // namespace silkstore 
} // namespace leveldb


#endif