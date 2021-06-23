
#include "nvm/nvmmanager.h"

namespace leveldb{
namespace silkstore{



// init nvm memory 
void NvmManager::init(){
    std::cout << "init nvm pool size: "<< size_  / (1024 *1024) << "MB" << std::endl;
    //memory aliganment must be 4096
    assert((size_>0)&&(size_ % 4096 == 0));
    // pmdk allows PMEM_MMAP_HINT=map_addr to set the map address
    int is_pmem = false;
    size_t mapped_len = size_;
    data_ = (char *) pmem_map_file(nvm_file_, size_, PMEM_FILE_CREATE, 
            0666, &mapped_len, &is_pmem);

    if (data_ == NULL) {
       perror ("pmem_map_file");
       exit(1);
    }
    if (size_ != mapped_len) {
       fprintf(stderr, "Error: cannot map %lu bytes\n", size_);
       pmem_unmap(data_, mapped_len);
       exit(1);
    }
}

Nvmem* NvmManager::allocate(size_t size ){
    if (index_ + size >= size_){
        index_ = 0;
        if ( index_ + size > memUsage.front()){
            fprintf(stderr, "NvmManager is out ");
            assert(false);
        }
        fprintf(stdout, "########## $$$$$$$$$$$$$$$$  ###########\n");
        fprintf(stdout, "########## NvmManager Reset  ###########\n");
        fprintf(stdout, "########## $$$$$$$$$$$$$$$$  ###########\n");

    }
    Nvmem* nvm = new Nvmem(data_ + index_, size);
    memUsage.push(index_);
    index_ += size;
   // std::cout << "allocate nvm size: "<< size << std::endl;
    return nvm;
}

size_t NvmManager::getBeginAddress(){
    return index_;
}

void NvmManager::free(){
    memUsage.pop();
}

 NvmManager::NvmManager(const char * nvm_file , size_t size)
        :nvm_file_(nvm_file), size_(size), index_(0){
        init();
 }
 
 NvmManager::~NvmManager(){
        pmem_unmap(data_, size_);
 }
 
} // namespace silkstore 
} // namespace leveldb


