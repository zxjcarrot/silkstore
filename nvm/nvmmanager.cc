
#include "nvm/nvmmanager.h"

namespace leveldb{
namespace silkstore{

// init nvm memory 
void NvmManager::init(){
    std::cout << "init nvm pool size: "<< cap_  / (1024 *1024) << "MB" << std::endl;
    //memory aliganment must be 4096
    assert((cap_>0)&&(cap_ % 4096 == 0));
    // pmdk allows PMEM_MMAP_HINT=map_addr to set the map address
    int is_pmem = false;
    size_t mapped_len = cap_;
    data_ = (char *) pmem_map_file(nvm_file_, cap_, PMEM_FILE_CREATE, 
            0666, &mapped_len, &is_pmem);
    if (data_ == NULL) {
       perror ("pmem_map_file");
       exit(1);
    }
    if (cap_ != mapped_len) {
       fprintf(stderr, "Error: cannot map %lu bytes\n", cap_);
       pmem_unmap(data_, mapped_len);
       exit(1);
    }
}



Nvmem* NvmManager::allocate(size_t size){
    std::lock_guard<std::mutex> lk(mtx);
    //mutx.lock();
    if (index_ + size >= cap_){
        index_ = logCap_;
        if ( index_ + size > memUsage.front().first){
            fprintf(stderr, "NvmManager is out can't allocate nvmem \n");
            assert(false);
        }
        fprintf(stdout, "########## $$$$$$$$$$$$$$$$  ###########\n");
        fprintf(stdout, "########## NvmManager Reset  ###########\n");
        fprintf(stdout, "########## $$$$$$$$$$$$$$$$  ###########\n");
    }
    Nvmem* nvm = new Nvmem(data_ + index_, size, this);
    memUsage.emplace_back(index_,size);
/*
    std::cout << "memUsage.allocate: " <<  index_ <<" ";
    std::cout << " memUsage size: " << memUsage.size() << " data: ";
    for (auto it = memUsage.begin(); it != memUsage.end(); it++){
        std::cout << " " <<  it->first <<" ";
    }
    std::cout<< "\n";
*/
    index_ += size;
    //mutx.unlock();    
    return nvm;
}

/* 
NvmLog* NvmManager::initLog(size_t size){
    logCap_ = size;
    index_ = logCap_;
    if (logCap_ >= cap_){
        fprintf(stderr, "NvmManager is out can't init NvmLog \n");
        assert(false);
    }
    NvmLog* nvmLog = new NvmLog(data_, size);
    return nvmLog;
} */

std::string NvmManager::getNvmInfo(){
    std::string info;
    for(int i = 0; i < memUsage.size(); i++){
       // printf(" %ld ", memUsage[i].first );

       // info += std::to_string(memUsage[i].first) + ",";
       // info += std::to_string(memUsage[i].second) + ",";
    }
    return info;
}

void NvmManager::free(char * address){
    //mutx.lock();
    std::lock_guard<std::mutex> lk(mtx);
    if (memUsage.empty()){
        printf("memUsage is empty can free memory\n");
        assert(false);
       // mutx.unlock();    
        return ;
    }
    bool suc = false;
    for (auto it = memUsage.begin(); it != memUsage.end(); it++){
        if (data_ + it->first == address){
           // std::cout << "memUsage.erase: " <<  it->first <<" ";            
            memUsage.erase(it);
            suc = true;
            break;
        }
    }
    /*
    std::cout << " memUsage size: " << memUsage.size() << " data: ";
    for (auto it = memUsage.begin(); it != memUsage.end(); it++){
        std::cout << " " <<  it->first <<" ";
    }
    std::cout<< "\n";
    */
    
    if (!suc)
        printf("########### can't free memory error ##########\n");
    assert(suc);
   // mutx.unlock();
    
}

 NvmManager::NvmManager(const char * nvm_file , size_t cap)
        :nvm_file_(nvm_file), cap_(cap), index_(LOGCAP), logCap_(LOGCAP){
        init();
 }
 
 NvmManager::~NvmManager(){
        pmem_unmap(data_, cap_);
 }
 
} // namespace silkstore 
} // namespace leveldb


