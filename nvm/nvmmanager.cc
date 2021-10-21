
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

Nvmem* NvmManager::reallocate(size_t offset, size_t cap){
    std::lock_guard<std::mutex> lk(mtx);
    return new Nvmem(data_ + offset, cap, this);
}

Nvmem* NvmManager::allocate(size_t size){
    std::lock_guard<std::mutex> lk(mtx);
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
    index_ += size;
    return nvm;
}

std::string NvmManager::getNvmInfo(){
    std::string info;
    info += std::to_string(index_) + ",";
    for(int i = 0; i < memUsage.size(); i++){
        info += std::to_string(memUsage[i].first) + ",";
        info += std::to_string(memUsage[i].second) + ",";
    }
    return info;
}



bool NvmManager::recovery(const std::vector<size_t> &records){
    // recover data
    index_ = records[0];
    memUsage.clear();
    for (int i = 1; i < records.size(); i += 2) {
         memUsage.emplace_back(records[i],records[i+1]);
    }
    return true;
    // print test code
/*  
    std::cout << "recovery inedx:" << index_ << " "; 
    std::cout << " memUsage: ";
    for(int i = 0; i < memUsage.size(); i++){
        std::cout << memUsage[i].first << " " << memUsage[i].second<<" ";
    }
    std::cout << "\n"; 
*/
}

void NvmManager::free(char * address){
    std::lock_guard<std::mutex> lk(mtx);
    if (memUsage.empty()){
        printf("memUsage is empty can free memory\n");
        assert(false);
        return ;
    }
    bool suc = false;
    for (auto it = memUsage.begin(); it != memUsage.end(); it++){
        if (address - data_ == it->first){
            memUsage.erase(it);
            suc = true;
            break;
        }
    }
    if (!suc){
        printf("########### can't free memory error ##########\n");
      /*   
        std::cout<<  "memUsage. size " << memUsage.size() << "\n";
        for (auto it = memUsage.begin(); it != memUsage.end(); it++){
             std::cout <<  it->first <<" ";           
        }
        std::cout<<" free address : " << size_t(address - data_) << "\n"; 
    */
    }
    assert(suc);    
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


