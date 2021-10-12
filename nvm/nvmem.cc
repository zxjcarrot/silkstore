#include"nvm/nvmem.h"
#include <iostream>

namespace leveldb{

namespace silkstore{

// insert date into nvm
uint64_t Nvmem::insert(const char* value, int len){
    // Not enough memory assert
    if(index_ + len >= size_){
       fprintf(stderr, " nvm memory is full!  \
            index %lu , len: %d , size: %lu \n ",index_ , len, size_);
        assert(false);
    } else{
        // then, insert data and flush to nvm 
        memcpy(data_+ index_ , value, len);
        clwbmore(data_ + index_, data_ + index_ + len);
        // ntstoremore(data_ + index_, data_ + index_ + len + 4);
        sfence(); 
        // return data's address on nvm
        int resIndex = index_;
        index_ = index_ + len;
        return u_int64_t(resIndex + data_);
    } 
}

// update date into nvm
void Nvmem::update(uint64_t add ,char* value, int len){
    // then, insert data and flush to nvm 
    memcpy((void *)add , value, len);
    clwbmore(data_ + index_, data_ + index_ + len);
    sfence(); 
}



bool Nvmem::UpdateCounter(size_t counters){
    memcpy(data_ , &counters, 8);
    clwb(data_);
    sfence(); 
    return true;
}

size_t Nvmem::GetCounter(){
    size_t counter = 0;
    memcpy( &counter, data_ ,8);
    return counter;
}


void Nvmem::print(){
    printf("nvm's information index_ %lu, size_ %lu, data add %lu \n", index_ , size_, (size_t) data_);
}

Nvmem::Nvmem():data_(nullptr), index_(16), size_(0){}

Nvmem::Nvmem(char *data, size_t size)
    :data_(data), index_(16), size_(size){} 

Nvmem::~Nvmem(){
}


} // namespace silkstore
} // namespace leveldb