#include "nvm/nvmlog.h"



namespace leveldb{

namespace silkstore{

    NvmLog::NvmLog(char *data, size_t cap):data_(data),counter_(0), index_(0), cap_(cap){}

    void NvmLog::append(int64_t addr){
        index_ = index_ + 8;
        memcpy(&addr, (data_ + index_), 8);
        clwb(data_ + index_);
        counter_++;
        memcpy(&counter_, data_ , 8);
        clwb(data_);
        sfence();             
    }
    void NvmLog::reset(){
        counter_ = 0;
        index_ = 0;
        memcpy(&counter_, data_ , 8);
        clwb(data_);
        sfence();    
    }
}
}