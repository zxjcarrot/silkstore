//
// Created by zxjcarrot on 2019-07-18.
//

#ifndef SILKSTORE_UTIL_H
#define SILKSTORE_UTIL_H

#include <functional>

namespace leveldb {
namespace silkstore {

class DeferCode {
public:
    DeferCode(std::function<void()> code): code(code) {}
    ~DeferCode() { code(); }
private:
    std::function<void()> code;
};

}  // namespace silkstore
}  // namespace leveldb

#endif // SILKSTORE_UTIL_H
