//
// Created by zxjcarrot on 2019-07-24.
//

#ifndef SILKSTORE_ITER_H
#define SILKSTORE_ITER_H

#include <stdint.h>
#include "leveldb/db.h"
#include "db/dbformat.h"

namespace leveldb {
namespace silkstore {
// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
Iterator* NewDBIterator(const Comparator* user_key_comparator,
                        Iterator* internal_iter,
                        SequenceNumber sequence);

}  // namespace leveldb

}
#endif //SILKSTORE_ITER_H
