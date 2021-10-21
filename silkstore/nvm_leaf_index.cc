#include <stdexcept>
#include "nvm_leaf_index.h"
#include "util/coding.h"

namespace leveldb {
namespace silkstore {
	  
const Snapshot* NVMLeafIndex::GetSnapshot() {
	throw std::runtime_error("NVMLeafIndex::GetSnapshot not supported");
}

void NVMLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
	throw std::runtime_error("NVMLeafIndex::ReleaseSnapshot not supported");
}

NVMLeafIndex::~NVMLeafIndex() {

}


// Set the database entry for "key" to "value".  Returns OK on success,
// and a non-OK status on error.
// Note: consider setting options.sync = true.
Status NVMLeafIndex::Put(const WriteOptions& options,
		   				 const Slice& key,
		   				 const Slice& value) {
	// Format of an entry is concatenation of:
	//  key_size     : varint32 of internal_key.size()
	//  key bytes    : char[internal_key.size()]
	//  value_size   : varint32 of value.size()
	//  value bytes  : char[value.size()]
	size_t key_size = key.size();
	size_t val_size = value.size();
	size_t internal_key_size = key_size + 8;
	const size_t encoded_len =
		VarintLength(internal_key_size) + internal_key_size +
		VarintLength(val_size) + val_size;
	char* buf = static_cast<char*>(alloc.allocate(encoded_len));
	char* p = EncodeVarint32(buf, internal_key_size);
	memcpy(p, key.data(), key_size);
	p += key_size;
	EncodeFixed64(p, (s << 8) | type);
	p += 8;
	p = EncodeVarint32(p, val_size);
	memcpy(p, value.data(), val_size);
	assert(p + val_size == buf + encoded_len);
	->allocate()
}

}
}