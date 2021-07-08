

#ifndef SILKSTORE_NVM_COMMON_H
#define SILKSTORE_NVM_COMMON_H

#define KB      (1024)
#define MB	(1024*1024)
#define GB	(1024*1024*1024)

#define CACHE_LINE_SIZE    64
#define PAGE_SIZE          (4*KB)

// obtain the starting address of a cache line
#define getcacheline(addr) \
(((unsigned long long)(addr)) & (~(unsigned long long)(CACHE_LINE_SIZE-1)))

// check if address is aligned at line boundary
#define isaligned_atline(addr) \
(!(((unsigned long long)(addr)) & (unsigned long long)(CACHE_LINE_SIZE-1)))


#include <immintrin.h>


// flush a cache line 64 bits' data
static inline void clwb(void * addr){ 
  asm volatile("clwb %0": :"m"(*((char *)addr))); 
} 

// flush more than one cache line (64 bits') data
static inline void clwbmore(void *start, void *end){ 
  unsigned long long start_line= getcacheline(start);
  unsigned long long end_line= getcacheline(end);
  do {
    clwb((char *)start_line);
    start_line += CACHE_LINE_SIZE;
  } while (start_line <= end_line);
}

static inline void sfence(void){ 
  asm volatile("sfence"); 
}


static inline void nontemporal_store_256(void *mem_addr, void *c){
    __m256i x = _mm256_load_si256((__m256i const *)c);
    _mm256_stream_si256((__m256i *)mem_addr, x);
}




static inline void nontemporal_store_512(void *mem_addr, void *c){
    auto t = _mm512_load_si512((const __m512i *)c);
    _mm512_stream_si512((__m512i *)mem_addr, t);
}

#endif