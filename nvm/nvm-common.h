

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

/* 
// Non-temporal stores:  write a cache line 64 bits' data bypass CPU Cache
static inline void ntstore(void * addr){ 
  asm volatile("ntstore %0": :"m"(*((char *)addr))); 
} 

// Non-temporal stores:  write more than one cache line (64 bits') data bypass CPU Cache
static inline void ntstoremore(void *start, void *end){ 
  unsigned long long start_line= getcacheline(start);
  unsigned long long end_line= getcacheline(end);
  do {
    ntstore((char *)start_line);
    start_line += CACHE_LINE_SIZE;
  } while (start_line <= end_line);
}
 */
// call sfence
static inline void sfence(void){ 
  asm volatile("sfence"); 
}

#endif