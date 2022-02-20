#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include "global.h"

struct Segment {
	Segment *next;
	uint id;
};

struct Free_block {
	Free_block *next;
};

struct Small_allocator {
	char* free_block_head;
	char* uninitialized_block_head;
	char* segment_head;
	uint segment_id;
	const size_t block_size;
	const size_t segment_size;
	const size_t header_offset;
	
	Small_allocator();
	
	Small_allocator(size_t _block_size, size_t _segment_size, size_t _header_offset);
	//Allocator(const Allocator & a);
	
	void allocate_segment();
	
	inline void prefetch() {
		void * next_block = free_block_head ? free_block_head : uninitialized_block_head;
		__builtin_prefetch(next_block, 1, 0); 
	}
	
	inline void* allocate() {
		if(unlikely(!segment_head)) allocate_segment();
		void * ret;
		if(free_block_head) {
			ret = free_block_head;
			free_block_head = (char *) ((Free_block*)free_block_head) -> next;
		}
		else { 
			ret = uninitialized_block_head;
			uninitialized_block_head += block_size;
			if(uninitialized_block_head - segment_head + block_size > segment_size) allocate_segment();
		}
		prefetch(); // is this useful?
		return ret;
	}
	
	inline void free(void *ptr) {
		char * old_free_block_head = free_block_head;
		free_block_head = (char*)ptr;
		((Free_block*) free_block_head) -> next = (Free_block*)old_free_block_head;
	}
	
	void shrink();//回收时先把空闲链表按照空闲块所在segment的标号sort一波，然后按标号倒序扫每个segment的非空闲块（用version判断非空闲）
};

struct Huge_allocator{
	inline void * allocate(size_t size){
		size = (size+sizeof(size_t)+PAGE_SIZE-1) & ~(PAGE_SIZE-1);
		size_t *buf = (size_t*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | 0x40000 /*HUGEPAGE*/, -1, 0);
		if(unlikely(buf == MAP_FAILED)){
		    	puts("map failed");
		    	exit(1);
	   	}
	   	*buf = size;
	   	return buf + 1;
	}
	
	inline void free(void *ptr){
		size_t *buf = (size_t *)ptr - 1;
		if(unlikely(munmap(buf, *buf) != 0)) {
			puts("unmap failed");
		    	exit(1);
		}
	}
};

struct Allocator{
	static constexpr uint n_category = 64;
	static constexpr uint segment_header_offset = 64 + 8 - sizeof(uc);
	static const uint block_size[n_category];
	
	Small_allocator sa[n_category];
	Huge_allocator ha;
	
	Allocator(); 
	
	inline void * allocate(size_t size) { // assert size > 0
		size += sizeof(uc);
		uc pos = 0;
		for(uc jmp = n_category>>1; jmp; jmp >>= 1) {
			uc nxt = pos + jmp;
			if(size > block_size[nxt])
				pos = nxt;
		}
		pos ++;
		uc *buf;
		if(unlikely(pos == n_category)) buf = (uc*)ha.allocate(size);
		else buf = (uc*)sa[pos].allocate();
		*buf = pos;
		return buf + 1;
	}
	
	inline void free(void *ptr) {
		uc *buf = (uc*)ptr - 1;
		if(unlikely(*buf == n_category)) ha.free(buf);
		else sa[*buf].free(buf);
	}
	
	void shrink();
};

	
#endif
