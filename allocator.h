#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include "global.h"

struct Segment {
	Segment *next;
	uint id;
	uint start_offset;
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
	const int segment_start_offset;
	
	Small_allocator();
	
	Small_allocator(size_t _block_size, size_t _segment_size, int _offset);
	//Allocator(const Allocator & a);
	
	void allocate_segment();
	
	inline void prefetch() {
		void * next_block = free_block_head ? free_block_head : uninitialized_block_head;
		__builtin_prefetch(next_block, 1, 0); 
	}
	
	inline void* allocate() {
		if(unlikely(!segment_head)) allocate_segment();
		char * ret;
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
		*ret = 1;
		return ret;
	}
	
	inline void free(void *ptr) {
		*(uc*)ptr = 0;
	
		char * old_free_block_head = free_block_head;
		free_block_head = (char*)ptr;
		((Free_block*) free_block_head) -> next = (Free_block*)old_free_block_head;
	}
	
	void shrink();//回收时先把空闲链表按照空闲块所在segment的标号sort一波，然后按标号倒序扫每个segment的非空闲块（用version判断非空闲）
};

inline void fence()
{
	__sync_synchronize();
}

struct Huge_mem{//做checkpoint时必须把batch队列清空
	volatile size_t size;
	Huge_mem * volatile nxt;
	Huge_mem * volatile pre;
	volatile int lock_nxt;
	inline void acquire() {
		while(!__sync_bool_compare_and_swap(&lock_nxt, 0, 1));
		fence();
	}
	inline void release() {
		fence();
		lock_nxt = 0;
	}
};

struct Huge_allocator{
	// one thread modify, another one only read
	Huge_mem head, tail;
	const int segment_start_offset;
	
	Huge_allocator();
	
	Huge_allocator(int _offset);
	
	void * allocate(size_t size);
	
	void free(void *ptr);
};

struct Allocator{
	static constexpr uint n_category = 64;
	static constexpr uint n_mask_bit = 31-__builtin_clz(n_category);
	static const uint block_size[n_category];
	
	Small_allocator sa[n_category];
	Huge_allocator ha;
	
	Allocator();
	
	Allocator(int _offset); 
	
	inline void * allocate(size_t size) { // assert size > 0
		uc pos = 0;
		for(uc jmp = n_category>>1; jmp; jmp >>= 1) {
			uc nxt = pos + jmp;
			if(size > block_size[nxt])
				pos = nxt;
		}
		pos ++;
		uc *buf;
		if(unlikely(pos == n_category-1)) buf = (uc*)ha.allocate(size);
		else buf = (uc*)sa[pos].allocate();
		*buf |= pos << 1;
		return buf;
	}
	
	inline void free(void *ptr) {
		uc *buf = (uc*)ptr;
		uc category = (*buf>>1) & (n_category-1);
		if(unlikely(category == n_category-1)) ha.free(buf);
		else sa[category].free(buf);
	}
	
	void shrink();
};

struct Allocator_pair{
	Allocator a[2];
	uint id_inuse;
	
	Allocator_pair();
	
	inline void * allocate(size_t size) {
		uc *buf = (uc*)a[id_inuse].allocate(size);
		*buf |= id_inuse<<1<<Allocator::n_mask_bit;
		return buf;
	}
	inline void free(void *ptr) {
		uc *buf = (uc*) ptr;
		uc id = *buf>>Allocator::n_mask_bit>>1;
		a[id].free(buf);
	}
	inline void exchange() {
		id_inuse ^= 1;
	}
};
	
#endif
