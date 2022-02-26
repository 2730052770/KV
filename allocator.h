#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include "global.h"

inline void* mmap_hugepage(size_t size) {
	void *ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | 0x40000 /*HUGEPAGE*/, -1, 0);
	if(unlikely(ret == MAP_FAILED)) {
		puts("mmap failed");
		exit(0);
	}
	return ret;
}

struct Segment {
	Segment *next;
	uint start_offset;
};

struct Free_block {
	uc meta;
	Free_block ** queue_entry;
}__attribute__((packed));

struct Small_allocator {
	char* uninitialized_block_head;
	char* segment_head;
	
	Free_block ** queue_head;
	Free_block ** queue_tail;
	const size_t queue_segment_size;
	
	const size_t block_size;
	const size_t segment_size;
	const int segment_start_offset;
	
	Small_allocator();
	
	Small_allocator(size_t _block_size, size_t _segment_size, int _offset, size_t _queue_segment_size);
	//Allocator(const Allocator & a);
	
	void allocate_segment();
	
	inline void prefetch() {
		void * next_block = queue_head != queue_tail ? (void*)*queue_head : (void*)uninitialized_block_head;
		// *queue_head can be an address of a Free_block or a queue_segment, however we don't care that
		__builtin_prefetch(next_block, 1, 0); 
	}
	
	inline void* allocate() {
		if(unlikely(!segment_head)) allocate_segment();
			
		Free_block * ret;
		Free_block ** nxt_head;
		retry:
		if(queue_head != queue_tail) {
			if(unlikely( ((ull)(queue_head+1) & (queue_segment_size-1)) == 0 )) {
				nxt_head = (Free_block**) *queue_head;
				munmap((void*)((ull)queue_head & ~(queue_segment_size-1)), queue_segment_size);
				queue_head = nxt_head;
			}
			ret = *queue_head;
			queue_head++;
			if(unlikely(ret == NULL))
				goto retry;
		}
		else { 
			ret = (Free_block *)uninitialized_block_head;
			uninitialized_block_head += block_size;
			if(uninitialized_block_head - segment_head + block_size > segment_size) allocate_segment();
		}
		prefetch(); // is this useful?
		ret->meta = 1;
		return ret;
	}
	
	inline void free(void *ptr) {
		if(unlikely(!queue_head)) 
			queue_head = queue_tail = (Free_block **)mmap_hugepage(queue_segment_size);

		Free_block *free_block = (Free_block *)ptr;
		free_block->meta = 0;
		if(unlikely( ((ull)(queue_tail+1) & (queue_segment_size-1)) == 0 )) {
			*queue_tail = (Free_block*) mmap_hugepage(queue_segment_size);
			queue_tail = (Free_block**) *queue_tail;
		}
		free_block->queue_entry = queue_tail;
		*queue_tail = free_block;
		queue_tail ++;
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
