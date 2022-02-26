#include "allocator.h"

Small_allocator::Small_allocator() 
:queue_segment_size(0), block_size(0), segment_size(0), segment_start_offset(0)
{}

Small_allocator::Small_allocator(size_t _block_size, size_t _segment_size, int _offset, size_t _queue_segment_size)
:uninitialized_block_head(NULL), segment_head(NULL), queue_head(NULL), queue_tail(NULL), queue_segment_size(_queue_segment_size), 
block_size(_block_size), segment_size(_segment_size), segment_start_offset(_offset)
{
	//printf("block size = %d, segment_size = %d, header_offset = %d, utilization = %.1lf%%\n", _block_size, _segment_size, _header_offset, 100.0*((_segment_size-_header_offset)/_block_size*_block_size) / _segment_size );
	assert(sizeof(Segment) + block_size <= segment_size);
	assert((int)sizeof(Segment) <= segment_start_offset);
	assert(sizeof(Free_block) <= block_size);
	assert(segment_size % PAGE_SIZE == 0);
}

// I think every mmap allocate 1 page is OK, may not need to batch several pages in mmap
void Small_allocator::allocate_segment() 
{ 
	char *old_segment_head = segment_head;
	segment_head = (char*)mmap_hugepage(segment_size);
   	((Segment*)segment_head) -> next = (Segment*)old_segment_head;
   	((Segment*)segment_head) -> start_offset = segment_start_offset;
   	uninitialized_block_head = segment_head + segment_start_offset;
}

void Small_allocator::shrink() 
{
	//œ»∞⁄¿√ 
}

Huge_allocator::Huge_allocator()
: segment_start_offset(0)
{
}

Huge_allocator::Huge_allocator(int _offset)
: segment_start_offset(_offset)
{
	//printf("%d %d\n", (int)segment_header_size, segment_start_offset);
	assert((int)sizeof(Huge_mem) <= segment_start_offset);
	head.nxt = &tail;
	head.pre = NULL;
	head.lock_nxt = 0;
	tail.nxt = NULL;
	tail.pre = &head;
	tail.lock_nxt = 0;
}

void * Huge_allocator::allocate(size_t size){
	// this function will not be called when another thread is using this allocator, so we don't call acquire()
	
	size = (size+segment_start_offset+PAGE_SIZE-1) & ~(PAGE_SIZE-1);
	Huge_mem *buf = (Huge_mem*)mmap_hugepage(size);
   	buf->size = size;
   	buf->nxt = head.nxt;
   	buf->pre = &head;
   	buf->lock_nxt = 0;
   	buf->nxt->pre = buf;
   	head.nxt = buf;
   	((uc*)buf)[segment_start_offset] = 1;
   	return (uc*)buf + segment_start_offset;
}

void Huge_allocator::free(void *ptr){
	Huge_mem *buf = (Huge_mem*)((char *)ptr - segment_start_offset);
	buf->pre->acquire();
	buf->acquire();
	buf->pre->nxt = buf->nxt;
	buf->nxt->pre = buf->pre;
	if(unlikely(munmap(buf, buf->size) != 0)) {
		puts("unmap failed");
	    	exit(1);
	}
	buf->pre->release();
}

const uint Allocator:: block_size[] = {	
	0, 0, 0, 0, 0, 16, 20, 24, 
	28, 36, 48, 64, 80, 96, 128, 160, 
	192, 256, 320, 384, 512, 640, 768, 1024, 
	1280, 1536, 2048, 2560, 3072, 4096, 5120, 6144, 
	8192, 10240, 12288, 16384, 20480, 24576, 32768, 40960, 
	49152, 65536, 81920, 98304, 131072, 163840, 196608, 262144, 
	327680, 393216, 524288, 655360, 786432, 1048576, 1310720, 1572864, 
	2097152, 2621440, 3145728, 4194304, 5242880, 6291456, 8388608, (uint)-1
};

Allocator::Allocator()
{
}

Allocator::Allocator(int _offset) 
{
	assert((n_category & (n_category-1)) == 0);// power of 2
	
	new(&ha) Huge_allocator(sizeof(Huge_mem));
		
	for(uint i = 0; i < n_category; i++) {
		if(!block_size[i]) continue;
		if(block_size[i] == (uint)-1) continue;
		int segment_size = 0, block_size_decrease = 0;
		for(int num = 1; num <= 8; num++){
			int tot = PAGE_SIZE * num;
			int cnt = tot / block_size[i];
			int v = cnt * block_size[i];
			if(1.0*v/tot > 0.8) {
				segment_size = tot;
				int cnt2 = (tot - _offset) / block_size[i];
				int v2 = cnt2 * block_size[i];
				if(1.0*v2/v < 0.981) block_size_decrease = _offset;
				break;
			}
		}
		assert(segment_size);
		new(sa + i) Small_allocator(block_size[i] - block_size_decrease, segment_size, _offset, PAGE_SIZE);
		/*
			segment_size[] = {
			0, 0, 0, 0, 0, 0, 1, 1, 
			1, 1, 1, 1, 1, 1, 1, 1, 
			1, 1, 1, 1, 1, 1, 1, 1, 
			1, 1, 1, 1, 1, 1, 1, 1, 
			1, 1, 1, 1, 1, 1, 1, 1, 
			1, 1, 1, 1, 1, 1, 1, 1, 
			1, 1, 1, 1, 1, 2, 1, 2, 
			3, 1, 3, 3, 2, 3, 3, 4
		};
		*/
	}
}

void Allocator::shrink() 
{
	//
}

Allocator_pair::Allocator_pair()
{
	id_inuse = 0;
	new(&a[0]) Allocator(64);
	new(&a[1]) Allocator(64);
}



