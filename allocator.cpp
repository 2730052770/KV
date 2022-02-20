#include "allocator.h"

Small_allocator::Small_allocator() 
:block_size(0), segment_size(0), header_offset(0)
{}

Small_allocator::Small_allocator(size_t _block_size, size_t _segment_size, size_t _header_offset)
:free_block_head(NULL), uninitialized_block_head(NULL), segment_head(NULL), segment_id(0), block_size(_block_size), segment_size(_segment_size), header_offset(_header_offset)
{
	//printf("block size = %d, segment_size = %d, header_offset = %d, utilization = %.1lf%%\n", _block_size, _segment_size, _header_offset, 100.0*((_segment_size-_header_offset)/_block_size*_block_size) / _segment_size );
	assert(sizeof(Segment) + block_size <= segment_size);
	assert(sizeof(Free_block) <= block_size);
	assert(segment_size % PAGE_SIZE == 0);
}

// I think every mmap allocate 1 page is OK, may not need to batch several pages in mmap
void Small_allocator::allocate_segment() 
{ 
	char *old_segment_head = segment_head;
	segment_head = (char*)mmap(NULL, segment_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | 0x40000 /*HUGEPAGE*/, -1, 0);
	if(unlikely(segment_head == MAP_FAILED)){
	    	puts("allocate_segment: map failed");
	    	exit(1);
   	}
   	((Segment*)segment_head) -> next = (Segment*)old_segment_head;
   	((Segment*)segment_head) -> id = ++segment_id;
   	uninitialized_block_head = segment_head + header_offset;
}

void Small_allocator::shrink() 
{
	//œ»∞⁄¿√ 
}

const uint Allocator:: block_size[] = {	
	0, 0, 0, 0, 0, 0, 16, 20, 
	24, 28, 36, 48, 64, 80, 96, 128, 
	160, 192, 256, 320, 384, 512, 640, 768, 
	1024, 1280, 1536, 2048, 2560, 3072, 4096, 5120, 
	6144, 8192, 10240, 12288, 16384, 20480, 24576, 32768, 
	40960, 49152, 65536, 81920, 98304, 131072, 163840, 196608, 
	262144, 327680, 393216, 524288, 655360, 786432, 1048576, 1310720, 
	1572864, 2097152, 2621440, 3145728, 4194304, 5242880, 6291456, 8388608,
};

Allocator::Allocator() 
{
	assert((n_category & (n_category-1)) == 0);// power of 2
	for(uint i = 0; i < n_category; i++) {
		if(!block_size[i]) continue;
		int segment_size = 0, block_size_decrease = 0;
		for(int num = 1; num <= 8; num++){
			int tot = PAGE_SIZE * num;
			int cnt = tot / block_size[i];
			int v = cnt * block_size[i];
			if(1.0*v/tot > 0.8) {
				segment_size = tot;
				int cnt2 = (tot - segment_header_offset) / block_size[i];
				int v2 = cnt2 * block_size[i];
				if(1.0*v2/v < 0.981) block_size_decrease = segment_header_offset;
				break;
			}
		}
		assert(segment_size);
		new(sa + i) Small_allocator(block_size[i] - block_size_decrease, segment_size, segment_header_offset);
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





