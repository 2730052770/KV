#include "main.h"

Allocator::Allocator(size_t _size, void* _base, size_t _pgsz)
:base((char*)_base), obj_size(_size), page_size(_pgsz), reserve_num (8*K/obj_size), batch_free_num(4*K/obj_size)
{
	head = 0;
	obj_tail = base;
	page_tail = base;
	page_num = 0;
	obj_num = 0;
	free_obj_num = 0;
	assert(page_size/obj_size >= 4); 
	assert(_pgsz == 2*M || _pgsz == 4*K);
}

// I think every mmap allocate 1 page is OK, may not need to batch several pages in mmap
char* Allocator::allocate_page() { 
	void *buf;
	buf = mmap(page_tail, page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | 0x40000 /*HUGEPAGE*/, -1, 0);
	if(buf == MAP_FAILED){
	    	puts("map failed");
	    	exit(1);
   	}
   	page_tail += page_size;
   	page_num ++;
   	char *ret = obj_tail;
   	while(obj_tail <= page_tail - obj_size) {
   		OBJ_NEXT(obj_tail) = obj_tail + obj_size;
   		BLOCK_ID(obj_tail) = obj_num;
   		obj_tail += obj_size;
   		obj_num ++;
   		free_obj_num ++;
	}
	OBJ_NEXT(obj_tail - obj_size) = NULL;
	return ret;
}
void Allocator::shrink() {
	//ÏÈ°ÚÀÃ 
}
