#include "allocator.h"

void test_all()
{
	Allocator_pair a;
	for(int i = 1; i <= 10; i++){
		void *ptr = a.allocate(17);
		printf("%llx\n", ptr);
		assert((ull)ptr % 4 == 0);
	}
	puts("pass test_all");
}


void test_small()
{
	const int size = 35, segsize = 2<<20;
	Small_allocator sa(size, segsize, 64);
	const int iter = 1000000;
	int free_cnt = 0;
	int in_use = 0;
	char ** buf = new char * [iter];
	for(int i = 0; i < iter; i++) {
		buf[i] = (char *)sa.allocate();
		in_use ++;
		if(free_cnt) {
			for(int p = sizeof(Free_block); p < size; p++)
				if(buf[i][p] != buf[i][size-1])
					puts("error");
			free_cnt --;
			//puts("checked");
		}
		memset(buf[i], (char)i, size);
		if(rand()&1) {
			int pos = rand() % (i+1);
			if(buf[pos]) in_use--, sa.free(buf[pos]), buf[pos] = NULL, free_cnt ++;
		}
	}
	printf("%d %.2lf\n", sa.segment_id, 1.0 * in_use * size / segsize);
}

int main()
{
	srand(time(0));
	
	test_small();
	
	test_all();
	
	return 0;
}
