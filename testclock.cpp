#include <unistd.h>
#include <sys/syscall.h>
#include "global.h"
//#include "test.h"

ull get_time_ns()
{
	struct timespec tm;
	clock_gettime(CLOCK_MONOTONIC_RAW, &tm);
	return tm.tv_sec * 1000000000ull + tm.tv_nsec;
}

int main()
{
	static const ull tot = 1e8;
	printf("%lld\n",tot);
	
	ull t[8] = {};
	
	ull monotonic_time;
	for(int i = 0; i < tot; i++) {
		//printf("%lld\n",tot);
		t[i&7] = get_time_ns();
	}
	printf("%lld\n",tot);
	double ns = 1e9*clock()/CLOCKS_PER_SEC;
	printf("%lf ns, %lf ns, %lld\n", ns, 1.0*ns/tot, tot);
	for(int i = 0; i < 8; i++) printf("%lld\n", t[i]);
	return 0;
}
