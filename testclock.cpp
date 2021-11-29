#include "test.h"

int main()
{
	ull t = get_time_ns();
	ull tot = 1e8, n;
	for(int i = 0; i < tot; i++) {
		n = get_time_ns();
	}
	printf("%lld ns, %.2lf ns\n", n-t, 1.0*(n-t)/tot);
	return 0;
}
