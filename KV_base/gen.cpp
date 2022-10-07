#include<bits/stdc++.h>
using namespace std;
int main()
{
	int x = 32, y = 4, cnt = 0;
	while(x <= 1<<16) {
		printf("%d, ", x);
		x = (int)(x * 1.19)+y-1&~(y-1);
		if(y <= x/8) y*=2; 
		cnt ++;
	}
	cout<< cnt;
	return 0;
}
