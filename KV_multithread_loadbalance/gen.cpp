#include<bits/stdc++.h>
using namespace std;
int seg[64], ps = 2<<20;
int cal_seg(int x) {
	for(int i = 1; i <= 8; i++){
		int tot = ps * i;
		int c = tot / x;
		int v = c * x;
		if(1.0*v/tot > 0.8) return i;
	}
	return -1;
}
int main()
{
	int x = 16, y = 4, cnt = 0;
	while(x <= 8<<20) {
		seg[cnt] = cal_seg(x);
		printf("%d, ", x, seg[cnt]);
		x = (int)(x * 1.19)+y-1&~(y-1);
		if(y <= x/8) y*=2; 
		cnt ++;
	}
	puts("");
	for(int i = 0; i < cnt; i++){
		printf("%d, ", seg[i]);
	}
	puts("");
	cout<<cnt<<endl;
	return 0;
}
