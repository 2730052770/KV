all: tester main

tester: tester.cpp main.h test.h
	g++ -o tester tester.cpp -O3 -Wfatal-errors -Wall -Wextra -Wshadow

main: kv.cpp allocator.cpp main.h test.h
	g++ -o main kv.cpp allocator.cpp  -O3 -Wfatal-errors -Wall -Wextra -Wshadow

.PHONY: clean	
clean:
	rm tester main
