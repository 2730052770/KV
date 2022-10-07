LIST=localtest_server test1 test2 test3 selftest mttest
WARNINGS=-Wall -Wextra -Wshadow #-Wfatal-errors -Wno-unused

all: $(LIST)

TEST_CLIENT_H=main.h global.h test.h kv.h allocator.h kvp.h
TEST_CLIENT_CPP=testutils.cpp
DEBUG_OR_OPT=-O3
LIBS=-lpthread #-mavx2 -mavx512f -mavx512vl -mavx512pf

test%: test%.cpp $(TEST_CLIENT_H) $(TEST_CLIENT_CPP)
	g++ -o $@ $< $(TEST_CLIENT_CPP) $(LIBS) $(DEBUG_OR_OPT) $(WARNINGS)

TEST_SERVER_H=allocator.h main.h global.h test.h kv.h kvp.h
TEST_SERVER_CPP=kv.cpp allocator.cpp testutils.cpp  # log.cpp

%: %.cpp $(TEST_SERVER_H) $(TEST_SERVER_CPP)
	g++ -o $@ $< $(TEST_SERVER_CPP) $(LIBS) $(DEBUG_OR_OPT) $(WARNINGS)
	
.PHONY: clean	
clean:
	rm $(LIST)
