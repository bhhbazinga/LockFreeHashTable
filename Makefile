CXX = g++
CXXFLAGS = -Wall -Wextra -pedantic -std=c++2a -g -o3
#-fsanitize=address -fsanitize=leak
#-fsanitize=thread
EXEC = test

all: $(EXEC)

$(EXEC):  test.cc lockfree_hashtable.h HazardPointer/reclaimer.h
	$(CXX) $(CXXFLAGS) test.cc -o $@ -lpthread 

HazardPointer/reclaimer.h:
	git submodule update --init

clean:
	rm -rf  $(EXEC)

.Phony:
	clean
