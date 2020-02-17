CXX = g++
CXXFLAGS = -Wall -Wextra -pedantic -std=c++2a -g -o3 -fsanitize=thread
#-fsanitize=address -fsanitize=leak
#-fsanitize=thread
EXEC = test

all: $(EXEC)

$(EXEC):  test.cc lockfree_hashtable.h
	$(CXX) $(CXXFLAGS) test.cc -o $@  

clean:
	rm -rf  $(EXEC)