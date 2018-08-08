CC = $(TOOLPREFIX)g++
AS = $(TOOLPREFIX)gas
LD = $(TOOLPREFIX)ld
OBJCOPY = $(TOOLPREFIX)objcopy
OBJDUMP = $(TOOLPREFIX)objdump

CFLAGS :=-std=c++11 -Wall -g -D_GNU_SOURCE -pthread -lm -fno-pic -O2 
CFLAGS +=-fsanitize=address
#CFLAGS +=-DVERBOSE


INCLUDES := -I. 

SRCS = 	db.cpp \
	bcache.cpp \
	db-tests.cpp \
	env.cpp \
	main.cpp\

TEST_SRCS = \
	test.cpp\


OBJS = $(SRCS:.c=.o)
TEST_OBJS = $(TEST_SRCS:.c=.o)

TESTS= tests
MAIN = db 


.PHONY: depend clean

all:    $(MAIN) $(TESTS)
#all:    $(TESTS)

$(TESTS): $(TEST_OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TESTS) $(TEST_OBJS) $(LFLAGS) $(LIBS)


$(MAIN): $(OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -o $(MAIN) $(OBJS) $(LFLAGS) $(LIBS)

.cpp.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	rm -f *.o *~ $(MAIN) $(TESTS)

depend: $(SRCS) $(TEST_SRCS)
	makedepend $(INCLUDES) $^


