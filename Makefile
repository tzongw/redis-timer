
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

ifeq ($(DEBUG), 1)
	DEBUG_FLAGS ?= -g -ggdb
endif

CFLAGS = -Wall -fPIC -std=gnu99
CC=gcc

all: timer.so

timer.so: timer.o
	$(LD) -o $@ $< $(SHOBJ_LDFLAGS) $(LIBS)

.c.o:
	$(CC) -I. $(CFLAGS) $(DEBUG_FLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

clean:
	rm -rf *.so *.o

FORCE:
