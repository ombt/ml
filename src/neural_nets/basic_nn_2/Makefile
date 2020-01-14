
DEBUG = 
# DEBUG = -DLOCAL_DEBUG=1

INCLUDES = -Ihdr -I../utils/hdr

LIBS_PATHS = -L../utils
LIBS = -lutils -lm

CPP = g++
CPP_OPTS = -g -c

LD = g++
LD_OPTS = -g

all:	base_nn

base_nn:	\
	obj \
	obj/main.o \
	obj/BaseNN.o \
	../utils/libutils.a
	$(LD) $(LD_OPTS) -o base_nn \
		obj/main.o \
		obj/BaseNN.o \
		$(LIBS_PATHS) \
		$(LIBS)

obj:
	mkdir -p obj

obj/main.o:	\
	src/main.C \
	hdr/BaseNN.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/main.o \
		src/main.C

obj/BaseNN.o: \
	src/BaseNN.C \
	hdr/BaseNN.H \
	../utils/hdr/ExtUseCntPtr.H \
	../utils/hdr/Atomic.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/BaseNN.o \
		src/BaseNN.C

clean:
	-(rm -fr obj base_nn);
	
