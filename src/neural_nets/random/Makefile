
DEBUG = 

INCLUDES = -Ihdr

CPP = g++
CPP_OPTS = -g -c

AR = ar
AR_OPTS = rv

RM = rm
RM_OPTS = -f

TARGET = librandom.a

all:	$(TARGET)

$(TARGET):	\
	obj \
	obj/Random.o
	$(RM) $(RM_OPTS) $(TARGET)
	$(AR) $(AR_OPTS) $(TARGET) \
		obj/Random.o

obj:
	mkdir -p obj

obj/Random.o: \
	src/Random.C \
	hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Random.o \
		src/Random.C

clean:
	-(rm -fr obj $(TARGET) );

