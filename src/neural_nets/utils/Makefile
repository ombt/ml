
DEBUG = 

INCLUDES = -Ihdr

CPP = g++
CPP_OPTS = -g -c

AR = ar
AR_OPTS = rv

RM = rm
RM_OPTS = -f

TARGET = libutils.a

all:	$(TARGET)

$(TARGET):	\
	obj \
	obj/StdStringUtils.o \
	obj/RegexMatch.o
	$(RM) $(RM_OPTS) $(TARGET)
	$(AR) $(AR_OPTS) $(TARGET) \
		obj/StdStringUtils.o \
		obj/RegexMatch.o

obj:
	mkdir -p obj

obj/StdStringUtils.o: \
	src/StdStringUtils.C \
	hdr/StdStringUtils.H \
	hdr/StdStringUtils.I
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/StdStringUtils.o \
		src/StdStringUtils.C

obj/RegexMatch.o: \
	src/RegexMatch.C \
	hdr/RegexMatch.H 
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/RegexMatch.o \
		src/RegexMatch.C

clean:
	-(rm -fr obj $(TARGET) );

