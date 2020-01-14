
DEBUG = 

INCLUDES = -Ihdr -I../utils/hdr -I../random/hdr

CPP = g++
CPP_OPTS = -g -c

AR = ar
AR_OPTS = rv

RM = rm
RM_OPTS = -f

TARGET = libdistribution.a

all:	$(TARGET)

$(TARGET):	\
	obj \
	obj/Bernoulli.o \
	obj/Beta.o \
	obj/Binomial.o \
	obj/DiscreteUniform.o \
	obj/Exponential.o \
	obj/Gamma.o \
	obj/Gaussian.o \
	obj/Geometric.o \
	obj/LogNormal.o \
	obj/mErlang.o \
	obj/NaiveArbitraryDiscrete.o \
	obj/NegativeBinomial.o \
	obj/PearsonTypeV.o \
	obj/PearsonTypeVI.o \
	obj/Poisson.o \
	obj/QPlaceArbitraryDiscrete.o \
	obj/Uniform.o \
	obj/Weibull.o
	$(RM) $(RM_OPTS) $(TARGET)
	$(AR) $(AR_OPTS) $(TARGET) \
		obj/Bernoulli.o \
		obj/Beta.o \
		obj/Binomial.o \
		obj/DiscreteUniform.o \
		obj/Exponential.o \
		obj/Gamma.o \
		obj/Gaussian.o \
		obj/Geometric.o \
		obj/LogNormal.o \
		obj/mErlang.o \
		obj/NaiveArbitraryDiscrete.o \
		obj/NegativeBinomial.o \
		obj/PearsonTypeV.o \
		obj/PearsonTypeVI.o \
		obj/Poisson.o \
		obj/QPlaceArbitraryDiscrete.o \
		obj/Uniform.o \
		obj/Weibull.o

obj:
	mkdir -p obj

obj/Bernoulli.o:	\
	src/Bernoulli.C \
	hdr/Bernoulli.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Bernoulli.o \
		src/Bernoulli.C

obj/Beta.o:	\
	src/Beta.C \
	hdr/Beta.H \
	hdr/Gamma.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../utils/hdr/ExtUseCntPtr.H \
	../random/hdr/Random.H 
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Beta.o \
		src/Beta.C

obj/Binomial.o:	\
	src/Binomial.C \
	hdr/Binomial.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Binomial.o \
		src/Binomial.C

obj/DiscreteUniform.o:	\
	src/DiscreteUniform.C \
	hdr/DiscreteUniform.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/DiscreteUniform.o \
		src/DiscreteUniform.C

obj/Exponential.o:	\
	src/Exponential.C \
	hdr/Exponential.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Exponential.o \
		src/Exponential.C

obj/Gamma.o:	\
	src/Gamma.C \
	hdr/Gamma.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Gamma.o \
		src/Gamma.C

obj/Gaussian.o:	\
	src/Gaussian.C \
	hdr/Gaussian.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Gaussian.o \
		src/Gaussian.C

obj/Geometric.o:	\
	src/Geometric.C \
	hdr/Geometric.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Geometric.o \
		src/Geometric.C

obj/LogNormal.o:	\
	src/LogNormal.C \
	hdr/LogNormal.H \
	hdr/Gaussian.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../utils/hdr/ExtUseCntPtr.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/LogNormal.o \
		src/LogNormal.C

obj/mErlang.o:	\
	src/mErlang.C \
	hdr/mErlang.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/mErlang.o \
		src/mErlang.C

obj/NaiveArbitraryDiscrete.o:	\
	src/NaiveArbitraryDiscrete.C \
	hdr/NaiveArbitraryDiscrete.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/NaiveArbitraryDiscrete.o \
		src/NaiveArbitraryDiscrete.C

obj/NegativeBinomial.o:	\
	src/NegativeBinomial.C \
	hdr/NegativeBinomial.H \
	hdr/Geometric.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/NegativeBinomial.o \
		src/NegativeBinomial.C

obj/PearsonTypeV.o:	\
	src/PearsonTypeV.C \
	hdr/PearsonTypeV.H \
	hdr/Gamma.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../utils/hdr/ExtUseCntPtr.H \
	../random/hdr/Random.H 
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/PearsonTypeV.o \
		src/PearsonTypeV.C

obj/PearsonTypeVI.o:	\
	src/PearsonTypeVI.C \
	hdr/PearsonTypeVI.H \
	hdr/Gamma.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../utils/hdr/ExtUseCntPtr.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/PearsonTypeVI.o \
		src/PearsonTypeVI.C

obj/Poisson.o:	\
	src/Poisson.C \
	hdr/Poisson.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Poisson.o \
		src/Poisson.C

obj/QPlaceArbitraryDiscrete.o:	\
	src/QPlaceArbitraryDiscrete.C \
	hdr/QPlaceArbitraryDiscrete.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/QPlaceArbitraryDiscrete.o \
		src/QPlaceArbitraryDiscrete.C

obj/Uniform.o:	\
	src/Uniform.C \
	hdr/Uniform.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Uniform.o \
		src/Uniform.C

obj/Weibull.o:	\
	src/Weibull.C \
	hdr/Weibull.H \
	../utils/hdr/Returns.H \
	../utils/hdr/Debug.H \
	../random/hdr/Random.H
	$(CPP) $(CPP_OPTS) \
		$(INCLUDES) \
		$(DEBUG) \
		-o obj/Weibull.o \
		src/Weibull.C

clean:
	-(rm -fr obj $(TARGET) );

