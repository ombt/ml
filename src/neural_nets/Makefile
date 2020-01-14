
MAKE = make
MAKE_OPTS =

CD = cd
CD_OPTS =

SUBDIRS = \
	utils \
	random \
	distributions \
	basic_nn_1 \
	basic_nn_2 \
	basic_nn_3

all:
	for SUBDIR in $(SUBDIRS); do \
		( $(CD) $(CD_OPTS) $$SUBDIR; \
		$(MAKE) $(MAKE_OPTS) -f Makefile all; ) \
	done

clean:
	for SUBDIR in $(SUBDIRS); do \
		( $(CD) $(CD_OPTS) $$SUBDIR; \
		$(MAKE) $(MAKE_OPTS) -f Makefile clean; ) \
	done
