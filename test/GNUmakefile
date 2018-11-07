#MYPREFIX := $(shell grep "S\[\"prefix\"\]=" ../../../config.status |awk -F'=' '{print $$2}' |awk -F'"' '{print $$2}')

gpdb_top=../../../..
prefix := $(MYPREFIX)

default: installcheck

DEF_PREFIX = $(HOME)/tmp_basedir_for_gpdb_bld
PSQLDIR =
ifeq ($(prefix)"set","set")
  PSQLDIR = $(DEF_PREFIX)/bin
else
  PSQLDIR = $(prefix)/bin
endif

TESTS := setup_protocol $(TESTS)

installcheck:
	sh -x setup_data.sh ; \
	sh -x setup_files.sh ; \
	$(gpdb_top)/src/test/regress/pg_regress --psqldir=$(PSQLDIR) --schedule=all_schedule ;

installcheck-tests:
	sh -x setup_data.sh ; \
	sh -x setup_files.sh ; \
	$(gpdb_top)/src/test/regress/pg_regress --psqldir=$(PSQLDIR) $(TESTS) ;

clean:
	rm -rf sql results expected input output

distclean: ;

.PHONY: clean distclean

