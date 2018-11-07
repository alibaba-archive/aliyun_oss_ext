MODULE_big = oss_ext
OBJS       = oss_ext.o ossapi.o compress_writer.o decompress_reader.o \
	lib/aos_buf.o     lib/aos_http_io.o  lib/aos_status.o  lib/aos_transport.o  \
	lib/oss_auth.o    lib/oss_define.o   lib/oss_object.o  lib/oss_xml.o \
	lib/aos_fstack.o  lib/aos_log.o      lib/aos_string.o  lib/aos_util.o  \
	lib/oss_bucket.o  lib/oss_multipart.o  lib/oss_util.o lib/oss_live.o

EXTENSION = oss_ext
DATA = oss_ext--2.0.sql oss_ext.control

PG_CPPFLAGS = -I/usr/local/include  -I/usr/local/include/curl -I/usr/include/apr-1 -Iinclude -I$(libpq_srcdir)

SHLIB_LINK = $(libpq)

PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
  PGXS := $(shell pg_config --pgxs)
  include $(PGXS)
else
  subdir = gpAux/extensions/gp_oss_ext
  top_builddir = ../../..
  include $(top_builddir)/src/Makefile.global
  include $(top_srcdir)/contrib/contrib-global.mk
  #NO_PGXS = 1
  #include ../../../src/makefiles/pgxs94.mk
endif

SHLIB_LINK = $(libpq) -Wl,-rpath,$$ORIGIN,-rpath,$$ORIGIN/lib,-rpath,$$ORIGIN/../lib -Wl,--as-needed  -L/usr/local/lib -lcurl -Wl,--as-needed  -L/usr/lib64 -lapr-1  -L/usr/local/lib -lcurl -Wl,--as-needed  -L/usr/lib64 -laprutil-1 -Wl,--as-needed -L/usr/local/lib -lmxml

MYPREFIX := $(shell grep "S\[\"prefix\"\]=" ../../../config.status |awk -F'=' '{print $$2}' |awk -F'"' '{print $$2}')

prefix := $(MYPREFIX)

DEF_PREFIX = $(HOME)/tmp_basedir_for_gpdb_bld

INSTALLDIR =
ifeq ($(prefix)"set","set")
  INSTALLDIR = $(DEF_PREFIX)
else
  INSTALLDIR = $(prefix)
endif

install:
	make ;
	make copy_lib;
	cp -Lfr oss_ext.control oss_ext--2.0.sql $(INSTALLDIR)/share/postgresql/extension/ ;
	if [[ -f /usr/bin/pigz ]]; then \
                cp -Lfr /usr/bin/pigz $(INSTALLDIR)/bin/ ;\
        fi

copy_lib:
	if [[ -f /usr/lib/libmxml.so.1 ]]; then \
		cp -Lfr /usr/lib/libmxml.so.1 $(INSTALLDIR)/lib ;\
	elif [[ -f /usr/lib64/libmxml.so.1 ]]; then \
	        cp -Lfr /usr/lib64/libmxml.so.1 $(INSTALLDIR)/lib ;\
	fi

installcheck check installcheck-good:
	make ; make install ; cd test ; make installcheck ;

clean distclean:
	rm -f *.o *.so *.a lib/*.a
	cd lib;rm -rf *.o;	
