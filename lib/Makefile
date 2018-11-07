#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for lib (miscellaneous stuff)
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/lib/Makefile,v 1.21 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/lib
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CFLAGS += -I/usr/local/include/curl -I/usr/include/apr-1
override LIBS := $(LIBS) /usr/local/lib/libcurl.a /usr/lib64/libapr-1.a -laprutil-1 /usr/lib/libmxml.a 

OBJS =  dllist.o stringinfo.o ossapi.o \
        aos_buf.o     aos_http_io.o  aos_status.o  aos_transport.o \
        oss_auth.o    oss_define.o     oss_object.o  oss_xml.o \
        aos_fstack.o  aos_log.o      aos_string.o  aos_util.o \
        oss_bucket.o  oss_multipart.o  oss_util.o oss_live.o \
	decompress_reader.o compress_writer.o


include $(top_srcdir)/src/backend/common.mk

