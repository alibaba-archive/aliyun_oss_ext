#ifndef INCLUDE_DECOMPRESS_READER_H_
#define INCLUDE_DECOMPRESS_READER_H_

#include "postgres.h"

#include "ossapi.h"

extern const char *str_oss_compression[];

#define OSS_ZIP_DEFAULT_CHUNKSIZE (1024 * 1024 * 2)

extern uint64	OSS_ZIP_DECOMPRESS_CHUNKSIZE;

#ifdef HAVE_LIBZ
#include <zlib.h>
#else

#error "oss decompressor must include zlib"

typedef struct _z_stream
{
	void	   *next_in;
	void	   *next_out;
	size_t		avail_in;
	size_t		avail_out;
} z_stream;
typedef z_stream *z_streamp;
#endif

typedef struct z_decompress_reader
{
	z_stream	zstream;
	char		*in;
	char		*out;
	uint64		outOffset;
} z_decompress_reader;

typedef struct ext_oss_t OssHander;
typedef struct ext_oss_t OssSource;

extern void z_decompress_reader_open(z_decompress_reader *reader, bool async, char *msg) ;
extern z_decompress_reader *init_z_decompress_reader(void);
extern void z_decompress_reader_destroy(z_decompress_reader *reader);
extern size_t z_decompress_internal(OssHander *myData, z_decompress_reader *com_hd, void *buf,
													size_t bufSize, bool async, char *msg);
extern size_t z_decompress_read(OssSource *self, void *buf, size_t bufSize);


#endif /* INCLUDE_DECOMPRESS_READER_H_ */
