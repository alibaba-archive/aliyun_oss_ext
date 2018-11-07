#ifndef INCLUDE_COMPRESS_WRITER_H_
#define INCLUDE_COMPRESS_WRITER_H_

#include "postgres.h"
#include "ossapi.h"

#define OSS_MIN_COMPRESS_THREAD_NUM			1
#define OSS_DEFAULT_COMPRESS_THREAD_NUM		3
#define OSS_MAX_COMPRESS_THREAD_NUM			8

#define	DEFAULT_PIPE_BLOCK_SIZE		32 * 1024
#define	MIN_PIPE_BLOCK_SIZE			8 * 1024
#define	MAX_PIPE_BLOCK_SIZE			8 * 1024 * 1024

#define DEFAULT_OSS_COMPRESS_LEVEL		6
#define MIN_COMPRESS_LEVEL			1
#define MAX_COMPRESS_LEVEL			9


extern int init_compress_writer(ext_oss_t * self, bool init_subprocess);

#endif
