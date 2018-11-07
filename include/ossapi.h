#ifndef OSSAPI_H
#define OSSAPI_H

#include "postgres.h"

#include "utils/rel.h"
#include "nodes/pg_list.h"

#ifndef WIN32
#include <sys/time.h>

typedef struct timeval TimevalStruct;

#define GETTIMEOFDAY(T) gettimeofday(T, NULL)
#define DIFF_MSEC(T, U, res) \
do { \
	res = ((((double) ((T)->tv_sec - (U)->tv_sec)) * 1000000.0 + \
	  ((double) ((T)->tv_usec - (U)->tv_usec))) / 1000.0); \
} while(0)

#else

#include <sys/types.h>
#include <sys/timeb.h>
#include <windows.h>

typedef LARGE_INTEGER TimevalStruct;
#define GETTIMEOFDAY(T) QueryPerformanceCounter(T)
#define DIFF_MSEC(T, U, res) \
do { \
	LARGE_INTEGER frq; 						\
											\
	QueryPerformanceFrequency(&frq); 		\
	res = (double)(((T)->QuadPart - (U)->QuadPart)/(double)frq.QuadPart); \
	res *= 1000; \
} while(0)

#endif

#define OSS_MAX_FILE_PATH	1024

#define SPIN_SLEEP_MSEC		10
#define READ_UNIT_SIZE		(1024 * 1024)
#define INITIAL_BUF_LEN		(16 * READ_UNIT_SIZE)
#define ERROR_MESSAGE_LEN	1024

#define WRITE_UNIT_SIZE		(1024 * 1024)

#define OSS_FLUSH_BUF_DEFAULT_SIZE		(10 * WRITE_UNIT_SIZE)

#define OSS_WRITE_FILE_DEFAULT_SIZE		(1024 * WRITE_UNIT_SIZE)

#define OSS_FLUSH_BUF_MIN_SIZE		WRITE_UNIT_SIZE
#define OSS_FLUSH_BUF_MAX_SIZE		(128 * WRITE_UNIT_SIZE)

#define OSS_WRITE_FILE_MIN_SIZE		(8 * WRITE_UNIT_SIZE)
#define OSS_WRITE_FILE_MAX_SIZE		(((uint64) 4000) * WRITE_UNIT_SIZE)

typedef struct oss_request_options {
    int speed_limit;
    int speed_time;
    int dns_cache_timeout;
    int connect_timeout;
} oss_request_options;

typedef struct oss_connect {
	char	   *osshost;
	char	   *ossid;
	char	   *osskey;
	char	   *bucket;
} oss_connect;

#ifdef HAVE_LONG_INT_64
#define int64_FMT			   "%ld"
#else
#define int64_FMT			   "%lld"
#endif

typedef struct Source Source;

typedef size_t (*SourceWriteProc) (void *self, void *buffer, size_t request_len);
typedef size_t (*SourceReadProc) (void *self, void *buffer, size_t request_len);
typedef void (*SourceCloseProc) (void *self);

struct Source
{
	SourceWriteProc write;		/** write */
	SourceReadProc read;		/** read */
	SourceCloseProc close;		/** close */
};

typedef enum
{
	OSS_COMPRESSION_NONE = 0,
	OSS_COMPRESSION_GZIP,
	OSS_COMPRESSION_ZLIB,
	OSS_COMPRESSION_UNKNOWN
} oss_compression_type;

#define AOS_CONNECT_TIMEOUT		10
#define AOS_DNS_CACHE_TIMOUT	60
#define AOS_MIN_SPEED_LIMIT		1024
#define AOS_MIN_SPEED_TIME		15

typedef struct oss_file_options {
	oss_compression_type	type;
	char					*osspath;
	char					*ossdir;
	char					*ossprefix;
} oss_file_options;

typedef struct oss_exp_options {
    int64	flush_block;
    int64	file_max_size;
	int		nthread;
	bool	async;
	int		pipe_block_size;
	int		compression_level;
} oss_exp_options;

typedef struct
{
	int64		length;
	char	   *filename;
} oss_file;

struct ext_oss_t
{
	char	   *url;
	char	   *protocol;

	oss_connect	conn;
	oss_file_options	file_opt;

	char	   *ossmode;

	char	   *currentfile;
	int64		length;
	int64		offset;

	int32		numsegments;
	int32		segindex;

	List	   *filelist;

	Source		base;

	/* async mode */
	bool		async;

	bool		eof;
	pthread_t	th;
	pthread_mutex_t lock;

	char	   *buffer;			/* read or write buffer */
	int			size;			/* buffer size */
	int			begin;			/* begin of the buffer finished with reading */
	int			end;			/* end of the buffer finished with reading */

	/*
	 * because ereport() does not support multi-thread, the read thread stores
	 * away error messsage in a message buffer.
	 */
	char		errmsg[ERROR_MESSAGE_LEN];

	/* for write */
	bool		is_export;
	uint32		flush_block;
	uint32		file_max_size;

	int			fileindex;
	int64		file_flush_offset;
	int64		write_row_count;
	int64		write_byte_count;
	double		flush_data_timer;
	char		*export_relname;

	oss_request_options	ro;

	void		*com_hd;

	oss_exp_options	write_opt;

	/* for name ossfile by distributed column */
	char		*distributed_column;
	bool		print_distributed_column;

	MemoryContext	ctx;
};

typedef struct ext_oss_t ext_oss_t;

extern void SourceClose(void *selfp);
extern size_t SourceRead(ext_oss_t *self, void *buffer, size_t len);
extern size_t SourceRead_internal(ext_oss_t *myData, void *buffer, size_t len,
				bool auto_next_file, bool async, char *msg);
extern void oss_env_init(void);
extern int64 oss_get_file_length(oss_connect *conn, char *filename, oss_request_options ro);
extern List *list_ossfiles_ondir(oss_connect *conn, char *dir, oss_request_options ro, bool is_prefix);
extern bool is_ossfile_exist(oss_connect *conn, char *filename, oss_request_options ro);
extern size_t oss_read_buffer(oss_connect *conn, char *filename, void *buffer, int64 offset, size_t len, bool async, char *msg, oss_request_options ro);
extern bool oss_append_file_from_buffer(oss_connect *conn, char *filename, char *data, size_t len, bool checktype,
										int64 append_position, oss_request_options ro,
										bool async, char *msg);
extern bool is_endpoint_in_white_list(char *endpoint);
extern void oss_next_file(ext_oss_t *myData);
extern void oss_wirte_next_file(ext_oss_t *myData);

#endif
