#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/extprotocol.h"
#include "access/fileam.h"
#include "catalog/pg_proc.h"
#include "commands/tablecmds.h"
#include "commands/extension.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include <inttypes.h>
#include "cdb/cdbvars.h"
#include "utils/resowner.h"

#include "ossapi.h"
#include "decompress_reader.h"
#include "compress_writer.h"

#define MAX_DELIMITER_ARRARY_LEN	4

static void list_oss_file(ext_oss_t * myData);
static List *list_ossfiles_usesegid(ext_oss_t * myData);

static void AsyncSourceClose(void *selfp);
static size_t AsyncSourceRead(void *selfp, void *buffer, size_t request_len);
static void CreateAsyncOssSource(ext_oss_t * self);
static void *AsyncOssSourceMain(void *arg);
static void CreateOssSource(ext_oss_t *self);

static size_t SourceWrite(void *selfp, void *buffer, size_t request_len);
static void WriteSourceClose(void *selfp);
static void CreateOssWriteSource(ext_oss_t * self);
static void flush_and_switch_to_next_file(ext_oss_t  *myData);
static void flush_ossfile(ext_oss_t  *myData);
static bool is_oss_protocol(char *protocol);
static char *truncate_options(const char *url_with_options);
static bool oss_isblank(const char c);
static char *find_delimiter(const char *url_with_options);
static char *get_opt_oss(const char *url, const char *key);
static ext_oss_t *parse_oss_protocol(Relation rel, char *url, bool is_export);
static void free_data(ext_oss_t *myData);
static void oss_ext_abort_callback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg);

/* Do the module magic dance */
PG_MODULE_MAGIC; 
PG_FUNCTION_INFO_V1(oss_import);
PG_FUNCTION_INFO_V1(oss_export);
PG_FUNCTION_INFO_V1(oss_validate_urls);

Datum		oss_import(PG_FUNCTION_ARGS);
Datum		oss_export(PG_FUNCTION_ARGS);
Datum		oss_validate_urls(PG_FUNCTION_ARGS);

static bool is_oss_ext_callback_registered = false;
static ext_oss_t	*curr_mydata = NULL;

static void
oss_ext_abort_callback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	ext_oss_t  *myData;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	myData = curr_mydata;
	if (myData)
	{
		free_data(myData);
		curr_mydata = NULL;
	}
}

/*
 * Import data into GPDB.
 */
Datum
oss_import(PG_FUNCTION_ARGS)
{
	ext_oss_t  *myData;
	char	   *data;
	int			datlen;
	size_t		nread = 0;
	static bool init_env = false;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_EXTPROTOCOL(fcinfo))
		elog(ERROR, "extprotocol_import: not called by external protocol manager");

	/* Get our internal description of the protocol */
	myData = (ext_oss_t *) EXTPROTOCOL_GET_USER_CTX(fcinfo);

	if (EXTPROTOCOL_IS_LAST_CALL(fcinfo))
	{
		if (myData)
		{
			free_data(myData);
			curr_mydata = NULL;
		}

		EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
		PG_RETURN_INT32(0);
	}

	if (myData == NULL)
	{
		char	   *url_with_options = EXTPROTOCOL_GET_URL(fcinfo);
		Relation	rel = EXTPROTOCOL_GET_RELATION(fcinfo);

		if (init_env == false)
		{
			oss_env_init();
			init_env = true;
		}

		if (is_oss_ext_callback_registered == false)
		{
			RegisterResourceReleaseCallback(oss_ext_abort_callback, NULL);
			is_oss_ext_callback_registered = true;
		}

		myData = parse_oss_protocol(rel, url_with_options, false);
		if (is_oss_protocol(myData->protocol) == false)
			elog(ERROR, "internal error: oss_ext called with a different protocol");

		list_oss_file(myData);

		oss_next_file(myData);

		if (myData->currentfile == NULL)
		{
			;
		}
		else if (myData->async == true)
		{
			CreateAsyncOssSource(myData);
		}
		else if (myData->async == false)
		{
			CreateOssSource(myData);
		}

		Assert(curr_mydata == NULL);
		curr_mydata = myData;
		EXTPROTOCOL_SET_USER_CTX(fcinfo, myData);
	}

	/*
	 * =======================================================================
	 * DO THE IMPORT
	 * =======================================================================
	 */

	data = EXTPROTOCOL_GET_DATABUF(fcinfo);
	datlen = EXTPROTOCOL_GET_DATALEN(fcinfo);

	if (datlen > 0)
	{
		if (myData->base.read != NULL)
		{
			nread = myData->base.read(myData, data, datlen);
		}
		else
		{
			nread = 0;
		}
	}

	PG_RETURN_INT32((int) nread);
}

Datum
oss_validate_urls(PG_FUNCTION_ARGS)
{
	int			nurls;
	int			i;
	ValidatorDirection direction;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_EXTPROTOCOL_VALIDATOR(fcinfo))
		elog(ERROR, "ossprot_validate_urls: not called by external protocol manager");

	nurls = EXTPROTOCOL_VALIDATOR_GET_NUM_URLS(fcinfo);
	direction = EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo);

	/*
	 * Dumb example 1: search each url for a substring we don't want to be
	 * used in a url. in this example it's 'secured_directory'.
	 */
	for (i = 1; i <= nurls; i++)
	{
		char	   *url = EXTPROTOCOL_VALIDATOR_GET_NTH_URL(fcinfo, i);

		elog(NOTICE, "url %d %s", i, url);
	}

	/*
	 * Dumb example 2: set a limit on the number of urls used. In this example
	 * we limit readable external tables that use our protocol to 2 urls max.
	 */
	if (direction == EXT_VALIDATE_READ && nurls > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
			   errmsg("more than 2 urls aren't allowed in this protocol ")));
	}

	PG_RETURN_VOID();
}

static void
list_oss_file(ext_oss_t * myData)
{
	List	   *files = NIL;
	ListCell   *lc = NULL;
	int			count = 0;
	List	   *freelist = NIL;

	if (myData->file_opt.ossdir)
	{
		files = list_ossfiles_ondir(&myData->conn, myData->file_opt.ossdir, myData->ro, false);
	}
	else if (myData->file_opt.ossprefix)
	{
		files = list_ossfiles_ondir(&myData->conn, myData->file_opt.ossprefix, myData->ro, true);
	}
	else
	{
		files = list_ossfiles_usesegid(myData);
	}

	if (myData->segindex == 0)
	{
		int			numfile = list_length(files);
		char	   *mode;

		if (myData->async)
		{
			mode = "asyncmode";
		}
		else
		{
			mode = "syncmode";
		}

		if (numfile <= 0)
		{
			elog(WARNING, "does not match any file in oss");
		}
		else if (myData->file_opt.osspath)
		{
			oss_file   *begin = lfirst(list_head(files));
			oss_file   *end = lfirst(list_tail(files));

			elog(NOTICE, "a total of %d files will be loaded, begin %s, end %s, %s",
				 numfile, begin->filename, end->filename, mode);
		}
		else
		{
			elog(NOTICE, "a total of %d files will be loaded, %s", numfile, mode);
		}
	}

	foreach(lc, files)
	{
		oss_file   *ossfile = (oss_file *) lfirst(lc);

		if ((count % myData->numsegments) == myData->segindex)
		{
			if (ossfile->length == 0)
			{
				ossfile->length = oss_get_file_length(&myData->conn, ossfile->filename, myData->ro);
			}
			myData->filelist = lappend(myData->filelist, ossfile);
		}
		else
		{
			freelist = lappend(freelist, ossfile);
		}
		count++;
	}

	if (freelist)
	{
		foreach(lc, freelist)
		{
			oss_file   *ossfile = (oss_file *) lfirst(lc);

			if (ossfile->filename)
			{
				pfree(ossfile->filename);
				ossfile->filename = NULL;
			}
		}

		list_free_deep(freelist);
	}

	if (files)
	{
		list_free(files);
	}
}

static List *
list_ossfiles_usesegid(ext_oss_t * myData)
{
	char		currentfile[OSS_MAX_FILE_PATH] = {0};
	int64		length = 0;
	int32		fileindex = 0;
	List	   *filelist = NIL;

	snprintf(currentfile, OSS_MAX_FILE_PATH - 1, "%s", myData->file_opt.osspath);
	length = oss_get_file_length(&myData->conn, currentfile, myData->ro);

	if (length >= 0)
	{
		oss_file   *ossfile = NULL;

		ossfile = (oss_file *) palloc(sizeof(oss_file));
		ossfile->filename = pstrdup(currentfile);
		ossfile->length = length;
		filelist = lappend(filelist, ossfile);
	}

	fileindex++;

	do
	{
		snprintf(currentfile, OSS_MAX_FILE_PATH - 1, "%s.%d", myData->file_opt.osspath, fileindex);

		length = oss_get_file_length(&myData->conn, currentfile, myData->ro);

		if (length >= 0)
		{
			oss_file   *ossfile = NULL;

			ossfile = (oss_file *) palloc(sizeof(oss_file));
			ossfile->filename = pstrdup(currentfile);
			ossfile->length = length;
			filelist = lappend(filelist, ossfile);
		}
		fileindex++;
	} while (length >= 0);

	return filelist;
}


/* ========================================================================
 * AsyncSource
 * ========================================================================*/
static void *
AsyncOssSourceMain(void *arg)
{
	ext_oss_t  *self = (ext_oss_t *) arg;
	size_t		bytesread = 0;
	int			begin;
	int			end;
	int			size;
	int			len;
	char	   *data;
	int64		offset = 0;

	Assert(self->begin == 0);
	Assert(self->end == 0);

	for (;;)
	{
		pthread_mutex_lock(&self->lock);

		begin = self->begin;
		end = self->end;
		size = self->size;
		data = self->buffer;

		if (begin > end)
		{
			len = begin - end;
			if (len <= READ_UNIT_SIZE)
				len = 0;	
		}
		else
		{
			len = size - end;
			if (begin == 0 && len <= READ_UNIT_SIZE)
				len = 0;
		}

		if (len == 0)
		{
			if (self->eof)
				break;

			pthread_mutex_unlock(&self->lock);
			pg_usleep(SPIN_SLEEP_MSEC * 1000);
			continue;
			/* retry */
		}

		len = Min(len, READ_UNIT_SIZE);

		if (self->file_opt.type == OSS_COMPRESSION_NONE)
		{
			if (self->length < 0)
			{
				self->eof = true;
				break;
			}
			else if (self->length == 0)
			{
				pthread_mutex_unlock(&self->lock);
				oss_next_file(self);
				continue;
			}
			else if (self->offset + len > self->length)
			{
				len = self->length - self->offset;
			}

			if (len == 0)
			{
				pthread_mutex_unlock(&self->lock);
				oss_next_file(self);
				continue;
			}
			else
			{
				offset = self->offset;
				bytesread = oss_read_buffer(&self->conn, self->currentfile, data + end, offset, len,
											true, self->errmsg, self->ro);
				self->offset += bytesread;
			}
		}
		else if (self->file_opt.type == OSS_COMPRESSION_GZIP)
		{
			z_decompress_reader *com_hd = (z_decompress_reader *)(self->com_hd);
			bytesread = z_decompress_internal(self, com_hd, data + end, len, true, self->errmsg);
		}

		if (bytesread == 0)
		{
			pthread_mutex_unlock(&self->lock);
			if (self->errmsg[0] == '\0')
			{
				self->eof = true;
			}
			return NULL;
		}

		end += bytesread;
		if (end == self->size)
			end = 0;

		self->end = end;
		if (self->eof)
			break;

		pthread_mutex_unlock(&self->lock);
	}

	pthread_mutex_unlock(&self->lock);

	return NULL;
}

static void
CreateAsyncOssSource(ext_oss_t * self)
{
	self->base.read = (SourceReadProc) AsyncSourceRead;
	self->base.close = (SourceCloseProc) AsyncSourceClose;

	self->size = INITIAL_BUF_LEN;
	self->begin = 0;
	self->end = 0;
	self->buffer = palloc(self->size);
	if (self->buffer == NULL)
		elog(ERROR, "out of memory");

	memset(self->buffer, 0, self->size);
	self->errmsg[0] = '\0';

	self->eof = false;

	if (self->file_opt.type == OSS_COMPRESSION_GZIP)
	{
		z_decompress_reader *com_hd = NULL;
		com_hd = init_z_decompress_reader();
		self->com_hd = (void *)com_hd;
		z_decompress_reader_open(com_hd, false, NULL);
	}

	pthread_mutex_init(&self->lock, NULL);
	if (pthread_create(&self->th, NULL, AsyncOssSourceMain, self) != 0)
		elog(ERROR, "create oss thread use pthread_create AsyncOssSourceMain faild");

	return;
}

static size_t
AsyncSourceRead(void *selfp, void *buffer, size_t request_len)
{
	char	   *data;
	int			size;
	int			begin;
	int			end;
	char		errhead;
	size_t		bytesread;
	int			n;
	ext_oss_t  *self = (ext_oss_t *) selfp;

	/* 4 times of the needs size allocate a buffer at least */
	if (self->size < request_len * 4)
	{
		char	   *newbuf;
		int			newsize;

		/* read buffer a multiple of READ_UNIT_SIZE */
		newsize = (request_len * 4 - 1) -
			((request_len * 4 - 1) % READ_UNIT_SIZE) +
			READ_UNIT_SIZE;
		newbuf = palloc(newsize);
		memset(newbuf, 0, newsize);

		pthread_mutex_lock(&self->lock);

		/* copy it in new buffer from old buffer */
		if (self->begin > self->end)
		{
			memcpy(newbuf, self->buffer + self->begin,
				   self->size - self->begin);
			memcpy(newbuf + self->size - self->begin, self->buffer, self->end);
			self->end = self->size - self->begin + self->end;
		}
		else
		{
			memcpy(newbuf, self->buffer + self->begin, self->end - self->begin);
			self->end = self->end - self->begin;
		}

		pfree(self->buffer);
		self->buffer = newbuf;
		self->size = newsize;
		self->begin = 0;

		pthread_mutex_unlock(&self->lock);
	}

	/* this value that a read thread does not change */
	data = self->buffer;
	size = self->size;
	begin = self->begin;

	bytesread = 0;
retry:
	end = self->end;
	errhead = self->errmsg[0];

	/* error in read thread */
	if (errhead != '\0')
	{
		/* wait for error message to be set */
		pthread_mutex_lock(&self->lock);
		pthread_mutex_unlock(&self->lock);

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("%s", self->errmsg)));
	}

	if (begin < end)
	{
		n = Min(request_len - bytesread, end - begin);
		memcpy((char *) buffer + bytesread, data + begin, n);
		begin += n;
		bytesread += n;
	}
	else if (begin > end)
	{
		n = Min(request_len - bytesread, size - begin);
		memcpy((char *) buffer + bytesread, data + begin, n);
		begin += n;
		bytesread += n;

		if (begin == size)
		{
			self->begin = begin = 0;

			if (bytesread < request_len)
				goto retry;
		}
	}

	self->begin = begin;

	if (bytesread == request_len || (self->eof && begin == end))
		return bytesread;

	pg_usleep(SPIN_SLEEP_MSEC * 1000);

	goto retry;
}

static void
AsyncSourceClose(void *selfp)
{
	ext_oss_t  *self = (ext_oss_t *) selfp;

	self->eof = true;

	pthread_mutex_unlock(&self->lock);

	if (self->th)
	{
		pthread_join(self->th, NULL);
	}

	if (self->buffer != NULL)
		pfree(self->buffer);
	self->buffer = NULL;

	if (self->file_opt.type == OSS_COMPRESSION_GZIP)
	{
		z_decompress_reader_destroy((z_decompress_reader *)(self->com_hd));
	}
}

static void
CreateOssSource(ext_oss_t * self)
{
	self->base.read = (SourceReadProc) SourceRead;
	self->base.close = (SourceCloseProc) SourceClose;

	if (self->file_opt.type == OSS_COMPRESSION_GZIP)
	{
		z_decompress_reader *com_hd = NULL;
		self->base.read = (SourceReadProc) z_decompress_read;
		com_hd = init_z_decompress_reader();
		self->com_hd = (void *)com_hd;
		z_decompress_reader_open(com_hd, false, NULL);
	}

	return;
}

Datum 
oss_export(PG_FUNCTION_ARGS)
{
	ext_oss_t   *myData;
	char			*data;
	int				 datlen;
	size_t			 wrote = 0;
	static bool init_env = false;	

	/* Must be called via the external table format manager */
	if (!CALLED_AS_EXTPROTOCOL(fcinfo))
		elog(ERROR, "extprotocol_export: not called by external protocol manager");

	/* Get our internal description of the protocol */
	myData = (ext_oss_t *) EXTPROTOCOL_GET_USER_CTX(fcinfo);

	if(EXTPROTOCOL_IS_LAST_CALL(fcinfo))
	{
		if (myData)
		{
			free_data(myData);
			curr_mydata = NULL;
		}

		EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
		PG_RETURN_INT32(0);
	}	

	if (myData == NULL)
	{
		/* first call. do any desired init */
		char	   *url_with_options = EXTPROTOCOL_GET_URL(fcinfo);
		Relation	rel = EXTPROTOCOL_GET_RELATION(fcinfo);

		if (init_env == false)
		{
			oss_env_init();
			init_env = true;
		}

		if (is_oss_ext_callback_registered == false)
		{
			RegisterResourceReleaseCallback(oss_ext_abort_callback, NULL);
			is_oss_ext_callback_registered = true;
		}

		myData = parse_oss_protocol(rel, url_with_options, true);
		if (is_oss_protocol(myData->protocol) == false)
			elog(ERROR, "internal error: oss_ext called with a different protocol");

		if (myData->segindex == 0)
		{
			elog(NOTICE, "begin writiing data to oss directory %s, with block size %u MB and oss file size %u MB", 
							myData->file_opt.ossprefix, myData->flush_block/WRITE_UNIT_SIZE, myData->file_max_size/WRITE_UNIT_SIZE);

			if (myData->file_opt.type == OSS_COMPRESSION_GZIP)
			{
				elog(NOTICE, "writiing ossfile type is gzip compress thread %d compress level %d pipe size %d kB", 
					myData->write_opt.nthread, myData->write_opt.compression_level, myData->write_opt.pipe_block_size/1024);
			}
		}

		oss_wirte_next_file(myData);

		if (myData->currentfile != NULL)
		{
			if (myData->file_opt.type == OSS_COMPRESSION_NONE)
			{
				CreateOssWriteSource(myData);
			}
			else if (myData->file_opt.type == OSS_COMPRESSION_GZIP)
			{
				init_compress_writer(myData, true);
			}
		}

		Assert(curr_mydata == NULL);
		curr_mydata = myData;
		EXTPROTOCOL_SET_USER_CTX(fcinfo, myData);
	}

	/* =======================================================================
	 *                            DO THE EXPORT
	 * ======================================================================= */
	
	data 	= EXTPROTOCOL_GET_DATABUF(fcinfo);
	datlen 	= EXTPROTOCOL_GET_DATALEN(fcinfo);

	if(datlen > 0)
	{
		if (myData->base.write != NULL)
		{
			wrote = myData->base.write(myData, data, datlen);
		}
	}

	PG_RETURN_INT32((int)wrote);
}

static void
CreateOssWriteSource(ext_oss_t * self)
{
	self->base.write = (SourceWriteProc) SourceWrite;
	self->base.close = (SourceCloseProc) WriteSourceClose;

	self->size = self->flush_block;
	self->begin = 0;
	self->end = 0;
	self->buffer = palloc(self->size);
	if (self->buffer == NULL)
		elog(ERROR, "out of memory while allocating buffer in CreateOssWriteSource");

	memset(self->buffer, 0, self->size);

	return;
}

static size_t
SourceWrite(void *selfp, void *buffer, size_t request_len)
{
	ext_oss_t  *myData = (ext_oss_t *) selfp;
	char	   *data = (char *) buffer;

	if (request_len <= 0)
	{
		return 0;
	}

	if (request_len > myData->flush_block)
	{
		elog(ERROR, "single row of data is greater than the length of the data cache, please set oss_flush_block_size greater than %zu MB", request_len/WRITE_UNIT_SIZE);
	}

	myData->write_row_count++;
	myData->write_byte_count += request_len;

	/* put into local buffer */
	if ((myData->offset + request_len) <= myData->flush_block)
	{
		memcpy(myData->buffer + myData->offset, data, request_len);
		myData->offset += request_len;
		return request_len;
	}

	flush_and_switch_to_next_file(myData);

	/* put into local buffer */
	memcpy(myData->buffer, data, request_len);
	myData->offset += request_len;

	return request_len;
}

static void
flush_and_switch_to_next_file(ext_oss_t  *myData)
{
	if ((myData->file_flush_offset + myData->offset) > myData->file_max_size)
	{
		oss_wirte_next_file(myData);
		myData->file_flush_offset = 0;
	}

	flush_ossfile(myData);
}

static void
flush_ossfile(ext_oss_t  *myData)
{
	TimevalStruct   before, after;
	double                  elapsed_msec = 0;

	GETTIMEOFDAY(&before);
	oss_append_file_from_buffer(&myData->conn, myData->currentfile, myData->buffer,
								myData->offset, false, 0, myData->ro, false, NULL);
	GETTIMEOFDAY(&after);
	DIFF_MSEC(&after, &before, elapsed_msec);

	myData->file_flush_offset += myData->offset;
	myData->flush_data_timer += elapsed_msec;
	myData->offset = 0;
}


static void
WriteSourceClose(void *selfp)
{
	ext_oss_t  *myData = (ext_oss_t *) selfp;

	if (myData->buffer)
	{
		if (myData->offset > 0)
		{
			flush_and_switch_to_next_file(myData);
		}

		pfree(myData->buffer);
		myData->buffer = NULL;
	}

	elog(DEBUG1, "segment %d wrote row " int64_FMT ", " int64_FMT " byte, write data cost %.3f ms", 
		myData->segindex, myData->write_row_count, myData->write_byte_count, myData->flush_data_timer);
}

static bool
is_oss_protocol(char *protocol)
{
	if (protocol == NULL)
		return false;

	while (*protocol != '\0')
	{
		if (*protocol == ' ' || *protocol == '\t' || *protocol == '\n')
			protocol++;
		else
			break;
	}

	if (strcasecmp(protocol, "oss") == 0)
	{
		return true;
	}

	return false;
}

static char *
truncate_options(const char *url_with_options)
{
	char	   *options = find_delimiter(url_with_options);
	int			url_len = strlen(url_with_options);

	if (options)
	{
		url_len = strlen(url_with_options) - strlen(options);
	}

	char	   *url = (char *) palloc(url_len + 1);

	if (url)
	{
		memcpy(url, url_with_options, url_len);
		url[url_len] = 0;
	}

	return url;
}

static bool
oss_isblank(const char c)
{
	return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static char *
get_opt_oss(const char *url, const char *key)
{
	const char *key_f = NULL;
	const char *key_tailing = NULL;
	char	   *key_val = NULL;
	int			val_len = 0;

	if (!url || !key)
	{
		return NULL;
	}

	char	   *key2search = (char *) palloc(strlen(key) + 3);

	if (!key2search)
	{
		elog(ERROR, "Can't allocate memory for string");
	}

	int			key_len = strlen(key);

	memcpy(key2search, key, key_len);
	key2search[key_len] = '=';
	key2search[key_len + 1] = 0;

	const char *options = find_delimiter(url);
	if (!options)
	{
		goto FAIL;
	}
	key_f = strstr(options, key2search);
	if (key_f == NULL)
	{
		goto FAIL;
	}

	key_f += strlen(key2search);
	if (*key_f == '\0')
	{
		goto FAIL;
	}

	while (oss_isblank(*key_f))
	{
		key_f++;
	}

	if (*key_f == '\0')
	{
		goto FAIL;
	}

	key_tailing = find_delimiter(key_f);
	val_len = 0;
	if (key_tailing)
	{
		val_len = strlen(key_f) - strlen(key_tailing);
	}
	else
	{
		val_len = strlen(key_f);
	}

	key_val = (char *) palloc(val_len + 1);
	if (!key_val)
	{
		goto FAIL;
	}

	memcpy(key_val, key_f, val_len);
	key_val[val_len] = 0;

	pfree(key2search);

	return key_val;

FAIL:
	if (key2search)
	{
		pfree(key2search);
	}

	if (key_val)
	{
		pfree(key_val);
	}

	return NULL;
}

static char *
find_delimiter(const char *url_with_options)
{
	char	*delimiter[MAX_DELIMITER_ARRARY_LEN] = {" ","\n","\r","\t"};
	char	*tmp[MAX_DELIMITER_ARRARY_LEN] = {NULL};
	char	*pos = NULL;
	int		i = 0;

	if (url_with_options == NULL)
		return NULL;

	for (i = 0; i < MAX_DELIMITER_ARRARY_LEN; i++)
	{
		tmp[i] = strstr((char *) url_with_options, delimiter[i]);
		if (pos == NULL && tmp[i] != NULL)
		{
			pos = tmp[i];
		}
		else if (pos != NULL && tmp[i] != NULL && pos > tmp[i])
		{
			pos = tmp[i];
		}
	}

	return pos;
}

static ext_oss_t *
parse_oss_protocol(Relation rel, char *url, bool is_export)
{
	int			protocol_len;
	char	   *host = NULL;
	char	   *asyncstr = NULL;
	char		*speed_limit = NULL;
	char		*speed_time = NULL;
	char		*dns_cache_timeout = NULL;
	char		*connect_timeout = NULL;
	char		*tmp_com_type = NULL;
	MemoryContext	ctx;
	MemoryContext	old_ctx;
	ext_oss_t		*oss;

	ctx = AllocSetContextCreate(CurrentMemoryContext, "oss_ext context",
						ALLOCSET_DEFAULT_MINSIZE,
						ALLOCSET_DEFAULT_INITSIZE,
						ALLOCSET_DEFAULT_MAXSIZE);

	old_ctx = MemoryContextSwitchTo(ctx);
	oss = palloc(sizeof(ext_oss_t));
	memset(oss, 0, sizeof(ext_oss_t));
	oss->ctx = ctx;
	oss->url = pstrdup(url);
	oss->is_export = is_export;

	/*
 * 	 * parse protocol
 * 	 	 */
	char	   *post_protocol = strstr(oss->url, "://");

	if (!post_protocol)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid oss prot URI \'%s\'", oss->url)));
	}

	protocol_len = post_protocol - oss->url;
	oss->protocol = (char *) palloc(protocol_len + 1);
	strncpy(oss->protocol, oss->url, protocol_len);
	oss->protocol[protocol_len] = 0;

	/* make sure there is more to the uri string */
	if (strlen(oss->url) <= protocol_len)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("invalid oss prot URI \'%s\' : missing path", oss->url)));


	host = truncate_options(oss->url);
	oss->conn.osshost = pstrdup(host + protocol_len + strlen("://"));
	pfree(host);

	oss->conn.ossid = get_opt_oss(oss->url, "id");
	oss->conn.osskey = get_opt_oss(oss->url, "key");

	if (oss->conn.osskey == NULL || oss->conn.ossid == NULL)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
			errmsg("invalid location uri \"%s\", no key or id",
			oss->url)));
	}

	oss->file_opt.osspath = get_opt_oss(oss->url, "filepath");
	oss->file_opt.ossprefix = get_opt_oss(oss->url, "prefix");
	oss->file_opt.ossdir = get_opt_oss(oss->url, "dir");

	oss->file_opt.type = OSS_COMPRESSION_NONE;
	tmp_com_type = get_opt_oss(oss->url, "compressiontype");
	if (tmp_com_type)
	{
		if (strcasecmp(tmp_com_type, str_oss_compression[OSS_COMPRESSION_GZIP]) == 0)
		{
			oss->file_opt.type = OSS_COMPRESSION_GZIP;
		}
		else if (strcasecmp(tmp_com_type, str_oss_compression[OSS_COMPRESSION_NONE]) == 0)
		{
			oss->file_opt.type = OSS_COMPRESSION_NONE;
		}
		else
		{
			elog(ERROR, "unknown compression type %s", tmp_com_type);
		}

		pfree(tmp_com_type);
		tmp_com_type = NULL;
	}

	oss->conn.bucket = get_opt_oss(oss->url, "bucket");
	
	asyncstr = get_opt_oss(oss->url, "async");
	if (asyncstr)
	{
		oss->async = DatumGetBool(DirectFunctionCall1(boolin, CStringGetDatum(asyncstr)));
		pfree(asyncstr);
	}
	else
	{
		oss->async = true;
	}

	if (oss->file_opt.ossdir == NULL && oss->file_opt.osspath == NULL && oss->file_opt.ossprefix == NULL)
	{
		elog(ERROR, "you must specify the parameter dir or filepath or prefix");
	}

	if ((oss->file_opt.ossdir && oss->file_opt.osspath) ||
		(oss->file_opt.ossdir && oss->file_opt.ossprefix) || 
		(oss->file_opt.osspath && oss->file_opt.ossprefix))
	{
		elog(ERROR, "filename or dir or prefix parameter can not be specified at the same time");
	}

	if (oss->conn.osshost == NULL || oss->conn.ossid == NULL ||
		oss->conn.osskey == NULL || oss->conn.bucket == NULL)
	{
		elog(ERROR, "you must specify host,id,key,bucket at the same time");
	}

	if (oss->file_opt.ossdir != NULL)
	{
		int			len = strlen(oss->file_opt.ossdir);

		if (oss->file_opt.ossdir[len - 1] != '/')
		{
			elog(ERROR, "ossdir must end with '/'");
		}
	}

	if (oss->file_opt.osspath != NULL && strlen(oss->file_opt.osspath) == 0)
			elog(ERROR, "osspath can not be empty");

	if (oss->file_opt.ossprefix != NULL && strlen(oss->file_opt.ossprefix) == 0)
			elog(ERROR, "ossprefix can not be empty");

	oss->numsegments = getgpsegmentCount();
	oss->segindex = GpIdentity.segindex;

	oss->currentfile = NULL;

	oss->fileindex = 0;
	oss->file_flush_offset = 0;

	oss->write_row_count = 0;
	oss->write_byte_count = 0;
	oss->flush_data_timer = 0;

	if (oss->is_export)
	{
		char	*str_fb = get_opt_oss(oss->url, "oss_flush_block_size");
		char	*str_fms = get_opt_oss(oss->url, "oss_file_max_size");
		char	*relname = RelationGetRelationName(rel);

		oss->ossmode = get_opt_oss(oss->url, "mode");

		Assert(relname != NULL);

		if (oss->ossmode != NULL && strcmp(oss->ossmode, "append") != 0)
		{
			elog(ERROR, "writeable oss table only supports the export of data in append mode");
		}

		if (oss->file_opt.ossdir == NULL && oss->file_opt.ossprefix == NULL)
		{
			elog(ERROR, "writeable oss table only supports the export of data to the virtual directory, dir or prefix must be specified");
		}

		if (oss->file_opt.ossdir && oss->file_opt.ossprefix)
		{
			elog(ERROR, "dir or prefix parameter can not be specified at the same time");
		}

		if (oss->file_opt.ossdir && oss->file_opt.ossprefix == NULL)
		{
			/* Here we replace ossdir to ossprefix */
			oss->file_opt.ossprefix = oss->file_opt.ossdir;
			oss->file_opt.ossdir = NULL;
		}

		if (str_fb == NULL)
		{
			oss->flush_block = OSS_FLUSH_BUF_DEFAULT_SIZE;
		}
		else
		{
			int tmp = atoi(str_fb);
			if (tmp * WRITE_UNIT_SIZE < OSS_FLUSH_BUF_MIN_SIZE ||
				tmp * WRITE_UNIT_SIZE > OSS_FLUSH_BUF_MAX_SIZE)
			{
				elog(ERROR, "flush to oss block size must be between 1 MB to 128 MB");
			}

			oss->flush_block = tmp * WRITE_UNIT_SIZE;
		}

		if (str_fms == NULL)
		{
			oss->file_max_size = OSS_WRITE_FILE_DEFAULT_SIZE;
		}
		else
		{
			uint64	tmp = atoi(str_fms);
			if (tmp * WRITE_UNIT_SIZE < OSS_WRITE_FILE_MIN_SIZE ||
				tmp * WRITE_UNIT_SIZE > OSS_WRITE_FILE_MAX_SIZE)
			{
				elog(ERROR, "oss file max size %"PRIu64" MB must be between 8 MB to 4000 MB", tmp);
			}

			oss->file_max_size = tmp * WRITE_UNIT_SIZE;
		}

		if (oss->flush_block > oss->file_max_size)
		{
			elog(ERROR, "flush to oss block size must less than oss file max size");
		}

		oss->export_relname = pstrdup(relname);

		if (oss->file_opt.type == OSS_COMPRESSION_GZIP)
		{
			char	*tmp_str = NULL;

			oss->async = true;

			oss->write_opt.flush_block = oss->flush_block;
			oss->write_opt.file_max_size = oss->file_max_size;
			oss->write_opt.nthread = OSS_DEFAULT_COMPRESS_THREAD_NUM;
			oss->write_opt.async = false;
			oss->write_opt.pipe_block_size = DEFAULT_PIPE_BLOCK_SIZE;
			oss->write_opt.compression_level = DEFAULT_OSS_COMPRESS_LEVEL;

			tmp_str = get_opt_oss(oss->url, "num_parallel_worker");
			if (tmp_str != NULL)
			{
				oss->write_opt.nthread = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(tmp_str)));
				if (oss->write_opt.nthread  < OSS_MIN_COMPRESS_THREAD_NUM ||
					oss->write_opt.nthread  > OSS_MAX_COMPRESS_THREAD_NUM)
				{
					elog(ERROR, "compression thread num must be greater than or equal to %d and less than or equal to %d",
												OSS_MIN_COMPRESS_THREAD_NUM, OSS_MAX_COMPRESS_THREAD_NUM);
				}
			}

			tmp_str = get_opt_oss(oss->url, "pipe_block_size");
			if (tmp_str != NULL)
			{
				oss->write_opt.pipe_block_size = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(tmp_str)));
				if (oss->write_opt.pipe_block_size > MAX_PIPE_BLOCK_SIZE ||
					oss->write_opt.pipe_block_size < MIN_PIPE_BLOCK_SIZE)
				{
					elog(ERROR, "pipe_block_size must be greater than or equal to %d and less than or equal to %d",
								MIN_PIPE_BLOCK_SIZE, MAX_PIPE_BLOCK_SIZE);
				}
			}
			
			tmp_str = get_opt_oss(oss->url, "compressionlevel");
			if (tmp_str != NULL)
			{
				oss->write_opt.compression_level = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(tmp_str)));
				if (oss->write_opt.compression_level > MAX_COMPRESS_LEVEL || 
					oss->write_opt.compression_level < MIN_COMPRESS_LEVEL)
				{
					elog(ERROR, "compression level must be greater than or equal to %d and less than or equal to %d",
								MIN_COMPRESS_LEVEL, MAX_COMPRESS_LEVEL);
				}
			}
		}
	}

	oss->ro.speed_limit = AOS_MIN_SPEED_LIMIT;
	oss->ro.speed_time = AOS_MIN_SPEED_TIME;
	oss->ro.speed_time = AOS_MIN_SPEED_TIME;	
	oss->ro.dns_cache_timeout = AOS_DNS_CACHE_TIMOUT;

	speed_limit = get_opt_oss(oss->url, "oss_speed_limit");
	speed_time = get_opt_oss(oss->url, "oss_speed_limit");
	dns_cache_timeout = get_opt_oss(oss->url, "oss_dns_cache_timeout");
	connect_timeout = get_opt_oss(oss->url, "oss_connect_timeout");
	if (speed_limit)
	{
		oss->ro.speed_limit = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(speed_limit)));
		pfree(speed_limit);
	}

	if (speed_time)
	{
		oss->ro.speed_time = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(speed_time)));
		pfree(speed_time);
	}

	if (dns_cache_timeout)
	{
		oss->ro.dns_cache_timeout = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(dns_cache_timeout)));
		pfree(dns_cache_timeout);
	}

	if (connect_timeout)
	{
		oss->ro.connect_timeout = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(connect_timeout)));
		pfree(connect_timeout);
	}

	if (oss->segindex == 0)
	{
		elog(DEBUG1, "oss request options: speed_limit %d K speed_time %d s dns_cache_timeout %d s connect_timeout %d s",
			oss->ro.speed_limit, oss->ro.speed_time, oss->ro.dns_cache_timeout, oss->ro.connect_timeout);
	}

	MemoryContextSwitchTo(old_ctx);

	return oss;
}

static void
free_data(ext_oss_t *myData)
{
	ListCell   *lc = NULL;

	if (myData->base.close)
	{
		myData->base.close(myData);
	}

	foreach(lc, myData->filelist)
	{
		oss_file   *file = (oss_file *) lfirst(lc);

		if (file && file->filename)
		{
			pfree(file->filename);
			file->filename = NULL;
		}
	}

	if (myData->filelist)
	{
		list_free_deep(myData->filelist);
		myData->filelist = NIL;
	}

	MemoryContextDelete(myData->ctx);
}

