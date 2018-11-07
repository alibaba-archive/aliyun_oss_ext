
#include "postgres.h"

#include "utils/guc.h"
#include "cdb/cdbvars.h"
#include "utils/timestamp.h"
#include "access/xact.h"

#include "ossapi.h"
#include "lib/oss_auth.h"
#include "lib/oss_util.h"
#include "lib/oss_xml.h"
#include "lib/oss_api.h"
#include "lib/oss_define.h"
#include "lib/aos_log.h"
#include "lib/aos_list.h"
#include "decompress_reader.h"

#ifdef HAVE_LONG_INT_64
#define int64_FMT			   "%ld"
#else
#define int64_FMT			   "%lld"
#endif

#define		OSS_CONTENT_LENGTH				"Content-Length"
#define		OSS_OBJECT_TYPE					"x-oss-object-type"
#define		OSS_OBJECT_TYPE_APPENDABLE		"Appendable"
#define		OSS_NEXT_APPEND_POSITION		"x-oss-next-append-position"
#define		OSS_ERROR_FILE_NOT_EXIST		404

#define OSS_RETRY_COUNT		30

#define MAX_RANGE_STR_LEN	64
#define MAX_RANGE_STR	"bytes="int64_FMT"-"int64_FMT""
#define ERROR_MESSAGE_LEN	1024

#define MSG_STR_MAX_LEN			1024

typedef struct oss_import_detail
{
	int64		lineno;
	char		ossfile[MSG_STR_MAX_LEN];
} oss_import_detail;

oss_import_detail	import_detail;

static aos_status_t *oss_get_file_metainfo(aos_pool_t * p, oss_request_options_t * options,
							aos_table_t ** resp_headers, aos_string_t bucket, aos_string_t object,
							bool async, char *msg);
static oss_request_options_t *oss_init_options(aos_pool_t * p, char *host, char *id, char *key, bool async, char *msg, oss_request_options ro);
static int oss_api_throw_exception(aos_pool_t *p, aos_status_t *s, char *object, int retrycount, bool async, char *msg, char *api);
static void set_oss_request_options(oss_request_options_t *options, oss_request_options ro);
static void set_oss_import_ossfile(char *ossfile);

static int
oss_api_throw_exception(aos_pool_t *p, aos_status_t *s, char *object, int retrycount, bool async, char *msg, char *api)
{
	int 	code = -1;
	char	*error_code = "unknown";
	char	*error_msg = "unknown";
	char	*req_id = "-1";

	if (s != NULL)
	{
		code = s->code;
		if (s->error_code)
			error_code = s->error_code;
		if (s->error_msg != NULL)
			error_msg = s->error_msg;
		if (s->req_id != NULL)
			req_id = s->req_id;
	}

	if (async)
	{
		snprintf(msg, ERROR_MESSAGE_LEN, "object %s %s failed: code %d error_code %s error_msg %s req_id %s, retry %d/%d",
			object, api, code, error_code, error_msg, req_id, retrycount, OSS_RETRY_COUNT);
		aos_pool_destroy(p);
		return 0;
	}
	else
	{
		elog(WARNING, "object %s %s failed: code %d error_code %s error_msg %s req_id %s, retry %d/%d",
			object, api, code, error_code, error_msg, req_id, retrycount, OSS_RETRY_COUNT);
		aos_pool_destroy(p);
		elog(ERROR, "ossapi %s call failure", api);
	}

	return 0;
}

void
oss_env_init(void)
{
	if (aos_http_io_initialize(NULL, 0) != AOSE_OK)
	{
		elog(ERROR, "aos_http_io_initialize failure.");
	}
	aos_log_set_level(AOS_LOG_OFF);

	return;
}

int64
oss_get_file_length(oss_connect *conn, char *filename, oss_request_options ro)
{
	aos_pool_t *p = NULL;
	aos_string_t bucket;
	aos_string_t object;
	oss_request_options_t *options = NULL;
	aos_status_t *s = NULL;
	aos_table_t *resp_headers = NULL;
	aos_table_t *headers = NULL;
	int64		filelength = 0;
	char	   *filestr;

	if (aos_pool_create(&p, NULL) != APR_SUCCESS)
	{
		elog(ERROR, "aos_pool_create failure.");
	}

	options = oss_init_options(p, conn->osshost, conn->ossid, conn->osskey, false ,NULL, ro);

	aos_str_set(&bucket, conn->bucket);
	aos_str_set(&object, filename);

	headers = aos_table_make(p, 0);
	if (headers == NULL)
	{
		aos_pool_destroy(p);
		elog(ERROR, "aos_table_make failure.");
	}

	s = oss_get_file_metainfo(p, options, &resp_headers, bucket, object, false, NULL);
	if (s != NULL && aos_status_is_ok(s))
	{
		filestr = (char *) apr_table_get(resp_headers, OSS_CONTENT_LENGTH);
		if (filestr == NULL)
		{
			aos_pool_destroy(p);
			elog(ERROR, "get ossfile length failure.");
		}
#ifdef _WIN64
		filelength = _atoi64(filestr);
#else
		filelength = atol(filestr);
#endif
	}
	else if (s != NULL && s->code == OSS_ERROR_FILE_NOT_EXIST)
	{
		filelength = -1;
		elog(DEBUG1, "ossfile %s does not exist.", filename);
	}

	aos_pool_destroy(p);

	return filelength;
}

size_t
oss_read_buffer(oss_connect *conn, char *filename, void *buffer, int64 offset, size_t len,
				bool async, char *msg, oss_request_options ro)
{
	aos_pool_t *p = NULL;
	aos_string_t bucket;
	aos_string_t object;
	oss_request_options_t *options = NULL;
	aos_status_t *s = NULL;
	aos_table_t *headers;
	aos_table_t *params = NULL;
	aos_table_t *resp_headers = NULL;
	aos_list_t	ossbuffers;
	aos_buf_t  *content;
	char	   *buf = (char *) buffer;
	int64		readlen = 0;
	int64		size = 0;
	int64		pos = 0;
	char		rangbuf[MAX_RANGE_STR_LEN] = {0};
	int			retrycount = 0;

	if (aos_pool_create(&p, NULL) != APR_SUCCESS)
	{
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_pool_create failure.");
			return 0;
		}
		else
		{
			elog(ERROR, "aos_pool_create failure.");
		}
	}

	options = oss_init_options(p, conn->osshost, conn->ossid, conn->osskey, async, msg, ro);
	if (async && options == NULL)
	{
		aos_pool_destroy(p);
		return 0;
	}

	aos_str_set(&bucket, conn->bucket);
	aos_str_set(&object, filename);

	headers = aos_table_make(p, 0);
	if (headers == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_table_make failure.");
			return 0;
		}
		else
		{
			elog(ERROR, "aos_table_make failure.");
		}
	}

	snprintf(rangbuf, MAX_RANGE_STR_LEN, MAX_RANGE_STR, offset, (int64) (offset + len - 1));
	apr_table_set(headers, "Range", rangbuf);

	aos_list_init(&ossbuffers);

retry_get_buffer:

	s = oss_get_object_to_buffer(options, &bucket, &object, headers, params, &ossbuffers, &resp_headers);
	if (s == NULL || !aos_status_is_ok(s))
	{
		if (aos_should_retry(s) == 1 && retrycount < OSS_RETRY_COUNT)
		{
			retrycount++;
			if (async == false)
			{
				elog(WARNING, "get ossfile %s oss_get_object_to_buffer time out, retry %d/%d", filename, retrycount, OSS_RETRY_COUNT);
			}
			goto retry_get_buffer;
		}
		else
		{
			return oss_api_throw_exception(p, s, filename, retrycount, async, msg, "oss_get_object_to_buffer");
		}
	}

	readlen = aos_buf_list_len(&ossbuffers);
	if (readlen > len || readlen <= 0)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "abnormal aos_buf_list_len offset " int64_FMT " len %d", offset, (int) len);
			return 0;
		}
		else
		{
			elog(ERROR, "abnormal aos_buf_list_len offset " int64_FMT " len %d", offset, (int) len);
		}
	}

	//copy buffer content to memory
	aos_list_for_each_entry(aos_buf_t, content, &ossbuffers, node)
	{
		size = aos_buf_size(content);
		memcpy(buf + pos, content->pos, (size_t) size);
		pos += size;
		if (pos > readlen)
		{
			aos_pool_destroy(p);
			if (async)
			{
				snprintf(msg, ERROR_MESSAGE_LEN, "abnormal copy buffer to local offset " int64_FMT " len %d", offset, (int) len);
				return 0;
			}
			else
			{
				elog(ERROR, "abnormal copy buffer to local offset " int64_FMT " len %d", offset, (int) len);
			}
		}
	}

	aos_pool_destroy(p);

	if (!async)
	{
		elog(DEBUG5, "read buffer from oss success. offset " int64_FMT " len %d", offset, (int) len);
	}

	return (size_t) readlen;
}

List *
list_ossfiles_ondir(oss_connect *conn, char *dir, oss_request_options ro, bool is_prefix)
{
	aos_pool_t *p = NULL;
	aos_string_t bucket;
	oss_request_options_t *options = NULL;
	aos_table_t *resp_headers = NULL;
	aos_status_t *s = NULL;
	oss_list_object_params_t *params_t = NULL;
	oss_list_object_content_t *content_t = NULL;
	int			retrycount = 0;
	char	   *filename = NULL;
	int			filenamestrlen = 0;
	List	   *filelist = NIL;
	int			truncated = 0;

	if (aos_pool_create(&p, NULL) != APR_SUCCESS)
	{
		elog(ERROR, "aos_pool_create failure.");
	}

	options = oss_init_options(p, conn->osshost, conn->ossid, conn->osskey, false, NULL, ro);

	params_t = oss_create_list_object_params(p);
	params_t->truncated = 0;

	aos_str_set(&params_t->prefix, dir);
	aos_str_set(&bucket, conn->bucket);

	if (!is_prefix)
	{
		aos_str_set(&params_t->delimiter, "/");
	}

	do
	{
		s = oss_list_object(options, &bucket, params_t, &resp_headers);
		if (NULL != s && aos_status_is_ok(s))
		{
			/* found */
			;
		}
		else if (NULL != s && s->code == OSS_ERROR_FILE_NOT_EXIST)
		{
			aos_pool_destroy(p);
			elog(DEBUG1, "ossdir %s does not exist.", dir);
			return 0;
		}
		else if (aos_should_retry(s) == 1 && retrycount < OSS_RETRY_COUNT)
		{
			retrycount++;
			elog(WARNING, "list ossdir %s use oss_get_object_to_buffer time out, retry %d/%d", dir, retrycount, OSS_RETRY_COUNT);
			continue;
		}
		else
		{
			oss_api_throw_exception(p, s, dir, retrycount, false, NULL, "oss_list_object");
			return NIL;
		}

		truncated = params_t->truncated;

		aos_list_for_each_entry(oss_list_object_content_t, content_t, &params_t->object_list, node)
		{
			oss_file   *ossfile = NULL;

			filenamestrlen = content_t->key.len;
			filename = content_t->key.data;	

			if (filename[filenamestrlen - 1] == '/')
			{
				continue;
			}

			ossfile = (oss_file *) palloc(sizeof(oss_file));
			ossfile->filename = pstrdup(filename);
			ossfile->length = 0;
			filelist = lappend(filelist, ossfile);
		}

		resp_headers = NULL;
		if (params_t->truncated == 1)
		{
			aos_list_init(&params_t->object_list);
			aos_str_set(&params_t->marker, filename);
			params_t->truncated = 0;
		}
	} while (truncated == 1);

	aos_pool_destroy(p);

	return filelist;
}

bool
is_ossfile_exist(oss_connect *conn, char *filename, oss_request_options ro)
{
	bool		exist = false;
	aos_pool_t *p = NULL;
	aos_string_t bucket;
	aos_string_t object;
	aos_status_t *s = NULL;
	aos_table_t *resp_headers = NULL;
	oss_request_options_t *options = NULL;

	if (aos_pool_create(&p, NULL) != APR_SUCCESS)
	{
		elog(ERROR, "aos_pool_create failure.");
	}

	options = oss_init_options(p, conn->osshost, conn->ossid, conn->osskey, false, NULL, ro);

	aos_str_set(&bucket, conn->bucket);
	aos_str_set(&object, filename);

	s = oss_get_file_metainfo(p, options, &resp_headers, bucket, object, false, NULL);
	if (s != NULL && aos_status_is_ok(s))
	{
		exist = true;
	}
	else if (s != NULL && s->code == OSS_ERROR_FILE_NOT_EXIST)
	{
		exist = false;
	}

	aos_pool_destroy(p);

	return exist;
}

bool
oss_append_file_from_buffer(oss_connect *conn, char *filename, char *data, size_t len,
								bool checktype, int64 append_position, oss_request_options ro,
								bool async, char *msg)
{
	aos_pool_t *p = NULL;
	aos_string_t bucket;
	aos_string_t object;
	aos_status_t *s = NULL;
	int64		position = 0;
	aos_table_t *headers2 = NULL;
	aos_table_t *resp_headers = NULL;
	oss_request_options_t *options = NULL;
	aos_list_t	buffer;
	aos_buf_t  *content = NULL;
	char	   *next_append_position = NULL;
	char	   *object_type = NULL;
	int			retrycount = 0;

	if (aos_pool_create(&p, NULL) != APR_SUCCESS)
	{
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_pool_create failure.");
			return false;
		}
		else
		{
			elog(ERROR, "aos_pool_create failure.");
		}
	}

	options = oss_init_options(p, conn->osshost, conn->ossid, conn->osskey, async, msg, ro);
	if (async && options == NULL)
	{
		aos_pool_destroy(p);
		return false;
	}

	aos_str_set(&bucket, conn->bucket);
	aos_str_set(&object, filename);

	s = oss_get_file_metainfo(p, options, &resp_headers, bucket, object, async, msg);
	if (s != NULL && aos_status_is_ok(s))
	{
		object_type = (char *) (apr_table_get(resp_headers, OSS_OBJECT_TYPE));
		if (checktype && 0 != strncmp(OSS_OBJECT_TYPE_APPENDABLE, object_type, strlen(OSS_OBJECT_TYPE_APPENDABLE)))
		{
			aos_pool_destroy(p);
			if (async)
			{
				snprintf(msg, ERROR_MESSAGE_LEN, "object[%s]'s type[%s] is not Appendable", filename, object_type);
				return false;
			}
			else
			{
				elog(ERROR, "object[%s]'s type[%s] is not Appendable", filename, object_type);
			}
		}

		if (append_position == 0)
		{
			next_append_position = (char *) (apr_table_get(resp_headers, OSS_NEXT_APPEND_POSITION));
#ifdef WIN32
			position = atol(next_append_position);
#else
			position = atoll(next_append_position);
#endif
		}
		else
		{
			position = append_position;
		}
	}
	else if (s != NULL && s->code == OSS_ERROR_FILE_NOT_EXIST)
	{
		;
	}
	else
	{
		if (async)
		{
			if (msg[0] == 0)
			{
				snprintf(msg, ERROR_MESSAGE_LEN, "oss_get_file_metainfo failure.");
			}
			return false;
		}
		else
		{
			elog(ERROR, "aos_table_make failure.");
		}
	}

	headers2 = aos_table_make(p, 0);
	if (headers2 == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_table_make failure.");
			return false;
		}
		else
		{
			elog(ERROR, "aos_table_make failure.");
		}
	}

	aos_list_init(&buffer);
	content = aos_buf_pack(p, data, len);
	if (content == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_buf_pack failure.");
			return false;
		}
		else
		{
			elog(ERROR, "aos_buf_pack failure.");
		}
	}
	aos_list_add_tail(&content->node, &buffer);

retry_loaddata:

	s = oss_append_object_from_buffer(options, &bucket, &object,
								 position, &buffer, headers2, &resp_headers);
	if (s != NULL || aos_status_is_ok(s))
	{
		;
	}
	else if (aos_should_retry(s) == 1 && retrycount < OSS_RETRY_COUNT)
	{
		retrycount++;
		if (async == false)
		{
			elog(WARNING, "oss_append_object_from_buffer time out, filename %s, retry %d/%d", filename, retrycount, OSS_RETRY_COUNT);
		}
		goto retry_loaddata;
	}
	else
	{
		return oss_api_throw_exception(p, s, filename, retrycount, async, msg, "oss_append_object_from_buffer");
	}

	aos_pool_destroy(p);

	return true;
}

static aos_status_t *
oss_get_file_metainfo(aos_pool_t *p, oss_request_options_t * options,
				aos_table_t ** resp_headers, aos_string_t bucket, aos_string_t object,
				bool async, char *msg)
{
	aos_status_t *s = NULL;
	aos_table_t *headers = NULL;
	int			retrycount = 0;

	if (p == NULL || options == NULL)
	{
		return NULL;
	}

	headers = aos_table_make(p, 0);
	if (headers == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_pool_create failure.");
			return NULL;
		}
		else
		{
			elog(ERROR, "aos_table_make failure.");
		}
	}

retry_getmetainfo:

	s = oss_head_object(options, &bucket, &object, headers, resp_headers);
	if (NULL != s && aos_status_is_ok(s))
	{
		;
	}
	else if (NULL != s && s->code == OSS_ERROR_FILE_NOT_EXIST)
	{
		;
	}
	else if (aos_should_retry(s) == 1 && retrycount < OSS_RETRY_COUNT)
	{
		retrycount++;
		if (async == false)
		{
			elog(WARNING, "get ossfile %s oss_head_object time out, retry %d/%d", object.data, retrycount, OSS_RETRY_COUNT);
		}
		goto retry_getmetainfo;
	}
	else
	{
		oss_api_throw_exception(p, s, object.data, retrycount, async, msg, "oss_head_object");
		return NULL;
	}

	return s;
}

static oss_request_options_t *
oss_init_options(aos_pool_t * p, char *host, char *id, char *key, bool async, char *msg, oss_request_options ro)
{
	oss_request_options_t *options = NULL;

	options = oss_request_options_create(p);
	if (options == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "oss_request_options_create failure.");
			return NULL;
		}
		else
		{
			elog(ERROR, "oss_request_options_create failure.");
		}
	}
	options->config = oss_config_create(options->pool);
	if (options->config == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "oss_config_create failure.");
			return NULL;
		}
		else
		{
			elog(ERROR, "oss_config_create failure.");
		}
	}
	aos_str_set(&options->config->endpoint, host);
	aos_str_set(&options->config->access_key_id, id);
	aos_str_set(&options->config->access_key_secret, key);
	options->config->is_cname = 0;
	options->ctl = aos_http_controller_create(options->pool, 0);
	if (options->ctl == NULL)
	{
		aos_pool_destroy(p);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "aos_http_controller_create failure.");
			return NULL;
		}
		else
		{
			elog(ERROR, "aos_http_controller_create failure.");
		}
	}

	set_oss_request_options(options, ro);

	return options;
}

static void
set_oss_request_options(oss_request_options_t *options, oss_request_options ro)
{
	if (options == NULL || options->ctl == NULL || options->ctl->options == NULL)
		return;

	options->ctl->options->speed_limit = ro.speed_limit;
	options->ctl->options->speed_time = ro.speed_time;
	options->ctl->options->connect_timeout = ro.connect_timeout;
	options->ctl->options->dns_cache_timeout = ro.dns_cache_timeout;

	return;
}

size_t
SourceRead(ext_oss_t *self, void *buffer, size_t len)
{
	return SourceRead_internal(self, buffer, len, true, false, NULL);
}

size_t
SourceRead_internal(ext_oss_t *myData, void *buffer, size_t len,
				bool auto_next_file, bool async, char *msg)
{
	size_t		datlen = 0;
	size_t		nread = 0;
	int64		offset = 0;
	char		*data = (char *)buffer;

retry:
	datlen = len;

	if (myData->length < 0)
	{
		return 0;
	}
	else if (myData->length == 0)
	{
		if (auto_next_file)
		{
			oss_next_file(myData);
			goto retry;
		}
		else
		{
			return 0;
		}
	}
	else if (myData->offset + datlen > myData->length)
	{
		datlen = myData->length - myData->offset;
	}

	if (datlen == 0)
	{
		if (auto_next_file)
		{
			oss_next_file(myData);
			goto retry;
		}
		else
		{
			return 0;
		}
	}

	offset = myData->offset;
	nread = oss_read_buffer(&myData->conn, myData->currentfile, data, offset, datlen, async, msg, myData->ro);

	if (nread < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
					 errmsg("oss_import: could not read file \"%s\"", myData->currentfile)));
	}

	myData->offset += nread;

	return nread;
}

void
SourceClose(void *selfp)
{
	ext_oss_t *self = (ext_oss_t *)selfp;

	if (self->file_opt.type == OSS_COMPRESSION_GZIP)
	{
		z_decompress_reader_destroy((z_decompress_reader *)(self->com_hd));
	}
}

void
oss_next_file(ext_oss_t * myData)
{
	ListCell   *l;

	if (myData->currentfile)
	{
		pfree(myData->currentfile);
		myData->currentfile = NULL;
		myData->offset = 0;
	}

	myData->length = -1;

	if (list_length(myData->filelist) > 0)
	{
		oss_file   *file = NULL;

		l = list_head(myData->filelist);
		file = (oss_file *) lfirst(l);

		myData->filelist = list_delete_ptr(myData->filelist, file);
		myData->currentfile = file->filename;
		myData->length = file->length;
		myData->offset = 0;

		set_oss_import_ossfile(myData->currentfile);

		pfree(file);
	}

	return;
}

void
oss_wirte_next_file(ext_oss_t *myData)
{
	char		currentfile[OSS_MAX_FILE_PATH] = {0};
	TimestampTz masterTime = 0;

	masterTime = GetCurrentStatementStartTimestamp();
	if (myData->fileindex == 0 && myData->segindex == 0)
	{
		snprintf(currentfile, OSS_MAX_FILE_PATH - 1, "%s%s_" int64_FMT,
					myData->file_opt.ossprefix, myData->export_relname, masterTime);
	}
	else
	{
		snprintf(currentfile, OSS_MAX_FILE_PATH - 1, "%s%s_" int64_FMT ".%d",
					myData->file_opt.ossprefix, myData->export_relname, masterTime,
					(myData->fileindex * myData->numsegments + myData->segindex));
	}

	myData->fileindex++;

	if (myData->currentfile)
	{
		pfree(myData->currentfile);
	}

	myData->currentfile = pstrdup(currentfile);

	if (is_ossfile_exist(&myData->conn, myData->currentfile, myData->ro))
	{
		elog(ERROR, "file %s exists, write process aborts", myData->currentfile);
	}
}

static void
set_oss_import_ossfile(char *ossfile)
{
	if (ossfile == NULL)
	{
		return;
	}

	snprintf(import_detail.ossfile, MSG_STR_MAX_LEN, "%s", ossfile);
	import_detail.lineno = 0;

	return;
}
