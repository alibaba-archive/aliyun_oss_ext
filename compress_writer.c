
#include "postgres.h"

#include "ossapi.h"
#include "compress_writer.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "utils/builtins.h"

#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils/guc.h"
#include "access/fileam.h"
#include "port.h"

extern char my_exec_path[];

#define MAX_PIPE_BUFFER_LEN			1024
#define MAX_OSS_OBJECT_NAME_LEN		4096
#define MAX_OSS_STR_LEN				128
#define	MAX_COMPRESS_BUFFER_LEN		8192

#define PIPE_READ							0
#define PIPE_WRITE							1

int		aStdinPipe[2] = {-1, -1};
int		aStdoutPipe[2] = {-1, -1};
int		aStderrPipe[2] = {-1, -1};

oss_request_options oss_ro = {AOS_MIN_SPEED_LIMIT,
							AOS_MIN_SPEED_TIME, 
							AOS_DNS_CACHE_TIMOUT, 
							AOS_CONNECT_TIMEOUT};

oss_exp_options oss_oe = {OSS_FLUSH_BUF_DEFAULT_SIZE, 
							OSS_WRITE_FILE_DEFAULT_SIZE,
							OSS_DEFAULT_COMPRESS_THREAD_NUM,
							false,
							DEFAULT_PIPE_BLOCK_SIZE,
							DEFAULT_OSS_COMPRESS_LEVEL};

char	oss_host[MAX_OSS_STR_LEN] = {0};
char	oss_id[MAX_OSS_STR_LEN] = {0};
char	oss_key[MAX_OSS_STR_LEN] = {0};
char	oss_bucket[MAX_OSS_STR_LEN] = {0};
char	oss_file_name[MAX_OSS_OBJECT_NAME_LEN] = {0};

char	oss_write_error_msg[ERROR_MESSAGE_LEN];

char	*oss_write_buffer = NULL;
char	*oss_write_temp = NULL;

#ifdef WIN32
#define		THREAD_PID_NULL		NULL
pthread_t	pid_compress = THREAD_PID_NULL;
pthread_t	th_writer = THREAD_PID_NULL;
#else
#define		THREAD_PID_NULL		-1
pid_t		pid_compress = THREAD_PID_NULL;
pthread_t	th_writer = THREAD_PID_NULL;
#endif

static volatile int	oss_writer_init = false;
static volatile int	oss_writer_exit_witherr = false;

static size_t compress_write(void *selfp, void *buffer, size_t request_len);
static void compress_writer_close(void *selfp);
static int start_subprocess_open_pipe(int nthread, int compression_level);
static void reset_global_variable(void);
static void *oss_write_main(void *arg);
static void shutdown_compress_main_env(void);
static void shutdown_write_thread(void);
static void write_buffer_to_pipe(ext_oss_t *myData);
static bool file_exists(const char *name);

#ifndef WIN32
/* Non-Windows implementation of pipe access */
#define pgpipe(a)			pipe(a)
#define piperead(a,b,c)		read(a,b,c)
#define pipewrite(a,b,c)	write(a,b,c)
#define pipeclose(a)		close(a)
#endif

int
init_compress_writer(ext_oss_t *self, bool init_subprocess)
{
	int	rc = -1;

	self->base.write = (SourceWriteProc) compress_write;
	self->base.close = (SourceCloseProc) compress_writer_close;

	self->size = self->write_opt.flush_block;

	if (init_subprocess)
	{
		rc = start_subprocess_open_pipe(self->write_opt.nthread, self->write_opt.compression_level);
		if (rc < 0)
		{
			shutdown_compress_main_env();
			elog(ERROR, "oss compresser subprocess start fail");
		}

		if (pthread_create(&th_writer, NULL, oss_write_main, (void *)self) != 0)
		{
			shutdown_compress_main_env();
			elog(ERROR, "oss writer thread start fail");
		}
		while(oss_writer_init == false)
		{
			pg_usleep(100L);
		}
		oss_writer_init = false;
	}

	self->errmsg[0] = 0;
	if (self->buffer == NULL)
	{
		self->buffer = palloc(self->write_opt.pipe_block_size);
		memset(self->buffer, 0, self->write_opt.pipe_block_size);
	}

	if (self->buffer == NULL)
	{
		shutdown_compress_main_env();
		elog(ERROR, "out of memory for pipe buffer");
	}

	return 0;
}

static void
shutdown_compress_main_env(void)
{
	int 		status;
	int 		r;

	if (aStdinPipe[PIPE_WRITE] != -1)
	{
		elog(DEBUG1, "close aStdinPipe writer");
		pipeclose(aStdinPipe[PIPE_WRITE]);
		aStdinPipe[PIPE_WRITE] = -1;
	}

#ifndef WIN32
	if (pid_compress > 0)
	{
		elog(DEBUG1, "wait compross %d exit", pid_compress);

		/* Just wait for the background process to exit */
		r = waitpid(pid_compress, &status, 0);
		if (r == -1)
		{
			elog(ERROR, "could not wait for child process: %s", strerror(errno));
		}
		if (r != pid_compress)
		{
			elog(ERROR, "child %d died, expected %d", r, pid_compress);
		}
		if (!WIFEXITED(status))
		{			
			pid_compress = -1;
			elog(ERROR, "child process did not exit normally");
		}
		if (WEXITSTATUS(status) != 0)
		{
			char msg[MAXPGPATH];

			snprintf(msg, MAXPGPATH, "unknown");
			if (aStderrPipe[PIPE_READ] != -1)
			{
				piperead(aStderrPipe[PIPE_READ], msg, MAXPGPATH);
			}
			pid_compress = -1;
			pipeclose(aStderrPipe[PIPE_READ]);
			aStderrPipe[PIPE_READ] = -1;
			elog(ERROR, "child process exited with error %d %s", WEXITSTATUS(status), msg);	
		}
		pid_compress = -1;
	}
#else
	if (pid_compress != THREAD_PID_NULL)
	{
		pthread_join(pid_compress, THREAD_PID_NULL);
		pid_compress = THREAD_PID_NULL;
	}
#endif

	if (th_writer != THREAD_PID_NULL)
	{
		pthread_join(th_writer, NULL);
		th_writer = THREAD_PID_NULL;
	}

	if (aStdoutPipe[PIPE_READ] != -1)
	{
		elog(WARNING, "aStdoutPipe reader not close");
		pipeclose(aStdoutPipe[PIPE_READ]);
		aStdoutPipe[PIPE_READ] = -1;
	}

	if (aStderrPipe[PIPE_READ] != -1)
	{
		elog(DEBUG1, "close aStderrPipe reader");
		pipeclose(aStderrPipe[PIPE_READ]);
		aStderrPipe[PIPE_READ] = -1;
	}

	aStdinPipe[PIPE_READ] = -1;
	aStdoutPipe[PIPE_WRITE] = -1;
	aStderrPipe[PIPE_WRITE] = -1;
	
	oss_writer_init = false;
	oss_writer_exit_witherr = false;

	oss_write_error_msg[0] = 0;
}

static size_t
compress_write(void *selfp, void *buffer, size_t request_len)
{
	ext_oss_t *myData = (ext_oss_t *) selfp;

	if (request_len <= 0)
	{
		return 0;
	}

	if (oss_writer_exit_witherr)
	{
		snprintf(myData->errmsg, ERROR_MESSAGE_LEN, "%s", oss_write_error_msg);
		shutdown_compress_main_env();
		elog(ERROR, "%s", myData->errmsg);
	}

	if (request_len > myData->write_opt.pipe_block_size)
	{		
		shutdown_compress_main_env();
		elog(ERROR, "meet one big row, please increase pipe_block_size");
	}

	myData->write_row_count++;
	myData->write_byte_count += request_len;

	if ((myData->file_flush_offset + request_len) > myData->write_opt.file_max_size)
	{
		elog(DEBUG1, "switch oss file");
		write_buffer_to_pipe(myData);
		shutdown_compress_main_env();
		oss_wirte_next_file(myData);
		init_compress_writer(myData, true);
		myData->file_flush_offset = 0;
	}

	if ((myData->offset + request_len) > myData->write_opt.pipe_block_size)
	{
		write_buffer_to_pipe(myData);
	}

	memcpy(myData->buffer + myData->offset, buffer, request_len);
	myData->offset += request_len;
	myData->file_flush_offset += request_len;

	return request_len;
}

static void
write_buffer_to_pipe(ext_oss_t *myData)
{
	int rc;
	TimevalStruct   before, after;
	double			elapsed_msec = 0;

	if (myData->offset <= 0)
	{
		return;
	}

	GETTIMEOFDAY(&before);
	rc = pipewrite(aStdinPipe[PIPE_WRITE], myData->buffer, myData->offset);
	if (rc < 0)
	{
		snprintf(myData->errmsg, ERROR_MESSAGE_LEN, "oss compress write to pipe fail %d %s", errno, strerror(errno));
		shutdown_compress_main_env();
		elog(ERROR, "%s", myData->errmsg);
	}

	myData->offset = 0;

	GETTIMEOFDAY(&after);
	DIFF_MSEC(&after, &before, elapsed_msec);
	myData->flush_data_timer += elapsed_msec;

	return;
}

static void
compress_writer_close(void *selfp)
{
	ext_oss_t *myData = (ext_oss_t *) selfp;

	if (myData)
	{
		if (oss_writer_exit_witherr)
		{
			snprintf(myData->errmsg, ERROR_MESSAGE_LEN, "%s", oss_write_error_msg);
			shutdown_compress_main_env();
			elog(ERROR, "%s", myData->errmsg);
		}

		write_buffer_to_pipe(myData);
		shutdown_compress_main_env();

		elog(DEBUG1, "oss compress end, wrote row " int64_FMT ", " int64_FMT " byte cost %.3f ms",
			myData->write_row_count, myData->write_byte_count, myData->flush_data_timer);

		//MemoryContextDelete(myData->wstate_cxt);
		//myData->wstate = NULL;
	}
}

static int
start_subprocess_open_pipe(int nthread, int compression_level)
{
	pid_t	pid = -1;
	char	str_nthread[16];
	char	str_com_level[16];
	char	pigz_exec_dir[MAXPGPATH];
	char	pigz_exec_path[MAXPGPATH];

	snprintf(pigz_exec_dir, MAXPGPATH, "%s", my_exec_path);
	get_parent_directory(pigz_exec_dir);
	snprintf(pigz_exec_path, MAXPGPATH, "%s/pigz", pigz_exec_dir);
	elog(DEBUG1, "pigz_exec_path %s", pigz_exec_path);
	if (!file_exists(pigz_exec_path))
	{
		elog(ERROR, "%s not exist", pigz_exec_path);
	}

	memset(str_nthread, 0, 16);
	memset(str_com_level, 0, 16);
	pg_ltoa(nthread, str_nthread);
	snprintf(str_com_level, 16, "-%d", compression_level);

	reset_global_variable();

	if (pgpipe(aStdinPipe) < 0)
	{
		elog(ERROR, "allocating stdin pipe fail");
		return -1;
	}

	if (pgpipe(aStdoutPipe) < 0)
	{
		pipeclose(aStdinPipe[PIPE_READ]);
		pipeclose(aStdinPipe[PIPE_WRITE]);
		aStdinPipe[PIPE_READ] = -1;
		aStdinPipe[PIPE_WRITE] = -1;
		elog(ERROR, "allocating stdout pipe fail");
		return -1;
	}

	if (pgpipe(aStderrPipe) < 0)
	{
		pipeclose(aStdinPipe[PIPE_READ]);
		pipeclose(aStdinPipe[PIPE_WRITE]);
		pipeclose(aStdinPipe[PIPE_READ]);
		pipeclose(aStdinPipe[PIPE_WRITE]);
		aStdinPipe[PIPE_READ] = -1;
		aStdinPipe[PIPE_WRITE] = -1;
		aStdinPipe[PIPE_READ] = -1;
		aStdinPipe[PIPE_WRITE] = -1;
		elog(ERROR, "allocating stderr pipe fail");
		return -1;
	}

#ifndef WIN32

	switch ((pid=fork())) 
	{
		case 0:
		{
			
			char *const ps_argv[] ={"pigz", "-p", str_nthread, str_com_level, "-f", NULL};

			if (dup2(aStdinPipe[PIPE_READ], STDIN_FILENO) == -1) {
				exit(errno);
			}

			// redirect stdout
			if (dup2(aStdoutPipe[PIPE_WRITE], STDOUT_FILENO) == -1) {
				exit(errno);
			}

			// redirect stderr
			if (dup2(aStderrPipe[PIPE_WRITE], STDERR_FILENO) == -1) {
				exit(errno);
			}

			// all these are for use by parent only
			pipeclose(aStdinPipe[PIPE_WRITE]);
			pipeclose(aStdoutPipe[PIPE_READ]);
			pipeclose(aStderrPipe[PIPE_READ]);

			if (execv(pigz_exec_path, ps_argv) < 0)
			{
				fprintf(stderr, "could not execute pigz\n");
				exit(1);
			}

			exit(0);
		}
		break;

		case -1:
		{
			pipeclose(aStdinPipe[PIPE_READ]);
			pipeclose(aStdinPipe[PIPE_WRITE]);
			pipeclose(aStdoutPipe[PIPE_READ]);
			pipeclose(aStdoutPipe[PIPE_WRITE]); 
			pipeclose(aStderrPipe[PIPE_READ]);
			pipeclose(aStderrPipe[PIPE_WRITE]);
			aStdinPipe[PIPE_READ] = -1;
			aStdinPipe[PIPE_WRITE] = -1;
			aStdoutPipe[PIPE_READ] = -1;
			aStdoutPipe[PIPE_WRITE] = -1;
			aStderrPipe[PIPE_READ] = -1;
			aStderrPipe[PIPE_WRITE] = -1;
			return -5;
		}
		break;

		default:
		{
			// parent process
			pid_compress = pid;

			pipeclose(aStdinPipe[PIPE_READ]);
			pipeclose(aStdoutPipe[PIPE_WRITE]);
			pipeclose(aStderrPipe[PIPE_WRITE]);

			aStdinPipe[PIPE_READ] = -1;
			aStdoutPipe[PIPE_WRITE] = -1;
			aStderrPipe[PIPE_WRITE] = -1;
			return 0;
		}
		break;
	}
#else

	/* for debug */
	if (pthread_create(&pid_compress, NULL, oss_compress_main, NULL) != 0)
		elog(ERROR, "pthread_create");

#endif

	return 0;
}

static void
reset_global_variable(void)
{
	if (aStdinPipe[PIPE_READ] != -1)
	{
		pipeclose(aStdinPipe[PIPE_READ]);
		aStdinPipe[PIPE_READ] = -1;
	}

	if (aStdinPipe[PIPE_WRITE] != -1)
	{
		pipeclose(aStdinPipe[PIPE_WRITE]);
		aStdinPipe[PIPE_WRITE] = -1;
	}

	if (aStdoutPipe[PIPE_READ] != -1)
	{
		pipeclose(aStdoutPipe[PIPE_READ]);
		aStdoutPipe[PIPE_READ] = -1;
	}

	if (aStdoutPipe[PIPE_WRITE] != -1)
	{
		pipeclose(aStdoutPipe[PIPE_WRITE]);
		aStdoutPipe[PIPE_WRITE] = -1;
	}

	if (aStderrPipe[PIPE_READ] != -1)
	{
		pipeclose(aStderrPipe[PIPE_READ]);
		aStderrPipe[PIPE_READ] = -1;
	}

	if (aStderrPipe[PIPE_WRITE] != -1)
	{
		pipeclose(aStderrPipe[PIPE_WRITE]);
		aStderrPipe[PIPE_WRITE] = -1;
	}

	if (pid_compress != THREAD_PID_NULL)
	{
#ifndef WIN32
		int 		status;
		int 		r;

		r = waitpid(pid_compress, &status, 0);
#endif
		elog(WARNING, "oss compress process does not close");
		pid_compress = THREAD_PID_NULL;
	}

	if (th_writer != THREAD_PID_NULL)
	{
		elog(WARNING, "oss compres thread does not close");
		th_writer = THREAD_PID_NULL;
	}

	oss_writer_init = false;
	oss_writer_exit_witherr = false;
}

static void * 
oss_write_main(void *arg)
{
	ext_oss_t *wstate = (ext_oss_t *)arg;
	int 	rlen = 0;
	int 	buffer_size = wstate->size;
	oss_connect		conn;
	int		offset = 0;
	bool	exit_with_error = true;

	snprintf(oss_host, MAX_OSS_STR_LEN, "%s", wstate->conn.osshost);
	snprintf(oss_id, MAX_OSS_STR_LEN, "%s", wstate->conn.ossid);
	snprintf(oss_key, MAX_OSS_STR_LEN, "%s", wstate->conn.osskey);
	snprintf(oss_bucket, MAX_OSS_STR_LEN, "%s", wstate->conn.bucket);
	snprintf(oss_file_name, MAX_OSS_OBJECT_NAME_LEN, "%s", wstate->currentfile); 
	oss_ro = wstate->ro;
	oss_oe = wstate->write_opt;
	conn.osshost = oss_host;
	conn.ossid = oss_id;
	conn.osskey = oss_key;
	conn.bucket = oss_bucket;
	oss_write_error_msg[0] = 0;

	if (oss_write_buffer)
	{
		free(oss_write_buffer);
		oss_write_buffer = NULL;
	}
	oss_write_buffer = malloc(buffer_size);
	if (oss_write_buffer == NULL)
	{
		snprintf(oss_write_error_msg, ERROR_MESSAGE_LEN, "oss write thread out of memory");
		goto oss_write_err;
	}

	if (oss_write_temp)
	{
		free(oss_write_temp);
		oss_write_temp = NULL;
	}
	oss_write_temp = malloc(buffer_size);
	if (oss_write_temp == NULL)
	{
		snprintf(oss_write_error_msg, ERROR_MESSAGE_LEN, "oss write thread out of memeory");
		goto oss_write_err;
	}

	oss_writer_init = true;
	while(1)
	{
		rlen = piperead(aStdoutPipe[PIPE_READ], oss_write_temp, buffer_size);
		if (rlen > 0)
		{
			bool	rc = 0;

			if (offset + rlen > buffer_size)
			{
				rc = oss_append_file_from_buffer(&conn, oss_file_name, oss_write_buffer,
											offset, false, 0, oss_ro, true, oss_write_error_msg);
				if (rc == false)
				{
					goto oss_write_err;
				}
				memset(oss_write_buffer, 0, buffer_size);
				offset = 0;
			}

			memcpy(oss_write_buffer + offset, oss_write_temp, rlen);
			offset += rlen;
		}
		else
		{
			if(rlen < 0)
			{
				snprintf(oss_write_error_msg, ERROR_MESSAGE_LEN, "oss compress read from pipe fail %d %s", errno, strerror(errno));
				goto oss_write_err;
			}
			pipeclose(aStdoutPipe[PIPE_READ]);
			aStdoutPipe[PIPE_READ] = -1;
			break;
		}
	}

	exit_with_error = false;

	if (offset > 0)
	{
		bool rc = oss_append_file_from_buffer(&conn, oss_file_name, oss_write_buffer,
								offset, false, 0, oss_ro, true, oss_write_error_msg);
		if (rc == false)
		{
			exit_with_error = true;
		}
		offset = 0;
	}

oss_write_err:

	oss_writer_exit_witherr = exit_with_error;
	shutdown_write_thread();
	return NULL;
}

static void
shutdown_write_thread(void)
{
	oss_host[0] = 0;
	oss_id[0] = 0;
	oss_key[0] = 0;
	oss_bucket[0] = 0;
	oss_file_name[0] = 0;

	if (oss_write_buffer)
	{
		free(oss_write_buffer);
		oss_write_buffer = NULL;
	}

	if (oss_write_temp)
	{
		free(oss_write_temp);
		oss_write_temp = NULL;
	}

	oss_ro.speed_limit = AOS_MIN_SPEED_LIMIT;
	oss_ro.speed_time = AOS_MIN_SPEED_TIME;
	oss_ro.dns_cache_timeout = AOS_DNS_CACHE_TIMOUT;
	oss_ro.connect_timeout = AOS_CONNECT_TIMEOUT;

	oss_oe.flush_block = OSS_FLUSH_BUF_DEFAULT_SIZE;
	oss_oe.file_max_size = OSS_WRITE_FILE_DEFAULT_SIZE;
	oss_oe.nthread = OSS_DEFAULT_COMPRESS_THREAD_NUM;
	oss_oe.async = false;
	oss_oe.pipe_block_size = DEFAULT_PIPE_BLOCK_SIZE;
	oss_oe.compression_level = DEFAULT_OSS_COMPRESS_LEVEL;
}

static bool
file_exists(const char *name)
{
	struct stat st;

	AssertArg(name != NULL);

	if (stat(name, &st) == 0)
		return S_ISDIR(st.st_mode) ? false : true;
	else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not access file \"%s\": %m", name)));

	return false;
}

