#include "postgres.h"

#include "ossapi.h"
#include "decompress_reader.h"

/*
 * For inflate, windowBits can be greater than 15 for optional gzip decoding. Add 32 to windowBits
 * to enable zlib and gzip decoding with automatic header detection.
*/
#define OSS_INFLATE_WINDOWSBITS (MAX_WBITS + 16 + 16)

const char *str_oss_compression[] = 
{
	"text",
	"gzip",
	"zlib",
	"unknown",
	NULL
};

uint64 OSS_ZIP_DECOMPRESS_CHUNKSIZE = OSS_ZIP_DEFAULT_CHUNKSIZE;

#define		GZIP_MAGIC_BLOCK		"\x1f\x8b\x08"
#define		BZ2_MAGIC_BLOCK		"\x42\x5a\x68"

static uint64 get_z_decompressed_bytes_num(z_decompress_reader *reader);
static void z_decompress(OssHander	*myData, z_decompress_reader *reader, bool async, char *msg);

z_decompress_reader *
init_z_decompress_reader(void)
{
	z_decompress_reader *reader = palloc(sizeof(z_decompress_reader));

	reader->in = palloc(OSS_ZIP_DECOMPRESS_CHUNKSIZE);
	reader->out = palloc(OSS_ZIP_DECOMPRESS_CHUNKSIZE);
	if (reader->in == NULL || reader->out == NULL)
	{
		elog(ERROR, "create decompress buffer out of memory");
	}

	memset(reader->in, 0, OSS_ZIP_DECOMPRESS_CHUNKSIZE);
	memset(reader->out, 0, OSS_ZIP_DECOMPRESS_CHUNKSIZE);
	
	reader->outOffset = 0;
	return reader;
}

void 
z_decompress_reader_destroy(z_decompress_reader *reader)
{
	inflateEnd(&reader->zstream);

	pfree(reader->in);
	pfree(reader->out);
	pfree(reader);

	return;
}

void 
z_decompress_reader_open(z_decompress_reader *reader, bool async, char *msg) 
{
	int ret = 0;

    /* allocate inflate state for zlib */
    reader->zstream.zalloc = Z_NULL;
    reader->zstream.zfree = Z_NULL;
    reader->zstream.opaque = Z_NULL;
    reader->zstream.next_in = Z_NULL;
    reader->zstream.next_out = (Byte *)reader->out;

    reader->zstream.avail_in = 0;
    reader->zstream.avail_out = OSS_ZIP_DECOMPRESS_CHUNKSIZE;

    reader->outOffset = 0;

    /* with OSS_INFLATE_WINDOWSBITS, it could recognize and decode both zlib and gzip stream. */
	ret = inflateInit2(&reader->zstream, OSS_INFLATE_WINDOWSBITS);
	if (ret != Z_OK)
	{
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "failed to initialize zlib library");
		}
		else
		{
			elog(ERROR, "failed to initialize zlib library");
		}
	}

	return;
}

size_t
z_decompress_read(OssSource *self, void *buf, size_t bufSize)
{
	z_decompress_reader *com_hd = self->com_hd;
	return z_decompress_internal(self, com_hd, buf, bufSize, false, NULL);
}

size_t
z_decompress_internal(OssHander	*myData, z_decompress_reader *com_hd, void *buf,
								size_t bufSize, bool async, char *msg)
{
	uint64 remainingOutLen = get_z_decompressed_bytes_num(com_hd) - com_hd->outOffset;
	uint64 count = 0;

	if (remainingOutLen == 0)
	{
		z_decompress(myData, com_hd, async, msg);
		if (async && msg[0] != '\0')
		{
			return 0;
		}
		com_hd->outOffset = 0;
		remainingOutLen = get_z_decompressed_bytes_num(com_hd);
	}

	count = Min(remainingOutLen, bufSize);
	memcpy(buf, com_hd->out + com_hd->outOffset, count);
	com_hd->outOffset += count;

	return count;
}

/*
 * Read compressed data from underlying reader and decompress to this->out buffer.
 * If no more data to consume, this->zstream.avail_out == OSS_ZIP_DECOMPRESS_CHUNKSIZE;
 */
static void 
z_decompress(OssHander	*myData, z_decompress_reader *reader, bool async, char *msg)
{
	int status = 0;

	if (reader->zstream.avail_in == 0)
	{
		uint64 hasRead = 0;

retry_next:
		reader->zstream.avail_out = OSS_ZIP_DECOMPRESS_CHUNKSIZE;
		reader->zstream.next_out = (Byte *)reader->out;

		/*
		* read OSS_ZIP_DECOMPRESS_CHUNKSIZE data from underlying reader and put into this->in
		* buffer. read() might happen more than once when reaching EOF, make sure every time read()
		* will return 0.
		*/
		hasRead = SourceRead_internal(myData, reader->in, OSS_ZIP_DECOMPRESS_CHUNKSIZE, false, async, msg);

		/* EOF, no more data to decompress. */
		if (hasRead == 0)
		{
			if (async == false)
			{
				elog(DEBUG1,
					"No more data to decompress: avail_in = %u, avail_out = %u, total_in = %lu, total_out = %lu",
					reader->zstream.avail_in, reader->zstream.avail_out, reader->zstream.total_in, reader->zstream.total_out);
			}
			else if (msg[0] != '\0')
			{
				return;
			}

			oss_next_file(myData);
			if (myData->currentfile != NULL)
			{
				inflateEnd(&reader->zstream);
				z_decompress_reader_open(reader, async, msg);
				if (async && msg[0] != '\0')
				{
					return;
				}
				else
				{
					goto retry_next;
				}
			}

		return;
        }

		reader->zstream.next_in = (Byte *)reader->in;
		reader->zstream.avail_in = hasRead;
	} 
	else
	{
 		/* Still have more data in 'in' buffer to decode. */
		reader->zstream.avail_out = OSS_ZIP_DECOMPRESS_CHUNKSIZE;
		reader->zstream.next_out = (Byte *)reader->out;
	}

	status = inflate(&reader->zstream, Z_NO_FLUSH);
	if (status == Z_STREAM_END)
	{
		if (async == false)
		{
			elog(DEBUG1, "Decompression finished: Z_STREAM_END.");
		}
	} 
	else if (status < 0 || status == Z_NEED_DICT)
	{
		inflateEnd(&reader->zstream);
		if (async)
		{
			snprintf(msg, ERROR_MESSAGE_LEN, "Failed to decompress data: %d", status);
		}
		else
		{
			elog(ERROR, "Failed to decompress data: %d", status);
		}
	}

	return;
}

static uint64 
get_z_decompressed_bytes_num(z_decompress_reader *reader)
{
	return OSS_ZIP_DECOMPRESS_CHUNKSIZE - reader->zstream.avail_out;
}

