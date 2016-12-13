#include <memory>
#include <city.h>

#ifdef USE_QUICKLZ
	#include <quicklz/quicklz_level1.h>
#endif

#include <lz4/lz4.h>
#include <lz4/lz4hc.h>
#include <zstd/zstd.h>

#include <DB/Common/unaligned.h>

#include <DB/IO/CompressedWriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_COMPRESS;
	extern const int UNKNOWN_COMPRESSION_METHOD;
}


void CompressedWriteBuffer::compress(Position begin, size_t size, PODArray<char> & result) const
{
	size_t compressed_size = 0;
	switch (method)
	{
		case CompressionMethod::QuickLZ:
		{
		#ifdef USE_QUICKLZ
			result.resize(size + QUICKLZ_ADDITIONAL_SPACE);

			compressed_size = qlz_compress(
				begin,
				&result[0],
				size,
				qlz_state.get());

			result[0] &= 3;
			break;
		#else
			throw Exception("QuickLZ compression method is disabled", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		#endif
		}
		case CompressionMethod::LZ4:
		case CompressionMethod::LZ4HC:
		{
			static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
			result.resize(header_size + LZ4_COMPRESSBOUND(size));
#pragma GCC diagnostic pop

			result[0] = static_cast<UInt8>(CompressionMethodByte::LZ4);

			if (method == CompressionMethod::LZ4)
				compressed_size = header_size + LZ4_compress(
					begin,
					&result[header_size],
					size);
			else
				compressed_size = header_size + LZ4_compressHC(
					begin,
					&result[header_size],
					size);

			UInt32 compressed_size_32 = compressed_size;
			UInt32 uncompressed_size_32 = size;

			unalignedStore(&result[1], compressed_size_32);
			unalignedStore(&result[5], uncompressed_size_32);
			break;
		}
		case CompressionMethod::ZSTD:
		{
			static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

			result.resize(header_size + ZSTD_compressBound(size));

			result[0] = static_cast<UInt8>(CompressionMethodByte::ZSTD);

			size_t res = ZSTD_compress(
				&result[header_size],
				result.size() - header_size,
				begin,
				size,
				1);

			if (ZSTD_isError(res))
				throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

			compressed_size = header_size + res;

			UInt32 compressed_size_32 = compressed_size;
			UInt32 uncompressed_size_32 = size;

			unalignedStore(&result[1], compressed_size_32);
			unalignedStore(&result[5], uncompressed_size_32);
			break;
		}
		default:
			throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
	}

	result.resize(compressed_size);
}


void CompressedWriteBuffer::nextImpl()
{
	if (!offset())
		return;

	size_t uncompressed_size = offset();

	/** Формат сжатого блока - см. CompressedStream.h
		*/
	
	compress(working_buffer.begin(), uncompressed_size, compressed_buffer);

	char * compressed_buffer_ptr = &compressed_buffer[0];
	size_t compressed_size = compressed_buffer.size();

	uint128 checksum = CityHash128(compressed_buffer_ptr, compressed_size);
	out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

	out.write(compressed_buffer_ptr, compressed_size);
}


CompressedWriteBuffer::CompressedWriteBuffer(
	WriteBuffer & out_,
	CompressionMethod method_,
	size_t buf_size)
	: BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), method(method_)
#ifdef USE_QUICKLZ
		, qlz_state(std::make_unique<qlz_state_compress>())
#endif
{
}


CompressedWriteBuffer::~CompressedWriteBuffer()
{
	try
	{
		next();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}

}
