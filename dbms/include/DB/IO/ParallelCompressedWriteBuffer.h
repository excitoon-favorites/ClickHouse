#pragma once

#include <DB/IO/CompressedWriteBuffer.h>
#include <list>
#include <atomic>
#include <future>
#include <numeric>


namespace DB
{

class ParallelCompressedWriteBuffer : public CompressedWriteBuffer
{
private:
	static constexpr size_t WRITE_BUFFER_DEFAULT_MAX_WORKERS = 16;

	const size_t max_workers;

	struct Worker
	{
		PODArray<char> input;
		PODArray<char> result;
		std::future<void> exception;
		std::atomic<bool> ready { false };

		void write(WriteBuffer & out);
	};
	std::list<Worker> workers;

	/// Pass data from workers to output.
	void gatherData(bool only_ready);

	/// Place buffer to workers.
	void scheduleData();

	void nextImpl() override;

public:
	ParallelCompressedWriteBuffer(
		WriteBuffer & out_,
		size_t max_workers = WRITE_BUFFER_DEFAULT_MAX_WORKERS,
		CompressionMethod method_ = CompressionMethod::LZ4,
		size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

	~ParallelCompressedWriteBuffer();
	
	/// Объём сжатых данных
	size_t getCompressedBytes() override
	{
		nextIfAtEnd();
		return out.count();
	}

	/// Сколько несжатых байт было записано в буфер
	size_t getUncompressedBytes() override
	{
		return std::accumulate(workers.begin(), workers.end(), count(), [] (size_t s, Worker & w) { return s + w.input.size(); });
	}

	/// Сколько байт находится в буфере (ещё не сжато)
	size_t getRemainingBytes() override
	{
		nextIfAtEnd();
		return std::accumulate(workers.begin(), workers.end(), offset(), [] (size_t s, Worker & w) { return s + w.input.size(); });
	}

};

}
