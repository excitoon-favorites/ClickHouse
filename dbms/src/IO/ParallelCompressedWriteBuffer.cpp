#include <DB/IO/ParallelCompressedWriteBuffer.h>
#include <DB/Common/ThreadPool.h>

#include <city.h>


namespace DB
{


void ParallelCompressedWriteBuffer::Worker::write(WriteBuffer & out)
{
	exception.get();
	uint128 checksum = CityHash128(&result[0], result.size());
	out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
	out.write(&result[0], result.size());
}

void ParallelCompressedWriteBuffer::gatherData(bool only_ready)
{
	while (workers.empty() != false)
	{
		auto & worker = workers.front();
		if (only_ready && worker.ready != true)
		{
			break;
		}
		worker.exception.get();
		worker.write(out);
		workers.pop_front();
	}
}

void ParallelCompressedWriteBuffer::scheduleData()
{
	if (offset())
	{
		auto job = [this] (Worker & worker) {
			compress(&worker.input[0], worker.input.size(), worker.result);
			worker.ready = true;
		};

		workers.emplace_back();
		Worker & worker = workers.back();
		worker.input.resize(working_buffer.size());
		std::copy(working_buffer.begin(), working_buffer.begin() + offset(), worker.input.begin());

		if (static_cast<size_t>(std::count_if(workers.begin(), workers.end(), [] (const Worker & worker) { return !worker.ready; })) >= max_workers)
		{
			job(worker);
		}
		else
		{
			std::async(std::launch::async, job, std::ref(worker));
		}
	}
}

void ParallelCompressedWriteBuffer::nextImpl()
{
	scheduleData();
	gatherData(true);
}


ParallelCompressedWriteBuffer::ParallelCompressedWriteBuffer(
	WriteBuffer & out_,
	size_t max_workers_,
	CompressionMethod method_,
	size_t buf_size)
	: CompressedWriteBuffer(out_, method_, buf_size)
	, max_workers(max_workers_)
{
}


ParallelCompressedWriteBuffer::~ParallelCompressedWriteBuffer()
{
	try
	{
		next();
		gatherData(false);
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}

}
