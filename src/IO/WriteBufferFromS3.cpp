#include <Common/config.h>

#if USE_AWS_S3

#    include <Common/setThreadName.h>
#    include <IO/WriteBufferFromS3.h>
#    include <IO/WriteHelpers.h>
#    include <Common/MemoryTracker.h>

#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/CreateMultipartUploadRequest.h>
#    include <aws/s3/model/CompleteMultipartUploadRequest.h>
#    include <aws/s3/model/PutObjectRequest.h>
#    include <aws/s3/model/UploadPartRequest.h>
#    include <common/logger_useful.h>

#    include <utility>


namespace ProfileEvents
{
    extern const Event S3WriteBytes;
}


namespace DB
{
// S3 protocol does not allow to have multipart upload with more than 10000 parts.
// In case server does not return an error on exceeding that number, we print a warning
// because custom S3 implementation may allow relaxed requirements on that.
const int S3_WARN_MAX_PARTS = 10000;


namespace ErrorCodes
{
    extern const int S3_ERROR;
}


WriteBufferFromS3::WriteBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    size_t minimum_upload_part_size_,
    size_t max_single_part_upload_size_,
    size_t thread_pool_size,
    std::optional<std::map<String, String>> object_metadata_,
    size_t buffer_size_)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size_, nullptr, 0)
    , bucket(bucket_)
    , key(key_)
    , object_metadata(std::move(object_metadata_))
    , client_ptr(std::move(client_ptr_))
    , minimum_upload_part_size(minimum_upload_part_size_)
    , max_single_part_upload_size(max_single_part_upload_size_)
{
    allocateBuffer();

    LOG_TRACE(log, "thread_pool_size = {}", thread_pool_size);
    if (thread_pool_size != 1)
    {
        writing_thread_pool = std::make_shared<ThreadPool>(thread_pool_size > 0 ? thread_pool_size : 1024);
    }
}

void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    temporary_buffer->write(working_buffer.begin(), offset());

    ProfileEvents::increment(ProfileEvents::S3WriteBytes, offset());

    last_part_size += offset();

    /// Data size exceeds singlepart upload threshold, need to use multipart upload.
    if (multipart_upload_id.empty() && last_part_size > max_single_part_upload_size)
        createMultipartUpload();

    if (!multipart_upload_id.empty() && last_part_size > minimum_upload_part_size)
    {
        writePart();
        allocateBuffer();
    }
}

void WriteBufferFromS3::allocateBuffer()
{
    temporary_buffer = Aws::MakeShared<Aws::StringStream>("temporary buffer");
    temporary_buffer->exceptions(std::ios::badbit);
    last_part_size = 0;
}

void WriteBufferFromS3::finalize()
{
    /// FIXME move final flush into the caller
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    finalizeImpl();
}

void WriteBufferFromS3::finalizeImpl()
{
    if (finalized)
        return;

    next();

    if (multipart_upload_id.empty())
    {
        makeSinglepartUpload();
    }
    else
    {
        /// Write rest of the data as last part.
        writePart();
        completeMultipartUpload();
    }

    finalized = true;
}

void WriteBufferFromS3::createMultipartUpload()
{
    Aws::S3::Model::CreateMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());

    auto outcome = client_ptr->CreateMultipartUpload(req);

    if (outcome.IsSuccess())
    {
        multipart_upload_id = outcome.GetResult().GetUploadId();
        LOG_DEBUG(log, "Multipart upload has created. Bucket: {}, Key: {}, Upload id: {}", bucket, key, multipart_upload_id);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

void WriteBufferFromS3::writePart()
{
    auto size = temporary_buffer->tellp();

    if (size < 0)
        throw Exception("Failed to write part. Buffer in invalid state.", ErrorCodes::S3_ERROR);

    if (size == 0)
    {
        LOG_DEBUG(log, "Skipping writing part. Buffer is empty.");
        return;
    }

    if (part_tags.size() == S3_WARN_MAX_PARTS)
    {
        // Don't throw exception here by ourselves but leave the decision to take by S3 server.
        LOG_WARNING(log, "Maximum part number in S3 protocol has reached (too many parts). Server may not accept this whole upload.");
    }

    auto part_data = std::make_shared<Aws::StringStream>();
    temporary_buffer.swap(part_data);

    size_t part_number = part_tags.size() + 1;
    auto part_tag = std::make_shared<String>();
    part_tags.emplace_back(part_tag);
    auto thread_group = CurrentThread::getGroup();
    auto write_job = [&, part_number, part_tag, part_data, thread_group]
    {
        doWritePart(part_data, part_number, part_tag, thread_group);
    };

    if (writing_thread_pool)
        writing_thread_pool->scheduleOrThrowOnError(std::move(write_job));
    else
        write_job();
}

void WriteBufferFromS3::doWritePart(std::shared_ptr<Aws::StringStream> part_data, size_t part_number, std::shared_ptr<String> output_tag, ThreadGroupStatusPtr thread_group)
{
    if (writing_thread_pool)
    {
        setThreadName("QueryPipelineEx");

        if (thread_group)
            CurrentThread::attachTo(thread_group);
    }

    SCOPE_EXIT(
            if (writing_thread_pool && thread_group)
                CurrentThread::detachQueryIfNotDetached();
    );

    LOG_DEBUG(log, "Writing part. Bucket: {}, Key: {}, Upload_id: {}, Size: {}", bucket, key, multipart_upload_id, part_data->tellp());

    Aws::S3::Model::UploadPartRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetPartNumber(part_number);
    req.SetUploadId(multipart_upload_id);
    req.SetContentLength(part_data->tellp());
    req.SetBody(part_data);

    auto outcome = client_ptr->UploadPart(req);

    if (outcome.IsSuccess())
    {
        auto etag = outcome.GetResult().GetETag();
        output_tag->assign(std::move(etag));
        LOG_DEBUG(log, "Writing part finished. Bucket: {}, Key: {}, Upload_id: {}, Etag: {}, Part: {}", bucket, key, multipart_upload_id, *output_tag, part_number);
    }
    else
    {
        LOG_DEBUG(log, "Writing part failed with error: \"{}\". Bucket: {}, Key: {}, Upload_id: {}, Part: {}",
                outcome.GetError().GetMessage(), bucket, key, multipart_upload_id, part_number);

        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

void WriteBufferFromS3::completeMultipartUpload()
{
    if (part_tags.empty())
        throw Exception("Failed to complete multipart upload. No parts have uploaded", ErrorCodes::S3_ERROR);

    if (writing_thread_pool)
    {
        LOG_TRACE(log, "Waiting {} threads to upload data. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}",
                writing_thread_pool->active(), bucket, key, multipart_upload_id, part_tags.size());

        writing_thread_pool->wait();
    }

    LOG_DEBUG(log, "Completing multipart upload. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", bucket, key, multipart_upload_id, part_tags.size());

    Aws::S3::Model::CompleteMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(multipart_upload_id);

    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (size_t i = 0; i < part_tags.size(); ++i)
    {
        Aws::S3::Model::CompletedPart part;
        multipart_upload.AddParts(part.WithETag(*part_tags[i]).WithPartNumber(i + 1));
    }

    req.SetMultipartUpload(multipart_upload);

    auto outcome = client_ptr->CompleteMultipartUpload(req);

    if (outcome.IsSuccess())
        LOG_DEBUG(log, "Multipart upload has completed. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", bucket, key, multipart_upload_id, part_tags.size());
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

void WriteBufferFromS3::makeSinglepartUpload()
{
    auto size = temporary_buffer->tellp();

    LOG_DEBUG(log, "Making single part upload. Bucket: {}, Key: {}, Size: {}", bucket, key, size);

    if (size < 0)
        throw Exception("Failed to make single part upload. Buffer in invalid state", ErrorCodes::S3_ERROR);

    if (size == 0)
    {
        LOG_DEBUG(log, "Skipping single part upload. Buffer is empty.");
        return;
    }

    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetContentLength(size);
    req.SetBody(temporary_buffer);
    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());

    auto outcome = client_ptr->PutObject(req);

    if (outcome.IsSuccess())
        LOG_DEBUG(log, "Single part upload has completed. Bucket: {}, Key: {}, Object size: {}", bucket, key, req.GetContentLength());
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

}

#endif
