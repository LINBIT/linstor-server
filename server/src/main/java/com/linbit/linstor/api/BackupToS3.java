package com.linbit.linstor.api;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultiObjectDeleteException.DeleteError;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
public class BackupToS3
{
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final StltConfigAccessor stltConfigAccessor;
    private final DecryptionHelper decHelper;
    private final ErrorReporter errorReporter;
    /**
     * Caches the .metafile content from s3. cache structure:
     * Map<Remote, Map<S3Key, Pair<Etag, content of .meta file>>>
     * The content of the .meta file is stored as String and is re-parsed as BackupMetaDataPojo with each request to
     * ensure immutability
     */
    private final HashMap<S3Remote, HashMap<String, Pair<String, String>>> cache;

    @Inject
    public BackupToS3(
        StltConfigAccessor stltConfigAccessorRef,
        DecryptionHelper decHelperRef,
        ErrorReporter errorReporterRef
    )
    {
        stltConfigAccessor = stltConfigAccessorRef;
        decHelper = decHelperRef;
        errorReporter = errorReporterRef;

        cache = new HashMap<>();
    }

    public String initMultipart(String key, S3Remote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException
    {
        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);
        String bucket = remote.getBucket(accCtx);

        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);

        InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(
            bucket,
            key
        ).withRequesterPays(reqPays);
        InitiateMultipartUploadResult initResp = s3.initiateMultipartUpload(initReq);
        return initResp.getUploadId();
    }

    public void putObjectMultipart(
        String key,
        InputStream input,
        long maxSize,
        String uploadId,
        S3Remote remote,
        AccessContext accCtx,
        byte[] masterKey
    ) throws AccessDeniedException, SdkClientException, AmazonServiceException, IOException, StorageException
    {
        assert maxSize >= 0;

        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);

        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);

        // make the buffer size (part size) as small as possible, without going over the limit of Integer.MAX_VALUE
        // while making sure to stay above the minimum part size of 5 MB and below 1000 parts.
        // we want small buffers in order to keep RAM usage low even with multiple concurrent uploads.
        long bufferSize = Math.max(5 << 20, (long) (Math.ceil((maxSize * 1024) / 1000.0) + 1.0));
        if (bufferSize > Integer.MAX_VALUE)
        {
            throw new StorageException(
                "Can only ship parts up to " + Integer.MAX_VALUE + " bytes." +
                    " Current shipment would require parts with a size of " + bufferSize + " bytes."
            );
        }
        List<PartETag> parts = new ArrayList<>();

        byte[] readBuf = new byte[(int) bufferSize];
        int offset = 0;
        int partId = 1;
        for (int readLen = input.read(readBuf, offset, readBuf.length - offset);
             readLen != -1;
             readLen = input.read(readBuf, offset, readBuf.length - offset))
        {
            offset += readLen;
            if (readBuf.length == offset)
            {
                UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(uploadId)
                    .withPartNumber(partId)
                    .withInputStream(new ByteArrayInputStream(readBuf))
                    .withPartSize(offset)
                    .withRequesterPays(reqPays);
                UploadPartResult uploadResult = s3.uploadPart(uploadRequest);
                parts.add(uploadResult.getPartETag());
                offset = 0;
                partId++;
            }
        }
        if (offset != 0)
        {
            UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partId)
                .withInputStream(new ByteArrayInputStream(readBuf, 0, offset))
                .withLastPart(true)
                .withPartSize(offset)
                .withRequesterPays(reqPays);
            UploadPartResult uploadResult = s3.uploadPart(uploadRequest);
            parts.add(uploadResult.getPartETag());
        }
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
            bucket,
            key,
            uploadId,
            parts
        ).withRequesterPays(reqPays);
        s3.completeMultipartUpload(compRequest);
        errorReporter.logTrace("Backup upload of %s to bucket %s completed in %d parts", key, bucket, partId);
    }

    public void abortMultipart(String key, String uploadId, S3Remote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException, SdkClientException, AmazonServiceException
    {
        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);

        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);

        AbortMultipartUploadRequest abortReq = new AbortMultipartUploadRequest(
            bucket,
            key,
            uploadId
        ).withRequesterPays(reqPays);
        s3.abortMultipartUpload(abortReq);
    }

    public void putObject(String key, String content, S3Remote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException, SdkClientException, AmazonServiceException
    {
        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);
        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(content.getBytes().length);
        PutObjectRequest req = new PutObjectRequest(bucket, key, new ByteArrayInputStream(content.getBytes()), meta)
            .withRequesterPays(reqPays);
        s3.putObject(req);
    }

    public void deleteObjects(Set<String> keys, S3Remote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException
    {
        String bucket = remote.getBucket(accCtx);
        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);
        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
        deleteObjectsRequest.withKeys(keys.toArray(new String[keys.size()])).withRequesterPays(reqPays);
        if (remote.isMultiDeleteSupported(accCtx))
        {
            try
            {
                DeleteObjectsResult deletedObjs = s3.deleteObjects(deleteObjectsRequest);
                HashMap<String, Pair<String, String>> remoteCache;
                synchronized (cache)
                {
                    remoteCache = lazyGet(cache, remote);
                }
                synchronized (remoteCache)
                {
                    for (DeletedObject deleted : deletedObjs.getDeletedObjects())
                    {
                        remoteCache.remove(deleted.getKey());
                    }
                }
            }
            catch (Exception exc)
            {
                if (exc instanceof MultiObjectDeleteException)
                {
                    throw exc;
                }
                errorReporter.logWarning(
                    "Exception occurred while trying multi-object-delete on remote %s. Retrying with multiple single-object-deletes",
                    remote.getName().displayValue
                );
                remote.setMultiDeleteSupported(accCtx, false);
                deleteSingleObjects(keys, bucket, reqPays, s3, remote);
            }
        }
        else
        {
            errorReporter.logDebug(
                "Multi-object-delete not supported due to prior exception on remote %s. Using multiple single-object-deletes instead",
                remote.getName().displayValue
            );
            deleteSingleObjects(keys, bucket, reqPays, s3, remote);
        }
    }

    private void deleteSingleObjects(Set<String> keys, String bucket, boolean reqPays, AmazonS3 s3, S3Remote remote)
    {
        List<DeletedObject> deleted = new ArrayList<>();
        HashMap<String, Pair<String, String>> remoteCache;
        synchronized (cache)
        {
            remoteCache = lazyGet(cache, remote);
        }
        // could use threads, but doesn't seem to be slower this way
        synchronized (remoteCache)
        {
            for (String key : keys)
            {
                try
                {
                    DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
                    req.withRequesterPays(reqPays);
                    s3.deleteObject(req);
                    remoteCache.remove(key);
                    DeletedObject obj = new DeletedObject();
                    obj.setKey(key);
                    deleted.add(obj);
                }
                catch (Exception ignored)
                {
                    // ignored, see below
                }
            }
        }
        if (deleted.size() != keys.size())
        {
            // ignored because caller also ignores these
            List<DeleteError> errs = Collections.emptyList();
            MultiObjectDeleteException newExc = new MultiObjectDeleteException(errs, deleted);
            throw newExc;
        }
    }

    public BackupMetaDataPojo getMetaFile(String key, S3Remote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException, JsonParseException, JsonMappingException, IOException
    {
        String metaFileContent = null;
        if (key.endsWith(".meta"))
        {
            HashMap<String, Pair<String, String>> remoteCache;
            synchronized (cache)
            {
                remoteCache = lazyGet(cache, remote);
            }
            synchronized (remoteCache)
            {
                Pair<String, String> pair = remoteCache.get(key);
                if (pair != null && pair.objB != null)
                {
                    metaFileContent = pair.objB;
                }
            }
        }
        if (metaFileContent == null)
        {
            final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);

            String bucket = remote.getBucket(accCtx);
            boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);

            GetObjectRequest req = new GetObjectRequest(bucket, key, reqPays);
            S3Object obj = s3.getObject(req);
            S3ObjectInputStream s3is = obj.getObjectContent();

            {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] data = new byte[1024];
                for (int nRead = s3is.read(data, 0, data.length); nRead != -1; nRead = s3is.read(data, 0, data.length))
                {
                    buffer.write(data, 0, nRead);
                }

                buffer.flush();
                byte[] byteArray = buffer.toByteArray();

                metaFileContent = new String(byteArray, StandardCharsets.UTF_8);
            }
            {
                HashMap<String, Pair<String, String>> remoteCache;
                synchronized (cache)
                {
                    remoteCache = lazyGet(cache, remote);
                }
                synchronized (remoteCache)
                {
                    Pair<String, String> pair = remoteCache.get(key);
                    if (pair != null)
                    {
                        pair.objB = metaFileContent;
                    }
                }
            }
        }
        return OBJ_MAPPER.readValue(metaFileContent, BackupMetaDataPojo.class);
    }

    private HashMap<String, Pair<String, String>> lazyGet(
        HashMap<S3Remote, HashMap<String, Pair<String, String>>> mapRef,
        S3Remote remote
    )
    {
        HashMap<String, Pair<String, String>> ret = mapRef.get(remote);
        if (ret == null)
        {
            ret = new HashMap<>();
            mapRef.put(remote, ret);
        }
        return ret;
    }

    public void deleteRemoteFromCache(S3Remote remote)
    {
        synchronized (cache)
        {
            cache.remove(remote);
        }
    }

    public InputStream getObject(String key, S3Remote remote, AccessContext accCtx, byte[] masterKey)
        throws AccessDeniedException
    {
        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);

        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);

        GetObjectRequest req = new GetObjectRequest(bucket, key, reqPays);
        S3Object obj = s3.getObject(req);
        return obj.getObjectContent();
    }

    public List<S3ObjectSummary> listObjects(
        @Nullable String withPrefix,
        S3Remote remote,
        AccessContext accCtx,
        byte[] masterKey
    )
        throws AccessDeniedException
    {
        ReadOnlyProps backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        final AmazonS3 s3 = getS3Client(remote, accCtx, masterKey);
        String bucket = remote.getBucket(accCtx);
        boolean reqPays = getRequesterPays(remote, accCtx, s3, bucket);

        ListObjectsV2Request req = new ListObjectsV2Request();
        req.withRequesterPays(reqPays)
            .withBucketName(bucket)
            .withSdkClientExecutionTimeout(
                Integer.parseInt(backupProps.getPropWithDefault(ApiConsts.KEY_BACKUP_TIMEOUT, "5")) * 1000
        );
        if (withPrefix != null && withPrefix.length() != 0)
        {
            req.withPrefix(withPrefix);
        }
        ListObjectsV2Result result = s3.listObjectsV2(req);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        while (result.isTruncated())
        {
            result = s3.listObjectsV2(req.withContinuationToken(result.getNextContinuationToken()));
            objects.addAll(result.getObjectSummaries());
        }

        // update local cache
        HashMap<String, Pair<String, String>> remoteCache;
        synchronized (cache)
        {
            remoteCache = lazyGet(cache, remote);
        }
        synchronized (remoteCache)
        {
            HashSet<String> keysToRemoveFromCache = new HashSet<>(remoteCache.keySet());
            for (S3ObjectSummary objSummary : objects)
            {
                String key = objSummary.getKey();

                // only cache .meta files, not data files
                if (key.endsWith(".meta"))
                {
                    keysToRemoveFromCache.remove(key);
                    String eTag = objSummary.getETag();

                    Pair<String, String> pair = remoteCache.get(key);
                    if (pair == null)
                    {
                        pair = new Pair<>(eTag, null);
                        remoteCache.put(key, pair);
                    }
                    else
                    {
                        if (!eTag.equals(pair.objA))
                        {
                            pair.objB = null; // clear the content
                        }
                    }
                }
            }
            for (String keyToRemove : keysToRemoveFromCache)
            {
                remoteCache.remove(keyToRemove);
            }
        }

        return objects;
    }

    private BasicAWSCredentials getCredentials(S3Remote remote, AccessContext accCtx, byte[] masterKey)
    {
        String accessKey;
        String secretKey;
        try
        {
            accessKey = new String(decHelper.decrypt(masterKey, remote.getAccessKey(accCtx)));
            secretKey = new String(decHelper.decrypt(masterKey, remote.getSecretKey(accCtx)));
        }
        catch (LinStorException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Decrypting the access key or secret key failed."
                )
            );
        }
        return new BasicAWSCredentials(
            accessKey,
            secretKey
        );
    }

    private AmazonS3 getS3Client(S3Remote remote, AccessContext accCtx, byte[] masterKey) throws AccessDeniedException
    {
        final BasicAWSCredentials awsCreds = getCredentials(remote, accCtx, masterKey);

        final EndpointConfiguration endpointConfiguration = new EndpointConfiguration(
            remote.getUrl(accCtx), remote.getRegion(accCtx));

        return AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(endpointConfiguration)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .withPathStyleAccessEnabled(remote.getFlags().isSet(accCtx, AbsRemote.Flags.S3_USE_PATH_STYLE))
            .build();
    }

    private boolean getRequesterPays(S3Remote remote, AccessContext accCtx, AmazonS3 s3, String bucket) throws AccessDeniedException
     {
         boolean reqPaysSupported = remote.isRequesterPaysSupported(accCtx);
         boolean ret = false;
         if (reqPaysSupported)
         {
             try
             {
                 ret = s3.isRequesterPaysEnabled(bucket);
             }
             catch (Exception exc)
             {
                 remote.setRequesterPaysSupported(accCtx, false);
                 errorReporter.logWarning(
                     "Exception occurred while checking for support of requester-pays on remote %s. Defaulting to false",
                     remote.getName().displayValue
                 );
             }
         }
         else
         {
             errorReporter.logDebug(
                 "Requester-pays not supported due to prior exception on remote %s.", remote.getName().displayValue
             );
         }
         return ret;
     }
}
