package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PartETag;
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
    private final StltConfigAccessor stltConfigAccessor;

    @Inject
    public BackupToS3(StltConfigAccessor stltConfigAccessorRef)
    {
        stltConfigAccessor = stltConfigAccessorRef;
    }

    public String initMultipart(String key)
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();

        InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET),
            key
        );
        InitiateMultipartUploadResult initResp = s3.initiateMultipartUpload(initReq);
        return initResp.getUploadId();
    }

    public void putObjectMultipart(String key, InputStream input, long maxSize, String uploadId)
        throws SdkClientException, AmazonServiceException, IOException, StorageException
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();

        String bucket = backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET);
        long bufferSize = Math.max(5 << 20, (long) (Math.ceil(maxSize / 10000.0) + 1.0));
        if (bufferSize > Integer.MAX_VALUE)
        {
            throw new StorageException(
                "Can only ship parts up to " + Integer.MAX_VALUE + " bytes." +
                    " Current shipment would require parts with a size of " + bufferSize + " bytes."
            );
        }
        List<PartETag> parts = new ArrayList<>();

        byte[] readBuf = new byte[(int) bufferSize];
        int readLen = 0;
        int offset = 0;
        int partId = 1;
        while ((readLen = input.read(readBuf, offset, readBuf.length - offset)) != -1)
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
                    .withPartSize(offset);
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
                .withPartSize(offset);
            UploadPartResult uploadResult = s3.uploadPart(uploadRequest);
            parts.add(uploadResult.getPartETag());
        }
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
            bucket,
            key,
            uploadId,
            parts
        );
        s3.completeMultipartUpload(compRequest);
    }

    public void abortMultipart(String key, String uploadId) throws SdkClientException, AmazonServiceException
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();

        AbortMultipartUploadRequest abortReq = new AbortMultipartUploadRequest(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET),
            key,
            uploadId
        );
        s3.abortMultipartUpload(abortReq);
    }

    public void putObject(String key, String content) throws SdkClientException, AmazonServiceException
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();
        s3.putObject(backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET), key, content);
    }

    public void deleteObject(String key) throws SdkClientException, AmazonServiceException
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();
        s3.deleteObject(backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET), key);
    }

    public void deleteObjects(Set<String> keys)
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET)
        );
        String[] helper = new String[keys.size()];
        deleteObjectsRequest.withKeys(keys.toArray(helper));
        s3.deleteObjects(deleteObjectsRequest);
    }

    public BackupMetaDataPojo getMetaFile(String key, String bucket)
        throws JsonParseException, JsonMappingException, IOException
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();

        S3Object obj = s3.getObject(
            bucket == null || bucket.length() == 0 ? backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET) : bucket, key
        );
        S3ObjectInputStream s3is = obj.getObjectContent();
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(s3is, BackupMetaDataPojo.class);
    }

    public InputStream getObject(String key, String bucket)
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();
        S3Object obj = s3.getObject(
            bucket == null || bucket.length() == 0 ? backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET) : bucket, key
        );
        return obj.getObjectContent();
    }

    public List<S3ObjectSummary> listObjects(String rsc, String bucket)
    {
        Props backupProps = stltConfigAccessor.getReadonlyProps(ApiConsts.NAMESPC_BACKUP_SHIPPING);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ACCESS_KEY),
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_SECRET_KEY)
        );

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(
            new EndpointConfiguration(
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_ENDPOINT),
                backupProps.getProp(ApiConsts.KEY_BACKUP_S3_REGION)
            )
        ).withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();

        ListObjectsV2Request req = new ListObjectsV2Request();
        req.withSdkClientExecutionTimeout(
            Integer.parseInt(backupProps.getPropWithDefault(ApiConsts.KEY_BACKUP_TIMEOUT, "5")) * 1000
        );
        if (bucket == null || bucket.length() == 0)
        {
            req.withBucketName(backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET));
        }
        else
        {
            req.withBucketName(bucket);
        }
        if (rsc != null && rsc.length() != 0)
        {
            req.withPrefix(rsc);
        }
        ListObjectsV2Result result = s3.listObjectsV2(req);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        while (result.isTruncated())
        {
            result = s3.listObjectsV2(req.withContinuationToken(result.getContinuationToken()));
            objects.addAll(result.getObjectSummaries());
        }
        return objects;
    }
}
