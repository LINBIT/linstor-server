package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.propscon.Props;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

    public void putObject(String key, InputStream input, ObjectMetadata metadata)
        throws SdkClientException, AmazonServiceException
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
        s3.putObject(
            backupProps.getProp(ApiConsts.KEY_BACKUP_S3_BUCKET), key, input, metadata
        );
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
