package com.linbit.linstor.api;

import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.propscon.Props;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.InputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

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
}
