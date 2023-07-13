package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteS3RemoteDriver
    extends AbsSatelliteDbDriver<S3Remote>
    implements S3RemoteDatabaseDriver
{
    private final StateFlagsPersistence<S3Remote> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<S3Remote, String> endpointDriver;
    private final SingleColumnDatabaseDriver<S3Remote, String> bucketDriver;
    private final SingleColumnDatabaseDriver<S3Remote, String> regionDriver;
    private final SingleColumnDatabaseDriver<S3Remote, byte[]> accessKeyDriver;
    private final SingleColumnDatabaseDriver<S3Remote, byte[]> secretKeyDriver;

    @Inject
    public SatelliteS3RemoteDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        endpointDriver = getNoopColumnDriver();
        bucketDriver = getNoopColumnDriver();
        regionDriver = getNoopColumnDriver();
        accessKeyDriver = getNoopColumnDriver();
        secretKeyDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<S3Remote, String> getEndpointDriver()
    {
        return endpointDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<S3Remote, String> getBucketDriver()
    {
        return bucketDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<S3Remote, String> getRegionDriver()
    {
        return regionDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<S3Remote, byte[]> getAccessKeyDriver()
    {
        return accessKeyDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<S3Remote, byte[]> getSecretKeyDriver()
    {
        return secretKeyDriver;
    }

    @Override
    public StateFlagsPersistence<S3Remote> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }
}
