package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteS3RemoteDriver implements S3RemoteDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new NoOpFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteS3RemoteDriver()
    {
    }

    @Override
    public void create(S3Remote fileRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(S3Remote fileRef) throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<S3Remote, String> getEndpointDriver()
    {
        return (SingleColumnDatabaseDriver<S3Remote, String>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<S3Remote, String> getBucketDriver()
    {
        return (SingleColumnDatabaseDriver<S3Remote, String>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<S3Remote, String> getRegionDriver()
    {
        return (SingleColumnDatabaseDriver<S3Remote, String>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<S3Remote, byte[]> getAccessKeyDriver()
    {
        return (SingleColumnDatabaseDriver<S3Remote, byte[]>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<S3Remote, byte[]> getSecretKeyDriver()
    {
        return (SingleColumnDatabaseDriver<S3Remote, byte[]>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<S3Remote> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<S3Remote>) stateFlagsDriver;
    }
}
