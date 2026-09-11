package com.linbit.linstor.core.objects.remotes;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.S3Remote.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.ACCESS_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.BUCKET;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.ENDPOINT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.REGION;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.SECRET_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.S3Remotes.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.function.Function;

@Singleton
public final class S3RemoteDbDriver extends AbsDatabaseDriver<S3Remote, S3Remote.InitMaps, Void>
    implements S3RemoteCtrlDatabaseDriver
{
    final PropsContainerFactory propsContainerFactory;
    final TransactionObjectFactory transObjFactory;
    final Provider<? extends TransactionMgr> transMgrProvider;

    final SingleColumnDatabaseDriver<S3Remote, String> endpointDriver;
    final SingleColumnDatabaseDriver<S3Remote, String> bucketDriver;
    final SingleColumnDatabaseDriver<S3Remote, String> regionDriver;
    final SingleColumnDatabaseDriver<S3Remote, byte[]> accessKeyDriver;
    final SingleColumnDatabaseDriver<S3Remote, byte[]> secretKeyDriver;
    final StateFlagsPersistence<S3Remote> flagsDriver;

    @Inject
    public S3RemoteDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.S3_REMOTES, dbEngine);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits());
        setColumnSetter(ENDPOINT, remote -> remote.getUrl());
        setColumnSetter(BUCKET, remote -> remote.getBucket());
        setColumnSetter(REGION, remote -> remote.getRegion());
        setColumnSetter(ACCESS_KEY, remote -> remote.getAccessKey());
        setColumnSetter(SECRET_KEY, remote -> remote.getSecretKey());

        endpointDriver = generateSingleColumnDriver(ENDPOINT, remote -> remote.getUrl(), Function.identity());
        bucketDriver = generateSingleColumnDriver(BUCKET, remote -> remote.getBucket(), Function.identity());
        regionDriver = generateSingleColumnDriver(REGION, remote -> remote.getRegion(), Function.identity());
        accessKeyDriver = generateSingleColumnDriver(
            ACCESS_KEY, ignored -> MSG_DO_NOT_LOG, Function.identity()
        );
        secretKeyDriver = generateSingleColumnDriver(
            SECRET_KEY, ignored -> MSG_DO_NOT_LOG, Function.identity()
        );

        flagsDriver = generateFlagDriver(FLAGS, S3Remote.Flags.class);

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
        return flagsDriver;
    }

    @Override
    protected Pair<S3Remote, InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final RemoteName remoteName = raw.<String, RemoteName, InvalidNameException>build(DSP_NAME, RemoteName::new);
        final long initFlags;
        final byte[] accessKey;
        final byte[] secretKey;
        initFlags = raw.get(FLAGS);
        accessKey = raw.get(ACCESS_KEY);
        secretKey = raw.get(SECRET_KEY);
        return new Pair<>(
            new S3Remote(
                raw.build(UUID, java.util.UUID::fromString),
                this,
                remoteName,
                initFlags,
                raw.get(ENDPOINT),
                raw.get(BUCKET),
                raw.get(REGION),
                accessKey,
                secretKey,
                transObjFactory,
                transMgrProvider
            ),
            new InitMapsImpl()
        );
    }

    @Override
    protected String getId(S3Remote dataRef)
    {
        return "S3Remote(" + dataRef.getName().displayValue + ")";
    }

    private static class InitMapsImpl implements S3Remote.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
