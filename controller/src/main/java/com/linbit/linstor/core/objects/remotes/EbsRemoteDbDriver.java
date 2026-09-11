package com.linbit.linstor.core.objects.remotes;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.ACCESS_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.AVAILABILITY_ZONE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.REGION;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.SECRET_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.URL;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.EbsRemotes.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Function;

@Singleton
public final class EbsRemoteDbDriver extends AbsDatabaseDriver<EbsRemote, EbsRemote.InitMaps, Void>
    implements EbsRemoteCtrlDatabaseDriver
{
    final PropsContainerFactory propsContainerFactory;
    final TransactionObjectFactory transObjFactory;
    final Provider<? extends TransactionMgr> transMgrProvider;

    final SingleColumnDatabaseDriver<EbsRemote, URL> urlDriver;
    final SingleColumnDatabaseDriver<EbsRemote, String> availabilityZoneDriver;
    final SingleColumnDatabaseDriver<EbsRemote, String> regionDriver;
    final SingleColumnDatabaseDriver<EbsRemote, byte[]> encryptedSecretKeyDriver;
    final SingleColumnDatabaseDriver<EbsRemote, byte[]> encryptedAccessKeyDriver;
    final StateFlagsPersistence<EbsRemote> flagsDriver;

    @Inject
    public EbsRemoteDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.EBS_REMOTES, dbEngine);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits());
        setColumnSetter(URL, remote -> remote.getUrl().toString());
        setColumnSetter(AVAILABILITY_ZONE, remote -> remote.getAvailabilityZone());
        setColumnSetter(REGION, remote -> remote.getRegion());

        setColumnSetter(ACCESS_KEY, remote -> remote.getEncryptedAccessKey());
        setColumnSetter(SECRET_KEY, remote -> remote.getEncryptedSecretKey());

        urlDriver = generateSingleColumnDriver(URL, remote -> remote.getUrl().toString(), java.net.URL::toString);
        availabilityZoneDriver = generateSingleColumnDriver(
            AVAILABILITY_ZONE,
            remote -> remote.getAvailabilityZone(),
            Function.identity()
        );
        regionDriver = generateSingleColumnDriver(
            REGION,
            remote -> remote.getRegion(),
            Function.identity()
        );

        switch (getDbType())
        {
            case SQL, K8S_CRD ->
            {
                encryptedSecretKeyDriver = generateSingleColumnDriver(
                    SECRET_KEY,
                    ingored -> "do not log",
                    Function.identity()
                );
                encryptedAccessKeyDriver = generateSingleColumnDriver(
                    ACCESS_KEY,
                    ingored -> "do not log",
                    Function.identity()
                );
            }
            default -> throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(FLAGS, LinstorRemote.Flags.class);
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, URL> getUrlDriver()
    {
        return urlDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, String> getAvailabilityZoneDriver()
    {
        return availabilityZoneDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, String> getRegionDriver()
    {
        return regionDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedSecretKeyDriver()
    {
        return encryptedSecretKeyDriver;
    }
    @Override
    public SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedAccessKeyDriver()
    {
        return encryptedAccessKeyDriver;
    }

    @Override
    public StateFlagsPersistence<EbsRemote> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<EbsRemote, EbsRemote.InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final RemoteName remoteName = raw.<String, RemoteName, InvalidNameException>build(DSP_NAME, RemoteName::new);
        final long initFlags;
        final byte[] encryptedSecretKey;
        final byte[] encryptedAccessKey;
        initFlags = raw.get(FLAGS);
        encryptedSecretKey = raw.get(SECRET_KEY);
        encryptedAccessKey = raw.get(ACCESS_KEY);

        try
        {
            return new Pair<>(
                new EbsRemote(
                    raw.build(UUID, java.util.UUID::fromString),
                    this,
                    remoteName,
                    initFlags,
                    new URL(raw.get(URL)),
                    raw.get(REGION),
                    raw.get(AVAILABILITY_ZONE),
                    encryptedSecretKey,
                    encryptedAccessKey,
                    transObjFactory,
                    transMgrProvider
                ),
                new InitMapsImpl()
            );
        }
        catch (MalformedURLException exc)
        {
            throw new DatabaseException("Could not restore persisted URL: " + raw.get(URL), exc);
        }
    }

    @Override
    protected String getId(EbsRemote dataRef)
    {
        return "EbsRemote(" + dataRef.getName().displayValue + ")";
    }

    private static class InitMapsImpl implements EbsRemote.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
