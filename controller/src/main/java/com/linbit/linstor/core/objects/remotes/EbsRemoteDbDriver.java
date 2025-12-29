package com.linbit.linstor.core.objects.remotes;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Function;

@Singleton
public final class EbsRemoteDbDriver extends AbsProtectedDatabaseDriver<EbsRemote, EbsRemote.InitMaps, Void>
    implements EbsRemoteCtrlDatabaseDriver
{
    protected final AccessContext dbCtx;

    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<EbsRemote, URL> urlDriver;
    protected final SingleColumnDatabaseDriver<EbsRemote, String> availabilityZoneDriver;
    protected final SingleColumnDatabaseDriver<EbsRemote, String> regionDriver;
    protected final SingleColumnDatabaseDriver<EbsRemote, byte[]> encryptedSecretKeyDriver;
    protected final SingleColumnDatabaseDriver<EbsRemote, byte[]> encryptedAccessKeyDriver;
    protected final StateFlagsPersistence<EbsRemote> flagsDriver;

    @Inject
    public EbsRemoteDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.EBS_REMOTES, dbEngine, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits(dbCtx));
        setColumnSetter(URL, remote -> remote.getUrl(dbCtxRef).toString());
        setColumnSetter(AVAILABILITY_ZONE, remote -> remote.getAvailabilityZone(dbCtx));
        setColumnSetter(REGION, remote -> remote.getRegion(dbCtx));

        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                setColumnSetter(ACCESS_KEY, remote -> remote.getEncryptedAccessKey(dbCtx));
                setColumnSetter(SECRET_KEY, remote -> remote.getEncryptedSecretKey(dbCtx));
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        urlDriver = generateSingleColumnDriver(URL, remote -> remote.getUrl(dbCtx).toString(), java.net.URL::toString);
        availabilityZoneDriver = generateSingleColumnDriver(
            AVAILABILITY_ZONE,
            remote -> remote.getAvailabilityZone(dbCtx),
            Function.identity()
        );
        regionDriver = generateSingleColumnDriver(
            REGION,
            remote -> remote.getRegion(dbCtx),
            Function.identity()
        );

        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
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
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
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
        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                initFlags = raw.get(FLAGS);
                encryptedSecretKey = raw.get(SECRET_KEY);
                encryptedAccessKey = raw.get(ACCESS_KEY);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        try
        {
            return new Pair<>(
                new EbsRemote(
                    getObjectProtection(ObjectProtection.buildPath(remoteName)),
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
    protected String getId(EbsRemote dataRef) throws AccessDeniedException
    {
        return "EbsRemote(" + dataRef.getName().displayValue + ")";
    }

    private class InitMapsImpl implements EbsRemote.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
