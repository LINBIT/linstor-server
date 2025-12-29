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
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteCtrlDatabaseDriver;
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

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.CLUSTER_ID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.ENCRYPTED_PASSPHRASE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.URL;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LinstorRemotes.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.function.Function;

@Singleton
public final class LinstorRemoteDbDriver extends AbsProtectedDatabaseDriver<LinstorRemote, LinstorRemote.InitMaps, Void>
    implements LinstorRemoteCtrlDatabaseDriver
{
    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<LinstorRemote, URL> urlDriver;
    protected final SingleColumnDatabaseDriver<LinstorRemote, byte[]> encryptedPassphraseDriver;
    protected final StateFlagsPersistence<LinstorRemote> flagsDriver;
    protected final AccessContext dbCtx;
    protected final SingleColumnDatabaseDriver<LinstorRemote, UUID> clusterIdDriver;

    @Inject
    public LinstorRemoteDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.LINSTOR_REMOTES, dbEngine, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits(dbCtx));
        setColumnSetter(URL, remote -> remote.getUrl(dbCtx).toString());
        setColumnSetter(
            CLUSTER_ID,
            remote -> remote.getClusterId(dbCtxRef) == null ? null : remote.getClusterId(dbCtxRef).toString()
        );

        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                setColumnSetter(ENCRYPTED_PASSPHRASE, remote -> remote.getEncryptedRemotePassphrase(dbCtx));
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        urlDriver = generateSingleColumnDriver(URL, remote -> remote.getUrl(dbCtx).toString(), java.net.URL::toString);

        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                encryptedPassphraseDriver = generateSingleColumnDriver(
                    ENCRYPTED_PASSPHRASE,
                    ingored -> "do not log",
                    Function.identity()
                );
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(FLAGS, LinstorRemote.Flags.class);
        clusterIdDriver = generateSingleColumnDriver(
            CLUSTER_ID,
            remote -> remote.getClusterId(dbCtxRef) == null ? null : remote.getClusterId(dbCtxRef).toString(),
            uuid -> uuid == null ? null : uuid.toString()
        );

    }

    @Override
    public SingleColumnDatabaseDriver<LinstorRemote, URL> getUrlDriver()
    {
        return urlDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<LinstorRemote, byte[]> getEncryptedRemotePassphraseDriver()
    {
        return encryptedPassphraseDriver;
    }

    @Override
    public StateFlagsPersistence<LinstorRemote> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<LinstorRemote, UUID> getClusterIdDriver()
    {
        return clusterIdDriver;
    }

    @Override
    protected Pair<LinstorRemote, LinstorRemote.InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final RemoteName remoteName = raw.<String, RemoteName, InvalidNameException>build(DSP_NAME, RemoteName::new);
        final long initFlags;
        final byte[] encryptedPassphrase;
        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                initFlags = raw.get(FLAGS);
                encryptedPassphrase = raw.get(ENCRYPTED_PASSPHRASE);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        try
        {
            return new Pair<>(
                new LinstorRemote(
                    getObjectProtection(ObjectProtection.buildPath(remoteName)),
                    raw.build(UUID, java.util.UUID::fromString),
                    this,
                    remoteName,
                    initFlags,
                    new URL(raw.get(URL)),
                    encryptedPassphrase,
                    raw.build(CLUSTER_ID, java.util.UUID::fromString),
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
    protected String getId(LinstorRemote dataRef) throws AccessDeniedException
    {
        return "LinstorRemote(" + dataRef.getName().displayValue + ")";
    }

    private class InitMapsImpl implements LinstorRemote.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
