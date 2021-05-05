package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.LinstorRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

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
import java.util.function.Function;

@Singleton
public class LinstorRemoteDbDriver extends AbsDatabaseDriver<LinstorRemote, LinstorRemote.InitMaps, Void>
    implements LinstorRemoteCtrlDatabaseDriver
{
    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<LinstorRemote, URL> urlDriver;
    protected final SingleColumnDatabaseDriver<LinstorRemote, byte[]> encryptedPassphraseDriver;
    protected final StateFlagsPersistence<LinstorRemote> flagsDriver;
    protected final AccessContext dbCtx;

    @Inject
    public LinstorRemoteDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.LINSTOR_REMOTES, dbEngine, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits(dbCtx));
        setColumnSetter(URL, remote -> remote.getUrl(dbCtx).toString());

        switch (getDbType())
        {
            case ETCD:
                setColumnSetter(
                    ENCRYPTED_PASSPHRASE,
                    remote -> Base64.encode(remote.getEncryptedTargetPassphrase(dbCtx))
                );
                break;
            case SQL:
                setColumnSetter(ENCRYPTED_PASSPHRASE, remote -> remote.getEncryptedTargetPassphrase(dbCtx));
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        urlDriver = generateSingleColumnDriver(URL, remote -> remote.getUrl(dbCtx).toString(), Function.identity());

        switch (getDbType())
        {
            case ETCD:
                encryptedPassphraseDriver = generateSingleColumnDriver(
                    ENCRYPTED_PASSPHRASE,
                    remote -> ByteUtils.bytesToHex(remote.getEncryptedTargetPassphrase(dbCtx)),
                    byteArr -> ByteUtils.bytesToHex(byteArr)
                );
                break;
            case SQL:
                encryptedPassphraseDriver = generateSingleColumnDriver(
                    ENCRYPTED_PASSPHRASE,
                    remote -> ByteUtils.bytesToHex(remote.getEncryptedTargetPassphrase(dbCtx)),
                    Function.identity()
                );
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(FLAGS, LinstorRemote.Flags.class);

    }

    @Override
    public SingleColumnDatabaseDriver<LinstorRemote, URL> getUrlDriver()
    {
        return urlDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<LinstorRemote, byte[]> getEncryptedPassphraseDriver()
    {
        return encryptedPassphraseDriver;
    }

    @Override
    public StateFlagsPersistence<LinstorRemote> getStateFlagsPersistence()
    {
        return flagsDriver;
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
            case ETCD:
                initFlags = Long.parseLong(raw.get(FLAGS));
                encryptedPassphrase = Base64.decode(raw.get(ENCRYPTED_PASSPHRASE));
                break;
            case SQL:
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
