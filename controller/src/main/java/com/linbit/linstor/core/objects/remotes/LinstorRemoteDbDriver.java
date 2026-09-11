package com.linbit.linstor.core.objects.remotes;

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
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
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

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.function.Function;

@Singleton
public final class LinstorRemoteDbDriver extends AbsDatabaseDriver<LinstorRemote, LinstorRemote.InitMaps, Void>
    implements LinstorRemoteCtrlDatabaseDriver
{
    final PropsContainerFactory propsContainerFactory;
    final TransactionObjectFactory transObjFactory;
    final Provider<? extends TransactionMgr> transMgrProvider;

    final SingleColumnDatabaseDriver<LinstorRemote, URL> urlDriver;
    final SingleColumnDatabaseDriver<LinstorRemote, byte[]> encryptedPassphraseDriver;
    final StateFlagsPersistence<LinstorRemote> flagsDriver;
    final SingleColumnDatabaseDriver<LinstorRemote, UUID> clusterIdDriver;

    @Inject
    public LinstorRemoteDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.LINSTOR_REMOTES, dbEngine);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, remote -> remote.getUuid().toString());
        setColumnSetter(NAME, remote -> remote.getName().value);
        setColumnSetter(DSP_NAME, remote -> remote.getName().displayValue);
        setColumnSetter(FLAGS, remote -> remote.getFlags().getFlagsBits());
        setColumnSetter(URL, remote -> remote.getUrl().toString());
        setColumnSetter(
            CLUSTER_ID,
            remote -> remote.getClusterId() == null ? null : remote.getClusterId().toString()
        );

        setColumnSetter(ENCRYPTED_PASSPHRASE, remote -> remote.getEncryptedRemotePassphrase());

        urlDriver = generateSingleColumnDriver(URL, remote -> remote.getUrl().toString(), java.net.URL::toString);

        encryptedPassphraseDriver = generateSingleColumnDriver(
            ENCRYPTED_PASSPHRASE,
            ingored -> "do not log",
            Function.identity()
        );

        flagsDriver = generateFlagDriver(FLAGS, LinstorRemote.Flags.class);
        clusterIdDriver = generateSingleColumnDriver(
            CLUSTER_ID,
            remote -> remote.getClusterId() == null ? null : remote.getClusterId().toString(),
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
        initFlags = raw.get(FLAGS);
        encryptedPassphrase = raw.get(ENCRYPTED_PASSPHRASE);

        try
        {
            return new Pair<>(
                new LinstorRemote(
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
    protected String getId(LinstorRemote dataRef)
    {
        return "LinstorRemote(" + dataRef.getName().displayValue + ")";
    }

    private static class InitMapsImpl implements LinstorRemote.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
