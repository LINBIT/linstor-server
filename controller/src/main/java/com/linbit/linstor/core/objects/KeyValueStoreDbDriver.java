package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.KeyValueStore.KVS_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.KeyValueStore.KVS_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.KeyValueStore.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class KeyValueStoreDbDriver
    extends AbsProtectedDatabaseDriver<KeyValueStore, KeyValueStore.InitMaps, Void>
    implements KeyValueStoreCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public KeyValueStoreDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef

    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.KEY_VALUE_STORE, dbEngineRef, objProtFactoryRef);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, kvs -> kvs.getUuid().toString());
        setColumnSetter(KVS_NAME, kvs -> kvs.getName().value);
        setColumnSetter(KVS_DSP_NAME, kvs -> kvs.getName().displayValue);
    }

    @Override
    protected Pair<KeyValueStore, KeyValueStore.InitMaps> load(
        RawParameters raw,
        Void ignored
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final KeyValueStoreName kvsName = raw.build(KVS_DSP_NAME, KeyValueStoreName::new);

        return new Pair<>(
            new KeyValueStore(
                raw.build(UUID, java.util.UUID::fromString),
                getObjectProtection(ObjectProtection.buildPath(kvsName)),
                kvsName,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            ),
            new InitMapsImpl()
        );
    }

    @Override
    protected String getId(KeyValueStore kvs) throws AccessDeniedException
    {
        return "(KvsName=" + kvs.getName().displayValue + ")";
    }

    private class InitMapsImpl implements KeyValueStore.InitMaps
    {
        // place holder class for future init maps
    }

}
