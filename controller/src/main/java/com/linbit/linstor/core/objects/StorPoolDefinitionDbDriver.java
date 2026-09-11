package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions.POOL_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions.POOL_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class StorPoolDefinitionDbDriver
    extends AbsDatabaseDriver<StorPoolDefinition, StorPoolDefinition.InitMaps, Void>
    implements StorPoolDefinitionCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private @Nullable StorPoolDefinition disklessStorPoolDfn;

    @Inject
    public StorPoolDefinitionDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(
            errorReporterRef,
            GeneratedDatabaseTables.STOR_POOL_DEFINITIONS,
            dbEngineRef
        );
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, spDfn -> spDfn.getUuid().toString());
        setColumnSetter(POOL_NAME, spDfn -> spDfn.getName().value);
        setColumnSetter(POOL_DSP_NAME, spDfn -> spDfn.getName().displayValue);
    }

    @Override
    public StorPoolDefinition createDefaultDisklessStorPool() throws DatabaseException
    {
        if (disklessStorPoolDfn == null)
        {
            loadAll(null); // trigger loading disklessStorPoolDfn if already in database
            if (disklessStorPoolDfn == null)
            {
                StorPoolName storPoolName;
                try
                {
                    storPoolName = new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME);
                }
                catch (InvalidNameException exc)
                {
                    throw new ImplementationError("Invalid hardcoded default diskless stor pool name", exc);
                }

                disklessStorPoolDfn = new StorPoolDefinition(
                    java.util.UUID.randomUUID(),
                    storPoolName,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>()
                );
                create(disklessStorPoolDfn);
            }
        }
        return disklessStorPoolDfn;
    }

    @Override
    protected Pair<StorPoolDefinition, StorPoolDefinition.InitMaps> load(
        RawParameters raw,
        Void ignored
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Map<NodeName, StorPool> storPoolsMap = new TreeMap<>();

        final StorPoolName storPoolName = raw.build(POOL_DSP_NAME, StorPoolName::new);

        StorPoolDefinition storPoolDfn = new StorPoolDefinition(
            raw.build(UUID, java.util.UUID::fromString),
            storPoolName,
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            storPoolsMap
        );
        if (storPoolName.displayValue.equals(LinStor.DISKLESS_STOR_POOL_NAME))
        {
            disklessStorPoolDfn = storPoolDfn;
        }
        return new Pair<>(
            storPoolDfn,
            new InitMapsImpl(storPoolsMap)
        );
    }

    @Override
    protected String getId(StorPoolDefinition data)
    {
        return " (StorPoolName=" + data.getName().displayValue + ")";
    }

    private static class InitMapsImpl implements StorPoolDefinition.InitMaps
    {
        private final Map<NodeName, StorPool> storPoolMap;

        private InitMapsImpl(Map<NodeName, StorPool> storPoolMapRef)
        {
            storPoolMap = storPoolMapRef;
        }

        @Override
        public Map<NodeName, StorPool> getStorPoolMap()
        {
            return storPoolMap;
        }
    }
}
