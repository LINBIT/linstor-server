package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions.POOL_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions.POOL_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.StorPoolDefinitions.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class StorPoolDefinitionDbDriver
    extends AbsDatabaseDriver<StorPoolDefinitionData, StorPoolDefinition.InitMaps, Void>
    implements StorPoolDefinitionDataDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private StorPoolDefinitionData disklessStorPoolDfn;

    @Inject
    public StorPoolDefinitionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.STOR_POOL_DEFINITIONS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, spDfn -> spDfn.getUuid().toString());
        setColumnSetter(POOL_NAME, spDfn -> spDfn.getName().value);
        setColumnSetter(POOL_DSP_NAME, spDfn -> spDfn.getName().displayValue);
    }

    @Override
    public StorPoolDefinitionData createDefaultDisklessStorPool() throws DatabaseException
    {
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

            disklessStorPoolDfn = new StorPoolDefinitionData(
                java.util.UUID.randomUUID(),
                getObjectProtection(ObjectProtection.buildPath(storPoolName)),
                storPoolName,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                new TreeMap<>()
            );
            create(disklessStorPoolDfn);
        }
        return disklessStorPoolDfn;
    }

    @Override
    protected Pair<StorPoolDefinitionData, InitMaps> load(
        RawParameters raw,
        Void ignored
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Map<NodeName, StorPool> storPoolsMap = new TreeMap<>();

        final StorPoolName storPoolName = raw.build(POOL_DSP_NAME, StorPoolName::new);
        return new Pair<>(
            new StorPoolDefinitionData(
                raw.build(UUID, java.util.UUID::fromString),
                getObjectProtection(ObjectProtection.buildPath(storPoolName)),
                storPoolName,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                storPoolsMap
            ),
            new InitMapsImpl(storPoolsMap)
        );
    }

    @Override
    protected String getId(StorPoolDefinitionData dataRef)
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    private class InitMapsImpl implements StorPoolDefinition.InitMaps
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
