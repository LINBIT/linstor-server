package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.FreeSpaceMgrName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.DRIVER_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.FREE_SPACE_MGR_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.FREE_SPACE_MGR_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.POOL_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class StorPoolDbDriver
    extends AbsDatabaseDriver<StorPool,
        StorPool.InitMaps,
        Pair<Map<NodeName, ? extends Node>,
            Map<StorPoolName, ? extends StorPoolDefinition>>>
    implements StorPoolDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final Map<FreeSpaceMgrName, FreeSpaceMgr> freeSpaceMgrMap;

    @Inject
    public StorPoolDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.NODE_STOR_POOL, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        freeSpaceMgrMap = new TreeMap<>();

        setColumnSetter(UUID, sp -> sp.getUuid().toString());
        setColumnSetter(NODE_NAME, sp -> sp.getNode().getName().value);
        setColumnSetter(POOL_NAME, sp -> sp.getName().value);
        setColumnSetter(DRIVER_NAME, sp -> sp.getDeviceProviderKind().name());
        setColumnSetter(FREE_SPACE_MGR_NAME, sp -> sp.getFreeSpaceTracker().getName().value);
        setColumnSetter(FREE_SPACE_MGR_DSP_NAME, sp -> sp.getFreeSpaceTracker().getName().displayValue);
    }

    public Map<FreeSpaceMgrName, FreeSpaceMgr> getAllLoadedFreeSpaceMgrs()
    {
        return freeSpaceMgrMap;
    }

    @Override
    protected Pair<StorPool, StorPool.InitMaps> load(
        RawParameters raw,
        Pair<Map<NodeName, ? extends Node>,
            Map<StorPoolName, ? extends StorPoolDefinition>> parent
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException
    {
        final NodeName nodeName = raw.build(NODE_NAME, NodeName::new);
        final StorPoolName poolName = raw.build(POOL_NAME, StorPoolName::new);
        final FreeSpaceMgrName fsmName = raw.build(FREE_SPACE_MGR_DSP_NAME, FreeSpaceMgrName::restoreName);

        final FreeSpaceMgr fsm = restore(fsmName);

        final Map<String, VlmProviderObject<Resource>> vlmMap = new TreeMap<>();
        final Map<String, VlmProviderObject<Snapshot>> snapVlmMap = new TreeMap<>();

        return new Pair<>(
            new StorPool(
                raw.build(UUID, java.util.UUID::fromString),
                parent.objA.get(nodeName),
                parent.objB.get(poolName),
                LinstorParsingUtils.asProviderKind(raw.<String>get(DRIVER_NAME)),
                fsm,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                vlmMap,
                snapVlmMap
            ),
            new InitMapsImpl(vlmMap, snapVlmMap)
        );
    }

    private FreeSpaceMgr restore(FreeSpaceMgrName fsmNameRef) throws DatabaseException
    {
        FreeSpaceMgr fsm = freeSpaceMgrMap.get(fsmNameRef);
        if (fsm == null)
        {
            fsm = new FreeSpaceMgr(
                dbCtx,
                getObjectProtection(ObjectProtection.buildPath(fsmNameRef)),
                fsmNameRef,
                transMgrProvider,
                transObjFactory
            );
            freeSpaceMgrMap.put(fsmNameRef, fsm);
        }
        return fsm;
    }

    @Override
    protected String getId(StorPool sp)
    {
        return "(NodeName=" + sp.getNode().getName().displayValue +
            " PoolName=" + sp.getName().displayValue + ")";
    }

    private class InitMapsImpl implements StorPool.InitMaps
    {
        private final Map<String, VlmProviderObject<Resource>> vlmMap;
        private final Map<String, VlmProviderObject<Snapshot>> snapVlmMap;

        InitMapsImpl(
            Map<String, VlmProviderObject<Resource>> vlmMapRef,
            Map<String, VlmProviderObject<Snapshot>> snapVlmMapRef
        )
        {
            vlmMap = vlmMapRef;
            snapVlmMap = snapVlmMapRef;
        }

        @Override
        public Map<String, VlmProviderObject<Resource>> getVolumeMap()
        {
            return vlmMap;
        }

        @Override
        public Map<String, VlmProviderObject<Snapshot>> getSnapshotVolumeMap()
        {
            return snapVlmMap;
        }
    }

}
