package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.DRIVER_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeStorPool.EXTERNAL_LOCKING;
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
public final class StorPoolDbDriver
    extends AbsProtectedDatabaseDriver<
        StorPool,
        StorPool.InitMaps,
        PairNonNull<Map<NodeName, ? extends Node>,
            Map<StorPoolName, ? extends StorPoolDefinition>>>
    implements StorPoolCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final Map<SharedStorPoolName, FreeSpaceMgr> freeSpaceMgrMap;

    @Inject
    public StorPoolDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.NODE_STOR_POOL, dbEngineRef, objProtFactoryRef);
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
        switch (getDbType())
        {
            case SQL: // fall-through
            case ETCD:
                setColumnSetter(EXTERNAL_LOCKING, sp -> Boolean.toString(sp.isExternalLocking()));
                break;
            case K8S_CRD:
                setColumnSetter(EXTERNAL_LOCKING, sp -> sp.isExternalLocking());
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
    }

    @Override
    public Map<SharedStorPoolName, FreeSpaceMgr> getAllLoadedFreeSpaceMgrs()
    {
        return freeSpaceMgrMap;
    }

    @Override
    protected Pair<StorPool, StorPool.InitMaps> load(
        RawParameters raw,
        PairNonNull<Map<NodeName, ? extends Node>,
            Map<StorPoolName, ? extends StorPoolDefinition>> parent
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException
    {
        final NodeName nodeName = raw.build(NODE_NAME, NodeName::new);
        final StorPoolName poolName = raw.build(POOL_NAME, StorPoolName::new);
        final SharedStorPoolName sharedStorPoolName = raw.build(
            FREE_SPACE_MGR_DSP_NAME, SharedStorPoolName::restoreName
        );
        final boolean externalLocking;

        switch (getDbType())
        {
            case ETCD:
                externalLocking = raw.build(EXTERNAL_LOCKING, Boolean::parseBoolean);
                break;
            case SQL: // fall-through
            case K8S_CRD:
                externalLocking = raw.get(EXTERNAL_LOCKING);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        final FreeSpaceMgr fsm = restore(sharedStorPoolName);

        final Map<String, VlmProviderObject<Resource>> vlmMap = new TreeMap<>();
        final Map<String, VlmProviderObject<Snapshot>> snapVlmMap = new TreeMap<>();

        return new Pair<>(
            new StorPool(
                raw.build(UUID, java.util.UUID::fromString),
                parent.objA.get(nodeName),
                parent.objB.get(poolName),
                LinstorParsingUtils.asProviderKind(raw.<String>get(DRIVER_NAME)),
                fsm,
                externalLocking,
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

    private FreeSpaceMgr restore(SharedStorPoolName sharedStorPoolNameRef) throws DatabaseException
    {
        FreeSpaceMgr fsm = freeSpaceMgrMap.get(sharedStorPoolNameRef);
        if (fsm == null)
        {
            fsm = new FreeSpaceMgr(
                sharedStorPoolNameRef,
                transMgrProvider,
                transObjFactory
            );
            freeSpaceMgrMap.put(sharedStorPoolNameRef, fsm);
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
