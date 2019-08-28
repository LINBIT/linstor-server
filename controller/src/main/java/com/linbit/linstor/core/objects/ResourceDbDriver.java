package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource.InitMaps;
import com.linbit.linstor.core.objects.Resource.Key;
import com.linbit.linstor.core.objects.ResourceDbDriver.RscInMap;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceDbDriver extends AbsDatabaseDriver<ResourceData, Resource.InitMaps, RscInMap>
    implements ResourceDataDatabaseDriver
{

    private final StateFlagsPersistence<ResourceData> flagsDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<? extends TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(
            errorReporterRef,
            GeneratedDatabaseTables.RESOURCES,
            dbEngine,
            objProtDriverRef
        );
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, Resource.RscFlags.class);

        setColumnSetter(UUID, rsc -> rsc.getUuid().toString());
        setColumnSetter(NODE_NAME, rsc -> rsc.getAssignedNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rsc -> rsc.getDefinition().getName().value);
        setColumnSetter(RESOURCE_FLAGS, rsc -> rsc.getStateFlags().getFlagsBits(dbCtxRef));
    }

    @Override
    public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    public Map<ResourceData, Resource.InitMaps> loadAll(
        Map<NodeName, Node> tmpNodesMapRef,
        Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef
    )
        throws DatabaseException
    {
        return loadAll(new RscInMap(tmpNodesMapRef, tmpRscDfnMapRef));
    }

    @Override
    protected Pair<ResourceData, InitMaps> load(
        RawParameters raw,
        RscInMap loadAllDataRef
    )
        throws DatabaseException, InvalidNameException, InvalidIpAddressException, ValueOutOfRangeException
    {
        NodeName nodeName = new NodeName(raw.get(NODE_NAME));
        ResourceName rscName = new ResourceName(raw.get(RESOURCE_NAME));
        Map<Key, ResourceConnection> rscConMap = new TreeMap<>();
        Map<VolumeNumber, Volume> vlmMap = new TreeMap<>();

        final long flags;
        switch (getDbType())
        {
            case ETCD:
                flags = Long.parseLong(raw.get(RESOURCE_FLAGS));
                break;
            case SQL:
                flags = raw.get(RESOURCE_FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new ResourceData(
                raw.build(UUID, java.util.UUID::fromString),
                getObjectProtection(ObjectProtection.buildPath(nodeName, rscName)),
                loadAllDataRef.rscDfnMap.get(rscName),
                loadAllDataRef.nodeMap.get(nodeName),
                flags,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider,
                rscConMap,
                vlmMap
            ),
            new InitMapImpl(rscConMap, vlmMap)
        );
    }

    @Override
    protected String getId(ResourceData rsc)
    {
        return "(NodeName=" + rsc.getAssignedNode().getName().displayValue +
            " ResName=" + rsc.getDefinition().getName().displayValue + ")";
    }


    public static class RscInMap
    {
        Map<NodeName, Node> nodeMap;
        Map<ResourceName, ResourceDefinition> rscDfnMap;

        public RscInMap(Map<NodeName, Node> nodeMapRef, Map<ResourceName, ResourceDefinition> rscDfnMapRef)
        {
            nodeMap = nodeMapRef;
            rscDfnMap = rscDfnMapRef;
        }
    }

    public static class InitMapImpl implements Resource.InitMaps
    {
        private final Map<Key, ResourceConnection> rscConMap;
        private final Map<VolumeNumber, Volume> vlmMap;

        public InitMapImpl(
            Map<Key, ResourceConnection> rscConMapRef,
            Map<VolumeNumber, Volume> vlmMapRef
        )
        {
            rscConMap = rscConMapRef;
            vlmMap = vlmMapRef;
        }

        @Override
        public Map<Key, ResourceConnection> getRscConnMap()
        {
            return rscConMap;
        }

        @Override
        public Map<VolumeNumber, Volume> getVlmMap()
        {
            return vlmMap;
        }
    }
}
