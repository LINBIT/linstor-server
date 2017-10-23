package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class SatelliteDbDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("EmptyDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    private final PropsConDatabaseDriver propsDriver = new SatellitePropDriver();
    private final NodeDataDatabaseDriver nodeDriver = new SatelliteNodeDriver();
    private final ResourceDataDatabaseDriver resourceDriver = new SatelliteResDriver();
    private final ResourceDefinitionDataDatabaseDriver resourceDefinitionDriver = new SatelliteResDfnDriver();
    private final VolumeDataDatabaseDriver volumeDriver = new SatelliteVolDriver();
    private final VolumeDefinitionDataDatabaseDriver volumeDefinitionDriver = new SatelliteVolDfnDriver();
    private final StorPoolDefinitionDataDatabaseDriver storPoolDriver = new SatelliteSpDriver();
    private final StorPoolDataDatabaseDriver storPoolDefinitionDriver = new SatelliteSpdDriver();
    private final NetInterfaceDataDatabaseDriver netInterfaceDriver = new SatelliteNiDriver();
    private final NodeConnectionDataDatabaseDriver nodeConnectionDriver = new SatelliteNodeConDfnDriver();
    private final ResourceConnectionDataDatabaseDriver resourceConnectionDriver = new SatelliteResConDfnDriver();
    private final VolumeConnectionDataDatabaseDriver volumeConnectionDriver = new SatelliteVolConDfnDriver();

    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    private final String dbUrl = "NO_OP";

    private final AccessContext dbCtx;
    private final Map<NodeName, Node> nodesMap;
    private final Map<ResourceName, ResourceDefinition> resDfnMap;
    private final Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;

    public SatelliteDbDriver(
        AccessContext privCtx,
        Map<NodeName, Node> nodesMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMapRef,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMapRef
    )
    {
        dbCtx = privCtx;
        nodesMap = nodesMapRef;
        resDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
    }

    @Override
    public void loadAll(TransactionMgr transMgr) throws SQLException
    {
        // no-op
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }

    @Override
    public String getDefaultConnectionUrl()
    {
        return dbUrl;
    }

    @Override
    public PropsConDatabaseDriver getPropsDatabaseDriver()
    {
        return propsDriver;
    }

    @Override
    public NodeDataDatabaseDriver getNodeDatabaseDriver()
    {
        return nodeDriver;
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver()
    {
        return resourceDriver;
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver()
    {
        return resourceDefinitionDriver;
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver()
    {
        return volumeDriver;
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver()
    {
        return volumeDefinitionDriver;
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver()
    {
        return storPoolDriver;
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver()
    {
        return storPoolDefinitionDriver;
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver()
    {
        return netInterfaceDriver;
    }

    @Override
    public NodeConnectionDataDatabaseDriver getNodeConnectionDataDatabaseDriver()
    {
        return nodeConnectionDriver;
    }

    @Override
    public ResourceConnectionDataDatabaseDriver getResourceConnectionDataDatabaseDriver()
    {
        return resourceConnectionDriver;
    }

    @Override
    public VolumeConnectionDataDatabaseDriver getVolumeConnectionDataDatabaseDriver()
    {
        return volumeConnectionDriver;
    }

    private class SatellitePropDriver implements PropsConDatabaseDriver
    {
        @Override
        public Map<String, String> load(String instanceName, TransactionMgr transMgr) throws SQLException
        {
            return Collections.emptyMap();
        }

        @Override
        public void persist(String instanceName, String key, String value, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void persist(String instanceName, Map<String, String> props, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void remove(String instanceName, String key, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void remove(String instanceName, Set<String> keys, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void removeAll(String instanceName, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteNodeDriver implements NodeDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<NodeData> getStateFlagPersistence()
        {
            return (StateFlagsPersistence<NodeData>) stateFlagsDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NodeData, NodeType> getNodeTypeDriver()
        {
            return (SingleColumnDatabaseDriver<NodeData, NodeType>) singleColDriver;
        }

        @Override
        public void create(NodeData node, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public NodeData load(NodeName nodeName, boolean logWarnIfNotExists, TransactionMgr transMgr)
            throws SQLException
        {
            return (NodeData) nodesMap.get(nodeName);
        }

        @Override
        public void delete(NodeData node, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteResDriver implements ResourceDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
        {
            return (StateFlagsPersistence<ResourceData>) stateFlagsDriver;
        }

        @Override
        public void create(ResourceData res, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public ResourceData load(
            Node node,
            ResourceName resourceName,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            ResourceData resource = null;
            try
            {
                resource = (ResourceData) node.getResource(dbCtx, resourceName);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return resource;
        }

        @Override
        public void delete(ResourceData resourceData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteResDfnDriver implements ResourceDefinitionDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
        {
            return (StateFlagsPersistence<ResourceDefinitionData>) stateFlagsDriver;
        }

        @Override
        public void create(ResourceDefinitionData resDfn, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public boolean exists(ResourceName resourceName, TransactionMgr transMgr) throws SQLException
        {
            return resDfnMap.containsKey(resourceName);
        }

        @Override
        public ResourceDefinitionData load(
            ResourceName resourceName,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return (ResourceDefinitionData) resDfnMap.get(resourceName);
        }

        @Override
        public void delete(ResourceDefinitionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteVolDriver implements VolumeDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<VolumeData> getStateFlagsPersistence()
        {
            return (StateFlagsPersistence<VolumeData>) stateFlagsDriver;
        }

        @Override
        public VolumeData load(
            Resource resource,
            VolumeDefinition volumeDefinition,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            VolumeData volume = null;
            try
            {
                volume = (VolumeData) resource.getVolume(volumeDefinition.getVolumeNumber(dbCtx));
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return volume;
        }

        @Override
        public void create(VolumeData vol, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(VolumeData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteVolDfnDriver implements VolumeDefinitionDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence()
        {
            return (StateFlagsPersistence<VolumeDefinitionData>) stateFlagsDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber> getMinorNumberDriver()
        {
            return (SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber>) singleColDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver()
        {
            return (SingleColumnDatabaseDriver<VolumeDefinitionData, Long>) singleColDriver;
        }

        @Override
        public void create(VolumeDefinitionData volDfnData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public VolumeDefinitionData load(
            ResourceDefinition resourceDefinition,
            VolumeNumber volumeNumber,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            VolumeDefinitionData volumeDfn = null;
            try
            {
                volumeDfn = (VolumeDefinitionData) resourceDefinition.getVolumeDfn(dbCtx, volumeNumber);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return volumeDfn;
        }

        @Override
        public void delete(VolumeDefinitionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteSpDriver implements StorPoolDefinitionDataDatabaseDriver
    {
        @Override
        public void create(StorPoolDefinitionData storPoolDefinitionData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public StorPoolDefinitionData load(
            StorPoolName storPoolName,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return (StorPoolDefinitionData) storPoolDfnMap.get(storPoolName);
        }

        @Override
        public void delete(StorPoolDefinitionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteSpdDriver implements StorPoolDataDatabaseDriver
    {
        @Override
        public StorPoolData load(
            Node node,
            StorPoolDefinition storPoolDefinition,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            StorPoolData storPool = null;
            try
            {
                storPool = (StorPoolData) node.getStorPool(dbCtx, storPoolDefinition.getName());
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return storPool;
        }

        @Override
        public void create(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(StorPoolData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void ensureEntryExists(StorPoolData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteNiDriver implements NetInterfaceDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress> getNetInterfaceAddressDriver()
        {
            return (SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress>) singleColDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> getNetInterfaceTypeDriver()
        {
            return (SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType>) singleColDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NetInterfaceData, Integer> getNetInterfacePortDriver()
        {
            return (SingleColumnDatabaseDriver<NetInterfaceData, Integer>) singleColDriver;
        }

        @Override
        public NetInterfaceData load(
            Node node,
            NetInterfaceName netInterfaceName,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            NetInterfaceData netInterface = null;
            try
            {
                netInterface = (NetInterfaceData) node.getNetInterface(dbCtx, netInterfaceName);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return netInterface;
        }

        @Override
        public void create(NetInterfaceData netInterfaceData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(NetInterfaceData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteFlagDriver implements StateFlagsPersistence<Object>
    {
        @Override
        public void persist(Object parent, long flags, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteSingleColDriver<NOOP_KEY, NOOP> implements SingleColumnDatabaseDriver<NOOP_KEY, NOOP>
    {
        @Override
        public void update(NOOP_KEY parent, NOOP element, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteNodeConDfnDriver implements NodeConnectionDataDatabaseDriver
    {
        @Override
        public NodeConnectionData load(
            Node sourceNode,
            Node targetNode,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            NodeConnectionData nodeConnection = null;
            try
            {
                nodeConnection = (NodeConnectionData) sourceNode.getNodeConnection(dbCtx, targetNode);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return nodeConnection;
        }

        @Override
        public List<NodeConnectionData> loadAllByNode(
            Node node,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return Collections.emptyList();
        }

        @Override
        public void create(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteResConDfnDriver implements ResourceConnectionDataDatabaseDriver
    {
        @Override
        public ResourceConnectionData load(
            Resource source,
            Resource target,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            ResourceConnectionData resourceConnection = null;
            try
            {
                resourceConnection = (ResourceConnectionData) source.getResourceConnection(dbCtx, target);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return resourceConnection;
        }

        @Override
        public List<ResourceConnectionData> loadAllByResource(
            Resource resource,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return Collections.emptyList();
        }

        @Override
        public void create(ResourceConnectionData conDfnData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(ResourceConnectionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private class SatelliteVolConDfnDriver implements VolumeConnectionDataDatabaseDriver
    {
        @Override
        public VolumeConnectionData load(
            Volume sourceVolume,
            Volume targetVolume,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            VolumeConnectionData volumeConnection = null;
            try
            {
                volumeConnection = (VolumeConnectionData) sourceVolume.getVolumeConnection(dbCtx, targetVolume);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                handleAccessDeniedException(accDeniedExc);
            }
            return volumeConnection;
        }

        @Override
        public List<VolumeConnectionData> loadAllByVolume(
            Volume volume,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return Collections.emptyList();
        }

        @Override
        public void create(VolumeConnectionData conDfnData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(VolumeConnectionData conDfnData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    public void handleAccessDeniedException(AccessDeniedException accDeniedExc)
    {
        throw new ImplementationError(
            "SatelliteDbDriver's accessContext has not enough privileges",
            accDeniedExc
        );
    }
}
