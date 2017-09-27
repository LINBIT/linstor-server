package com.linbit.drbdmanage.dbdrivers;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ResourceConnectionData;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeConnectionData;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeConnectionData;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
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
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class NoOpDriver implements DatabaseDriver
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

    private static final PropsConDatabaseDriver NO_OP_PROPS_DRIVER = new NoOpPropDriver();
    private static final NodeDataDatabaseDriver NO_OP_NODE_DRIVER = new NoOpNodeDriver();
    private static final ResourceDataDatabaseDriver NO_OP_RES_DRIVER = new NoOpResDriver();
    private static final ResourceDefinitionDataDatabaseDriver NO_OP_RES_DFN_DRIVER = new NoOpResDfnDriver();
    private static final VolumeDataDatabaseDriver NO_OP_VOL_DRIVER = new NoOpVolDriver();
    private static final VolumeDefinitionDataDatabaseDriver NO_OP_VOL_DFN_DRIVER = new NoOpVolDfnDriver();
    private static final StorPoolDefinitionDataDatabaseDriver NO_OP_SP_DRIVER = new NoOpSpDriver();
    private static final StorPoolDataDatabaseDriver NO_OP_SPD_DRIVER = new NoOpSpdDriver();
    private static final NetInterfaceDataDatabaseDriver NO_OP_NI_DRIVER = new NoOpNiDriver();
    private static final NodeConnectionDataDatabaseDriver NO_OP_NODE_CON_DFN_DRIVER = new NoOpNodeConDfnDriver();
    private static final ResourceConnectionDataDatabaseDriver NO_OP_RES_CON_DFN_DRIVER = new NoOpResConDfnDriver();
    private static final VolumeConnectionDataDatabaseDriver NO_OP_VOL_CON_DFN_DRIVER = new NoOpVolConDfnDriver();

    private static final StateFlagsPersistence<?> NO_OP_FLAG_DRIVER = new NoOpFlagDriver();
    private static final SingleColumnDatabaseDriver<?, ?> NO_OP_OBJ_DB_DRIVER = new NoOpObjDbDriver<>();

    private static final String NO_OP_STRING = "NO_OP";

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
        return NO_OP_STRING;
    }

    @Override
    public PropsConDatabaseDriver getPropsDatabaseDriver()
    {
        return NO_OP_PROPS_DRIVER;
    }

    @Override
    public NodeDataDatabaseDriver getNodeDatabaseDriver()
    {
        return NO_OP_NODE_DRIVER;
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver()
    {
        return NO_OP_RES_DRIVER;
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver()
    {
        return NO_OP_RES_DFN_DRIVER;
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver()
    {
        return NO_OP_VOL_DRIVER;
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver()
    {
        return NO_OP_VOL_DFN_DRIVER;
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver()
    {
        return NO_OP_SP_DRIVER;
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver()
    {
        return NO_OP_SPD_DRIVER;
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver()
    {
        return NO_OP_NI_DRIVER;
    }

    @Override
    public NodeConnectionDataDatabaseDriver getNodeConnectionDataDatabaseDriver()
    {
        return NO_OP_NODE_CON_DFN_DRIVER;
    }

    @Override
    public ResourceConnectionDataDatabaseDriver getResourceConnectionDataDatabaseDriver()
    {
        return NO_OP_RES_CON_DFN_DRIVER;
    }

    @Override
    public VolumeConnectionDataDatabaseDriver getVolumeConnectionDataDatabaseDriver()
    {
        return NO_OP_VOL_CON_DFN_DRIVER;
    }

    private static class NoOpPropDriver implements PropsConDatabaseDriver
    {
        @Override
        public Map<String, String> load(String instanceName, TransactionMgr transMgr) throws SQLException
        {
            return null;
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

    private static class NoOpNodeDriver implements NodeDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<NodeData> getStateFlagPersistence()
        {
            return (StateFlagsPersistence<NodeData>) NO_OP_FLAG_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NodeData, NodeType> getNodeTypeDriver()
        {
            return (SingleColumnDatabaseDriver<NodeData, NodeType>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public void create(NodeData nodeData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public NodeData load(NodeName nodeName, TransactionMgr transMgr)
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(NodeData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpResDriver implements ResourceDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
        {
            return (StateFlagsPersistence<ResourceData>) NO_OP_FLAG_DRIVER;
        }

        @Override
        public void create(ResourceData resData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public ResourceData load(
            Node node,
            ResourceName resourceName,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(ResourceData resourceData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpResDfnDriver implements ResourceDefinitionDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
        {
            return (StateFlagsPersistence<ResourceDefinitionData>) NO_OP_FLAG_DRIVER;
        }

        @Override
        public void create(ResourceDefinitionData resDfn, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public boolean exists(ResourceName resourceName, TransactionMgr transMgr) throws SQLException
        {
            return false;
        }

        @Override
        public ResourceDefinitionData load(ResourceName resourceName,TransactionMgr transMgr)
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(ResourceDefinitionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpVolDriver implements VolumeDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<VolumeData> getStateFlagsPersistence()
        {
            return (StateFlagsPersistence<VolumeData>) NO_OP_FLAG_DRIVER;
        }

        @Override
        public VolumeData load(
            Resource resource,
            VolumeDefinition volumeDefinition,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
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

    private static class NoOpVolDfnDriver implements VolumeDefinitionDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence()
        {
            return (StateFlagsPersistence<VolumeDefinitionData>) NO_OP_FLAG_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber> getMinorNumberDriver()
        {
            return (SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber>) NO_OP_OBJ_DB_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver()
        {
            return (SingleColumnDatabaseDriver<VolumeDefinitionData, Long>) NO_OP_OBJ_DB_DRIVER;
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
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(VolumeDefinitionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpSpDriver implements StorPoolDefinitionDataDatabaseDriver
    {
        @Override
        public void create(StorPoolDefinitionData storPoolDefinitionData, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public StorPoolDefinitionData load(StorPoolName storPoolName, TransactionMgr transMgr) throws SQLException
        {
            return null;
        }

        @Override
        public void delete(StorPoolDefinitionData data, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpSpdDriver implements StorPoolDataDatabaseDriver
    {
        @Override
        public StorPoolData load(
            Node node,
            StorPoolDefinition storPoolDefinition,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
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

    private static class NoOpNiDriver implements NetInterfaceDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress> getNetInterfaceAddressDriver()
        {
            return (SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress>) NO_OP_OBJ_DB_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> getNetInterfaceTypeDriver()
        {
            return (SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public NetInterfaceData load(
            Node node,
            NetInterfaceName netInterfaceName,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
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

    private static class NoOpFlagDriver implements StateFlagsPersistence<Object>
    {
        @Override
        public void persist(Object parent, long flags, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpObjDbDriver<NOOP_KEY, NOOP> implements SingleColumnDatabaseDriver<NOOP_KEY, NOOP>
    {
        @Override
        public void update(NOOP_KEY parent, NOOP element, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpNodeConDfnDriver implements NodeConnectionDataDatabaseDriver
    {
        @Override
        public NodeConnectionData load(
            Node sourceNode,
            Node targetNode,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
        }

        @Override
        public List<NodeConnectionData> loadAllByNode(
            Node node,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
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

    private static class NoOpResConDfnDriver implements ResourceConnectionDataDatabaseDriver
    {
        @Override
        public ResourceConnectionData load(
            Resource source,
            Resource target,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
        }

        @Override
        public List<ResourceConnectionData> loadAllByResource(
            Resource resource,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
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

    private static class NoOpVolConDfnDriver implements VolumeConnectionDataDatabaseDriver
    {
        @Override
        public VolumeConnectionData load(
            Volume sourceVolume,
            Volume targetVolume,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
        }

        @Override
        public List<VolumeConnectionData> loadAllByVolume(
            Volume volume,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return null;
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
}
