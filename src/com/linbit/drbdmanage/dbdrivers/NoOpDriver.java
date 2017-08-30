package com.linbit.drbdmanage.dbdrivers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ConnectionDefinitionData;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
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
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
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
    private static final ConnectionDefinitionDataDatabaseDriver NO_OP_CON_DFN_DRIVER = new NoOpConDfnDriver();

    private static final StateFlagsPersistence NO_OP_FLAG_DRIVER = new NoOpFlagDriver();
    private static final SingleColumnDatabaseDriver<?> NO_OP_OBJ_DB_DRIVER = new NoOpObjDbDriver<>();

    private static final String NO_OP_STRING = "NO_OP";

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
    public PropsConDatabaseDriver getPropsDatabaseDriver(String instanceName)
    {
        return NO_OP_PROPS_DRIVER;
    }

    @Override
    public NodeDataDatabaseDriver getNodeDatabaseDriver(NodeName nodeNameRef)
    {
        return NO_OP_NODE_DRIVER;
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver(NodeName nodeNameRef, ResourceName resName)
    {
        return NO_OP_RES_DRIVER;
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver(ResourceName resName)
    {
        return NO_OP_RES_DFN_DRIVER;
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver(Resource resRef, VolumeDefinition volDfnRef)
    {
        return NO_OP_VOL_DRIVER;
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        return NO_OP_VOL_DFN_DRIVER;
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver(StorPoolName name)
    {
        return NO_OP_SP_DRIVER;
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver(Node nodeRef, StorPoolDefinition storPoolDfnRef)
    {
        return NO_OP_SPD_DRIVER;
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver(Node node, NetInterfaceName name)
    {
        return NO_OP_NI_DRIVER;
    }

    @Override
    public ConnectionDefinitionDataDatabaseDriver getConnectionDefinitionDatabaseDriver(
        ResourceName resName,
        NodeName sourceNodeName,
        NodeName targetNodeName
    )
    {
        return NO_OP_CON_DFN_DRIVER;
    }

    private static class NoOpPropDriver implements PropsConDatabaseDriver
    {
        @Override
        public String getInstanceName()
        {
            return null;
        }

        @Override
        public Map<String, String> load(Connection con) throws SQLException
        {
            return null;
        }

        @Override
        public void persist(Connection con, String key, String value) throws SQLException
        {
            // no-op
        }

        @Override
        public void persist(Connection con, Map<String, String> props) throws SQLException
        {
            // no-op
        }

        @Override
        public void remove(Connection con, String key) throws SQLException
        {
            // no-op
        }

        @Override
        public void remove(Connection con, Set<String> keys) throws SQLException
        {
            // no-op
        }

        @Override
        public void removeAll(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpNodeDriver implements NodeDataDatabaseDriver
    {
        @Override
        public StateFlagsPersistence getStateFlagPersistence()
        {
            return NO_OP_FLAG_DRIVER;
        }

        @Override
        public StateFlagsPersistence getNodeTypeStateFlagPersistence()
        {
            return NO_OP_FLAG_DRIVER;
        }

        @Override
        public PropsConDatabaseDriver getPropsConDriver()
        {
            return NO_OP_PROPS_DRIVER;
        }

        @Override
        public void create(Connection con, NodeData nodeData) throws SQLException
        {
            // no-op
        }

        @Override
        public NodeData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr) throws SQLException
        {
            return null;
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpResDriver implements ResourceDataDatabaseDriver
    {
        @Override
        public StateFlagsPersistence getStateFlagPersistence()
        {
            return NO_OP_FLAG_DRIVER;
        }

        @Override
        public PropsConDatabaseDriver getPropsConDriver()
        {
            return NO_OP_PROPS_DRIVER;
        }

        @Override
        public void create(Connection dbCon, ResourceData resData) throws SQLException
        {
            // no-op
        }

        @Override
        public ResourceData load(Connection con, Node node, SerialGenerator serialGen, TransactionMgr transMgr)
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpResDfnDriver implements ResourceDefinitionDataDatabaseDriver
    {
        @Override
        public StateFlagsPersistence getStateFlagsPersistence()
        {
            return NO_OP_FLAG_DRIVER;
        }

        @Override
        public PropsConDatabaseDriver getPropsConDriver()
        {
            return NO_OP_PROPS_DRIVER;
        }

        @Override
        public void create(Connection dbCon, ResourceDefinitionData resDfn) throws SQLException
        {
            // no-op
        }

        @Override
        public boolean exists(Connection dbCon) throws SQLException
        {
            return false;
        }

        @Override
        public ResourceDefinitionData load(Connection dbCon, SerialGenerator serialGen, TransactionMgr transMgr)
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpVolDriver implements VolumeDataDatabaseDriver
    {
        @Override
        public StateFlagsPersistence getStateFlagsPersistence()
        {
            return NO_OP_FLAG_DRIVER;
        }

        @Override
        public PropsConDatabaseDriver getPropsConDriver()
        {
            return NO_OP_PROPS_DRIVER;
        }

        @Override
        public VolumeData load(Connection dbCon, SerialGenerator srlGen, TransactionMgr transMgr) throws SQLException
        {
            return null;
        }

        @Override
        public void create(Connection dbCon, VolumeData vol) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpVolDfnDriver implements VolumeDefinitionDataDatabaseDriver
    {
        @Override
        public StateFlagsPersistence getStateFlagsPersistence()
        {
            return NO_OP_FLAG_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<MinorNumber> getMinorNumberDriver()
        {
            return (SingleColumnDatabaseDriver<MinorNumber>) NO_OP_OBJ_DB_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<Long> getVolumeSizeDriver()
        {
            return (SingleColumnDatabaseDriver<Long>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public PropsConDatabaseDriver getPropsDriver()
        {
            return NO_OP_PROPS_DRIVER;
        }

        @Override
        public void create(Connection con, VolumeDefinitionData volDfnData) throws SQLException
        {
            // no-op
        }

        @Override
        public VolumeDefinitionData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr)
            throws SQLException
        {
            return null;
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpSpDriver implements StorPoolDefinitionDataDatabaseDriver
    {
        @Override
        public void create(Connection con, StorPoolDefinitionData storPoolDefinitionData) throws SQLException
        {
            // no-op
        }

        @Override
        public StorPoolDefinitionData load(Connection con) throws SQLException
        {
            return null;
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpSpdDriver implements StorPoolDataDatabaseDriver
    {
        @Override
        public StorPoolData load(Connection con, SerialGenerator serGen, TransactionMgr transMgr) throws SQLException
        {
            return null;
        }

        @Override
        public void create(Connection dbCon, StorPoolData storPoolData) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }

        @Override
        public void ensureEntryExists(Connection con, StorPoolData storPoolData) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpNiDriver implements NetInterfaceDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<DmIpAddress> getNetInterfaceAddressDriver()
        {
            return (SingleColumnDatabaseDriver<DmIpAddress>) NO_OP_OBJ_DB_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver()
        {
            return (SingleColumnDatabaseDriver<NetInterfaceType>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public NetInterfaceData load(Connection dbCon) throws SQLException
        {
            return null;
        }

        @Override
        public void create(Connection dbCon, NetInterfaceData netInterfaceData) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpFlagDriver implements StateFlagsPersistence
    {
        @Override
        public void persist(Connection dbConn, long flags) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpObjDbDriver<NOOP> implements SingleColumnDatabaseDriver<NOOP>
    {
        @Override
        public void update(Connection con, NOOP element) throws SQLException
        {
            // no-op
        }
    }

    private static class NoOpConDfnDriver implements ConnectionDefinitionDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<Integer> getConnectionNumberDriver()
        {
            return (SingleColumnDatabaseDriver<Integer>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public ConnectionDefinitionData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr)
            throws SQLException
        {
            return null;
        }

        @Override
        public void create(Connection con, ConnectionDefinitionData conDfnData) throws SQLException
        {
            // no-op
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            // no-op
        }
    }
}
