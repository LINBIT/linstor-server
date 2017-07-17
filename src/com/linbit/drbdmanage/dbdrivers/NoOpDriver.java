package com.linbit.drbdmanage.dbdrivers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.DrbdSqlRuntimeException;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.NetInterfaceDataDatabaseDriver;
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
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessDeniedException;
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

    private static final StateFlagsPersistence NO_OP_FLAG_DRIVER = new NoOpFlagDriver();
    private static final ObjectDatabaseDriver<?> NO_OP_OBJ_DB_DRIVER = new NoOpObjDbDriver<>();

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

    private static class NoOpPropDriver implements PropsConDatabaseDriver
    {
        @Override
        public String getInstanceName()
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public Map<String, String> load(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void persist(Connection con, String key, String value) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void persist(Connection con, Map<String, String> props) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void remove(Connection con, String key) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void remove(Connection con, Set<String> keys) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void removeAll(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
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
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public NodeData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con, NodeData node) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
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
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public ResourceData load(Connection con, Node node, SerialGenerator serialGen, TransactionMgr transMgr)
            throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con, ResourceData res) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
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
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public boolean exists(Connection dbCon) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public ResourceDefinitionData load(Connection dbCon, SerialGenerator serialGen, TransactionMgr transMgr)
            throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
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
        public VolumeData load(Connection dbCon, TransactionMgr transMgr, SerialGenerator srlGen) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void create(Connection dbCon, VolumeData vol) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
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
        public ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver()
        {
            return (ObjectDatabaseDriver<MinorNumber>) NO_OP_OBJ_DB_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public ObjectDatabaseDriver<Long> getVolumeSizeDriver()
        {
            return (ObjectDatabaseDriver<Long>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public PropsConDatabaseDriver getPropsDriver()
        {
            return NO_OP_PROPS_DRIVER;
        }

        @Override
        public void create(Connection con, VolumeDefinitionData volDfnData) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public VolumeDefinitionData load(Connection con, TransactionMgr transMgr, SerialGenerator serialGen)
            throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }
    }

    private static class NoOpSpDriver implements StorPoolDefinitionDataDatabaseDriver
    {
        @Override
        public void create(Connection con, StorPoolDefinitionData storPoolDefinitionData) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public StorPoolDefinitionData load(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }
    }

    private static class NoOpSpdDriver implements StorPoolDataDatabaseDriver
    {
        @Override
        public StorPoolData load(Connection con, TransactionMgr transMgr, SerialGenerator serGen) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void create(Connection dbCon, StorPoolData storPoolData) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void ensureEntryExists(Connection con, StorPoolData storPoolData) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }
    }

    private static class NoOpNiDriver implements NetInterfaceDataDatabaseDriver
    {
        @SuppressWarnings("unchecked")
        @Override
        public ObjectDatabaseDriver<DmIpAddress> getNetInterfaceAddressDriver()
        {
            return (ObjectDatabaseDriver<DmIpAddress>) NO_OP_OBJ_DB_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public ObjectDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver()
        {
            return (ObjectDatabaseDriver<NetInterfaceType>) NO_OP_OBJ_DB_DRIVER;
        }

        @Override
        public NetInterfaceData load(Connection dbCon) throws SQLException, AccessDeniedException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void create(Connection dbCon, NetInterfaceData netInterfaceData) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con, NetInterfaceData netInterfaceData) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }
    }

    private static class NoOpFlagDriver implements StateFlagsPersistence
    {
        @Override
        public void persist(Connection dbConn, long flags) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }
    }

    private static class NoOpObjDbDriver<NOOP> implements ObjectDatabaseDriver<NOOP>
    {
        @Override
        public void insert(Connection con, NOOP element) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void update(Connection con, NOOP element) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }

        @Override
        public void delete(Connection con, NOOP element) throws SQLException
        {
            throw new DrbdSqlRuntimeException("No-Op-Driver method called");
        }
    }
}
