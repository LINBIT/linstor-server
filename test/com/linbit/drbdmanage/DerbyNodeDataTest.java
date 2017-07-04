package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.storage.LvmDriver;
import com.linbit.drbdmanage.storage.StorageDriver;
import com.linbit.utils.UuidUtils;

public class DerbyNodeDataTest extends DerbyBase
{
    private static final String SELECT_ALL_NODES =
        " SELECT " + NODE_NAME + ", " + NODE_DSP_NAME + ", " + NODE_FLAGS + ", " + NODE_TYPE + ", " + OBJECT_PATH +
        " FROM " + TBL_NODES;
    private static final String SELECT_ALL_RESOURCES_FOR_NODE =
        " SELECT RES_DEF." + RESOURCE_NAME + ", RES_DEF." + RESOURCE_DSP_NAME +
        " FROM " + TBL_RESOURCE_DEFINITIONS + " AS RES_DEF " +
        " RIGHT JOIN " + TBL_NODE_RESOURCE + " ON " +
        "     " + TBL_NODE_RESOURCE + "." + RESOURCE_NAME + " = RES_DEF." + RESOURCE_NAME +
        " WHERE " + NODE_NAME + " = ?";
    private static final String SELECT_ALL_NET_INTERFACES_FOR_NODE =
        " SELECT " + NODE_NET_NAME + ", " + NODE_NET_DSP_NAME + ", " + INET_ADDRESS + ", " + INET_TRANSPORT_TYPE +
        " FROM " + TBL_NODE_NET_INTERFACES +
        " WHERE " + NODE_NAME + " = ?";
    private static final String SELECT_ALL_STOR_POOLS_FOR_NODE =
        " SELECT SPD." + POOL_NAME + ", SPD." + POOL_DSP_NAME + ", NSP." + DRIVER_NAME + ", NSP." + UUID +
        " FROM " + TBL_STOR_POOL_DEFINITIONS + " AS SPD" +
        " RIGHT JOIN " + TBL_NODE_STOR_POOL + " AS NSP ON " +
        "     NSP." + POOL_NAME + " = SPD." + POOL_NAME +
        " WHERE " + NODE_NAME + " = ?";
    private static final String SELECT_ALL_PROPS_FOR_NODE =
        " SELECT " + PROP_KEY + ", " + PROP_VALUE +
        " FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ?";

    public DerbyNodeDataTest() throws SQLException
    {
        super();
    }

    // TODO: bunch of tests for constraint checks
    // TODO: VolumeDefinitionsTest

    @Test
    public void testPersistSimple() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        String nodeName = "TestNodeName";
        NodeData.getInstance(sysCtx, new NodeName(nodeName), new HashSet<Node.NodeType>(), null, null, transMgr, true);
        con.commit();
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            assertEquals(nodeName.toUpperCase(), resultSet.getString(NODE_NAME));
            assertEquals(nodeName, resultSet.getString(NODE_DSP_NAME));
            assertEquals(0, resultSet.getLong(NODE_FLAGS));
            assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
            assertEquals("/nodes/" + nodeName.toUpperCase(), resultSet.getString(OBJECT_PATH));
        }
        else
        {
            fail("Database did not persist NodeData instance");
        }
        if (resultSet.next())
        {
            fail("Database contains too many datasets");
        }
        resultSet.close();
        stmt.close();

        stmt = con.prepareStatement(SELECT_ALL_RESOURCES_FOR_NODE);
        stmt.setString(1, nodeName.toUpperCase());
        resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            fail("Database persisted non existent resource");
        }
        resultSet.close();
        stmt.close();

        stmt = con.prepareStatement(SELECT_ALL_NET_INTERFACES_FOR_NODE);
        stmt.setString(1, nodeName.toUpperCase());
        resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            fail("Database persisted non existent net interface");
        }
        resultSet.close();
        stmt.close();

        stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS_FOR_NODE);
        stmt.setString(1, nodeName.toUpperCase());
        resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            fail("Database persisted non existent net interface");
        }
        resultSet.close();
        stmt.close();

        ObjectProtection objProt = ObjectProtection.getInstance(
            sysCtx,
            transMgr,
            ObjectProtection.buildPath(new NodeName(nodeName)),
            false
        );
        assertNotNull("Database did not persist objectProtection", objProt);

        stmt = con.prepareStatement(SELECT_ALL_PROPS_FOR_NODE);
        stmt.setString(1, "NODES/" + nodeName.toUpperCase());
        resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            fail("Database persisted non existent properties");
        }
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistResource() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        String nodeName = "TestNodeName";
        NodeData node = NodeData.getInstance(
            sysCtx,
            new NodeName(nodeName),
            new HashSet<Node.NodeType>(),
            null,
            null,
            transMgr,
            true
        );

        ResourceName resName = new ResourceName("TestRes");
        ResourceDefinitionData rdd = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            null,
            transMgr,
            true
        );

        Resource res = ResourceData.create(sysCtx, rdd, node, new NodeId(1), null, transMgr);
        node.addResource(sysCtx, res);

        con.commit();
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the ResDef and the Res got persisted
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCES_FOR_NODE);
        stmt.setString(1, nodeName.toUpperCase());
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
            assertEquals(resName.displayValue, resultSet.getString(RESOURCE_DSP_NAME));
        }
        else
        {
            fail("Database did not persist resource / resourceDefinition");
        }
        if (resultSet.next())
        {
            fail("Database persisted too many resources / resourceDefinitions");
        }
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistNetInterface() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        String nodeName = "TestNodeName";
        NodeData node = NodeData.getInstance(
            sysCtx,
            new NodeName(nodeName),
            new HashSet<Node.NodeType>(),
            null,
            null,
            transMgr,
            true
        );
        NetInterfaceName netInterfaceName = new NetInterfaceName("TestNetIface");
        String host = "127.0.0.1";
        int port = 1234;
        InetAddress inetAddress = new InetSocketAddress(host, port).getAddress();
        NetInterface niRef = new NetInterfaceData(
            sysCtx,
            node,
            netInterfaceName,
            inetAddress,
            transMgr,
            NetInterfaceType.IP
        );
        node.addNetInterface(sysCtx, niRef);

        con.commit();
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NET_INTERFACES_FOR_NODE);
        stmt.setString(1, nodeName.toUpperCase());
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            assertEquals(netInterfaceName.value, resultSet.getString(NODE_NET_NAME));
            assertEquals(netInterfaceName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
            assertEquals(host, resultSet.getString(INET_ADDRESS));
            assertEquals("IP", resultSet.getString(INET_TRANSPORT_TYPE));
            // transport: IP, RDMA, RoCE
        }
        else
        {
            fail("Database did not persist netInterface");
        }
        if (resultSet.next())
        {
            fail("Database persisted too many netInterfaces");
        }
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistStor() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        NodeName nodeName = new NodeName("TestNodeName");
        NodeData node = NodeData.getInstance(
            sysCtx,
            nodeName,
            new HashSet<Node.NodeType>(),
            null,
            null,
            transMgr,
            true
        );

        StorPoolName spName = new StorPoolName("TestStorPoolDefinition");
        StorPoolDefinitionData spdd = StorPoolDefinitionData.getInstance(
            sysCtx,
            spName,
            transMgr,
            true
        );

        StorPool pool = StorPoolData.getInstance(
            sysCtx,
            spdd,
            transMgr,
            null, // storageDriver
            LvmDriver.class.getSimpleName(),
            null, // serialGen
            node,
            true // create
        );
        node.addStorPool(sysCtx, pool);

        con.commit();
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS_FOR_NODE);
        stmt.setString(1, nodeName.value);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            assertEquals(pool.getName().value, resultSet.getString(POOL_NAME));
            assertEquals(pool.getName().displayValue, resultSet.getString(POOL_DSP_NAME));
            assertEquals(LvmDriver.class.getSimpleName(), resultSet.getString(DRIVER_NAME));
            assertArrayEquals(UuidUtils.asByteArray(pool.getUuid()), resultSet.getBytes(UUID));
        }
        else
        {
            fail("Database did not persist storPool");
        }
        if (resultSet.next())
        {
            fail("Database persisted too many storPools");
        }
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdateFlags() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        NodeName nodeName = new NodeName("TestNodeName");
        HashSet<NodeFlag> flags = new HashSet<>();
        flags.add(NodeFlag.REMOVE);
        NodeData.getInstance(sysCtx, nodeName, new HashSet<Node.NodeType>(), flags, null, transMgr, true);
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
            assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
            assertEquals(NodeFlag.REMOVE.flagValue, resultSet.getLong(NODE_FLAGS));
            assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
            assertEquals("/nodes/" + nodeName.value, resultSet.getString(OBJECT_PATH));
        }
        else
        {
            fail("Database did not persist NodeData");
        }
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdateTypes() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);
        NodeName nodeName = new NodeName("TestNodeName");
        HashSet<NodeType> types = new HashSet<Node.NodeType>();
        types.add(NodeType.CONTROLLER);
        types.add(NodeType.SATELLITE); // don't ask....
        NodeData.getInstance(sysCtx, nodeName, types, null, null, transMgr, true);
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
            assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
            assertEquals(0, resultSet.getLong(NODE_FLAGS));
            assertEquals(
                NodeType.CONTROLLER.getFlagValue() | NodeType.SATELLITE.getFlagValue(),
                resultSet.getInt(NODE_TYPE)
            );
            assertEquals("/nodes/" + nodeName.value, resultSet.getString(OBJECT_PATH));
        }
        else
        {
            fail("Database did not persist NodeData");
        }
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadSimple() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);

        NodeName nodeName = new NodeName("TestNodeName");

        java.util.UUID uuid = randomUUID();

        insertObjProt(con, ObjectProtection.buildPath(nodeName), sysCtx);
        insertNode(con, uuid, nodeName, 0, NodeType.AUXILIARY);
        con.commit();

        NodeData node = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(node);
        assertEquals(nodeName, node.getName()); // NodeName class implements equals
        assertEquals(0, node.getFlags().getFlagsBits(sysCtx));
        assertEquals(NodeType.AUXILIARY.getFlagValue(), node.getNodeTypes(sysCtx));
        assertEquals(1, node.getProps(sysCtx).size()); // serial number
    }

    @Test
    public void testLoadComplete() throws Throwable
    {
        Connection con = getConnection();
        TransactionMgr transMgr = new TransactionMgr(con);

        NodeName nodeName = new NodeName("TestNodeName");
        java.util.UUID nodeUuid = randomUUID();
        String nodeTestKey = "nodeTestKey";
        String nodeTestValue = "nodeTestValue";
        NodeId nodeId = new NodeId(13);

        java.util.UUID netIfUuid = randomUUID();
        NetInterfaceName netName = new NetInterfaceName("TestNetName");
        String netHost = "127.0.0.1";
        String netType = "IP";

        ResourceName resName = new ResourceName("TestResName");
        java.util.UUID resDfnUuid = randomUUID();
        java.util.UUID resUuid = randomUUID();
        String resTestKey = "resTestKey";
        String resTestValue = "resTestValue";

        int connNr = 1;

        java.util.UUID volDfnUuid = randomUUID();
        VolumeNumber volNr = new VolumeNumber(42);

        java.util.UUID volUuid = randomUUID();
        String volTestBlockDev = "/dev/do/not/use/me";
        String volTestKey = "volTestKey";
        String volTestValue = "volTestValue";

        java.util.UUID storPoolDfnId = randomUUID();
        StorPoolName poolName = new StorPoolName("TestPoolName");

        java.util.UUID storPoolId = randomUUID();
        String driver = LvmDriver.class.getSimpleName();

        String storPoolPropsInstance = PropsContainer.buildPath(
            poolName,
            nodeName
        );
        String storPoolTestKey = "storPoolTestKey";
        String storPoolTestValue = "storPoolTestValue";


        insertObjProt(con, ObjectProtection.buildPath(nodeName), sysCtx);
        insertNode(con, nodeUuid, nodeName, NodeFlag.QIGNORE.getFlagValue(), NodeType.AUXILIARY);
        insertProp(con, PropsContainer.buildPath(nodeName), nodeTestKey, nodeTestValue);

        insertObjProt(con, ObjectProtection.buildPath(nodeName, netName), sysCtx);
        insertNetInterface(con, netIfUuid, nodeName, netName, netHost, netType);

        insertObjProt(con, ObjectProtection.buildPath(resName), sysCtx);
        insertResDfn(con, resDfnUuid, resName);
        // TODO: gh - create db table for connectionDefinitions
        // TODO: gh - insertConnectionDfn
        insertObjProt(con, ObjectProtection.buildPath(nodeName, resName), sysCtx);
        insertRes(con, resUuid, nodeName, resName, nodeId, Resource.RscFlags.CLEAN);
        insertProp(con, PropsContainer.buildPath(nodeName, resName), resTestKey, resTestValue);
        // TODO: gh - insert stateFlags for resource
        insertVolDfn(con, volDfnUuid, resName, volNr, 5_000_000L, 10);
        insertVol(con, volUuid, nodeName, resName, volNr, volTestBlockDev, Volume.VlmFlags.CLEAN);
        insertProp(con, PropsContainer.buildPath(nodeName, resName, volNr), volTestKey, volTestValue);

        insertObjProt(con, ObjectProtection.buildPathSPD(poolName), sysCtx);
        insertStorPoolDfn(con, storPoolDfnId, poolName);

        insertObjProt(con, ObjectProtection.buildPathSP(poolName), sysCtx);
        insertStorPool(con, storPoolId, nodeName, poolName, driver);

        insertProp(con, storPoolPropsInstance, storPoolTestKey, storPoolTestValue);
        con.commit();

        NodeData node = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(node);
        assertEquals(NodeFlag.QIGNORE.flagValue, node.getFlags().getFlagsBits(sysCtx));
        assertEquals(nodeName, node.getName()); // NodeName class implements equals

        {
            NetInterface netIf = node.getNetInterface(sysCtx, netName);
            assertNotNull(netIf);
            {
                InetAddress address = netIf.getAddress(sysCtx);
                assertNotNull(address);
                assertEquals(netHost, address.getHostAddress());
            }
            assertEquals(netName, netIf.getName());
            assertEquals(NetInterfaceType.byValue(netType), netIf.getNetInterfaceType(sysCtx));
            assertEquals(node, netIf.getNode());
            assertNotNull(netIf.getObjProt());
            assertEquals(netIfUuid, netIf.getUuid());
        }

        assertEquals(NodeType.AUXILIARY.getFlagValue(), node.getNodeTypes(sysCtx));
        assertNotNull(node.getObjProt());
        {
            Props nodeProps = node.getProps(sysCtx);
            assertNotNull(nodeProps);
            assertEquals(nodeTestValue, nodeProps.getProp(nodeTestKey));
            assertNotNull(nodeProps.getProp(SerialGenerator.KEY_SERIAL));
            assertEquals(2, nodeProps.size()); // serial number + testEntry
        }
        {
            Resource res = node.getResource(sysCtx, resName);
            assertNotNull(res);
            assertEquals(node, res.getAssignedNode());
            {
                ResourceDefinition resDfn = res.getDefinition();
                assertNotNull(resDfn);
                ConnectionDefinition conDfn = resDfn.getConnectionDfn(sysCtx, nodeName, connNr);
                // TODO: gh - implement and test connections
            }
            assertEquals(nodeId, res.getNodeId());
            assertNotNull(res.getObjProt());
            {
                Props resProps = res.getProps(sysCtx);
                assertNotNull(resProps);

                assertEquals(resTestValue, resProps.getProp(resTestKey));
                assertNotNull(resProps.getProp(SerialGenerator.KEY_SERIAL));
                assertEquals(2, resProps.size()); // serial number + testEntry
            }
            {
                StateFlags<RscFlags> resStateFlags = res.getStateFlags();
                assertNotNull(resStateFlags);
                assertTrue(resStateFlags.isSet(sysCtx, RscFlags.CLEAN));
                assertFalse(resStateFlags.isSet(sysCtx, RscFlags.REMOVE));
            }
            assertEquals(resUuid, res.getUuid());
            {
                Volume vol = res.getVolume(volNr);
                assertNotNull(vol);
                {
                    StateFlags<VlmFlags> flags = vol.getFlags();
                    assertNotNull(flags);
                    flags.isSet(sysCtx, Volume.VlmFlags.CLEAN);
                }
                {
                    Props volProps = vol.getProps(sysCtx);
                    assertNotNull(volProps);
                    assertEquals(volTestValue, volProps.getProp(volTestKey));
                    assertNotNull(volProps.getProp(SerialGenerator.KEY_SERIAL));
                    assertEquals(2, volProps.size()); // serial number + testEntry
                }
            }
        }

        {
            StorPool storPool = node.getStorPool(sysCtx, poolName);
            assertNotNull(storPool);
            {
                Props storPoolConfig = storPool.getConfiguration(sysCtx);
                assertNotNull(storPoolConfig);
                assertEquals(storPoolTestValue, storPoolConfig.getProp(storPoolTestKey));
                assertNotNull(storPoolConfig.getProp(SerialGenerator.KEY_SERIAL));
                assertEquals(2, storPoolConfig.size()); // serial number + testEntry
            }
            {
                StorPoolDefinition storPoolDefinition = storPool.getDefinition(sysCtx);
                assertNotNull(storPoolDefinition);
                assertEquals(poolName, storPoolDefinition.getName());
                assertNotNull(storPoolDefinition.getObjProt());
                assertEquals(storPoolDfnId, storPoolDefinition.getUuid());
            }
            {
                StorageDriver storageDriver = storPool.getDriver(sysCtx);
                assertNull(storageDriver);
                // in controller storDriver HAS to be null (as we are testing database, we have to be testing the controller)
            }
            assertEquals(driver, storPool.getDriverName());
            assertEquals(poolName, storPool.getName());
            assertNotNull(storPool.getObjProt());
            assertEquals(storPoolId, storPool.getUuid());
        }
        assertEquals(nodeUuid, node.getUuid());
    }
}
