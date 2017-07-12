package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Before;
import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.storage.LvmDriver;
import com.linbit.drbdmanage.storage.StorageDriver;

public class NodeDataDerbyTest extends DerbyBase
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

    private final NodeName nodeName;

    private NodeDataDatabaseDriver dbDriver;
    private Connection con;
    private TransactionMgr transMgr;
    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private long initialFlags;
    private long initialTypes;
    private NodeData node;

    // TODO: bunch of tests for constraint checks
    // TODO: VolumeDefinitionsTest

    public NodeDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
    }

    @Before
    public void startUp() throws Exception
    {
        assertEquals("NODES table's column count has changed. Update tests accordingly!", 6, TBL_COL_COUNT_NODES);

        NodeDataDerbyDriver.clearCache();
        NetInterfaceDataDerbyDriver.clearCache();
        ResourceDataDerbyDriver.clearCache();

        dbDriver = new NodeDataDerbyDriver(sysCtx, nodeName);
        con = getConnection();
        transMgr = new TransactionMgr(con);

        uuid = randomUUID();
        objProt = ObjectProtection.getInstance(sysCtx, transMgr, ObjectProtection.buildPath(nodeName), true);
        initialFlags = NodeFlag.QIGNORE.flagValue;
        initialTypes = NodeType.AUXILIARY.getFlagValue();
        node = new NodeData(
            uuid,
            objProt,
            nodeName,
            initialTypes,
            initialFlags,
            null,
            transMgr
        );
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        node.initialized();
        dbDriver.create(con, node);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(NodeFlag.QIGNORE.flagValue, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
        assertEquals(ObjectProtection.buildPath(nodeName), resultSet.getString(OBJECT_PATH));

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }


    @Test
    public void testPersistGetInstance() throws Exception
    {
        NodeData.getInstance(
            sysCtx,
            nodeName,
            null,
            null,
            null,
            transMgr,
            true
        );
        con.commit();
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist NodeData instance", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(0, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
        assertEquals(ObjectProtection.buildPath(nodeName), resultSet.getString(OBJECT_PATH));
        assertFalse("Database contains too many datasets", resultSet.next());

        resultSet.close();
        stmt.close();

        stmt = con.prepareStatement(SELECT_ALL_RESOURCES_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent resource", resultSet.next());
        resultSet.close();
        stmt.close();

        stmt = con.prepareStatement(SELECT_ALL_NET_INTERFACES_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent net interface", resultSet.next());
        resultSet.close();
        stmt.close();

        stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent net interface", resultSet.next());
        resultSet.close();
        stmt.close();

        ObjectProtection loadedObjProt = ObjectProtection.getInstance(
            sysCtx,
            transMgr,
            ObjectProtection.buildPath(nodeName),
            false
        );
        assertNotNull("Database did not persist objectProtection", loadedObjProt);

        stmt = con.prepareStatement(SELECT_ALL_PROPS_FOR_NODE);
        stmt.setString(1, "NODES/" + nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent properties", resultSet.next());
        resultSet.close();
        stmt.close();
    }

    // TODO test if X is created without transMgr, and added to the node (should be persisted)

    @Test
    public void testUpdateFlags() throws Exception
    {
        insertNode(con, uuid, nodeName, 0, NodeType.AUXILIARY);
        transMgr.commit();

        NodeData loaded = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(loaded);
        loaded.setConnection(transMgr);
        loaded.getFlags().enableFlags(sysCtx, NodeFlag.REMOVE);
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database deleted NodeData", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(NodeFlag.REMOVE.flagValue, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
        assertEquals(ObjectProtection.buildPath(nodeName), resultSet.getString(OBJECT_PATH));
        assertFalse("Database contains too many datasets", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadSimple() throws Exception
    {
        dbDriver.create(con, node);

        NodeDataDerbyDriver.clearCache();

        NodeData loaded = dbDriver.load(con, null, transMgr);

        assertEquals(nodeName.value, loaded.getName().value);
        assertEquals(nodeName.displayValue, loaded.getName().displayValue);
        assertEquals(NodeFlag.QIGNORE.flagValue, loaded.getFlags().getFlagsBits(sysCtx));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), loaded.getNodeTypes(sysCtx));

    };

    @Test
    public void testLoadGetInstance() throws Exception
    {
        NodeData loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);
        assertNull(loadedNode);

        insertNode(con, uuid, nodeName, 0, NodeType.AUXILIARY);
        con.commit();
        NodeDataDerbyDriver.clearCache();

        loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(loadedNode);
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        assertEquals(0, loadedNode.getFlags().getFlagsBits(sysCtx));
        assertEquals(NodeType.AUXILIARY.getFlagValue(), loadedNode.getNodeTypes(sysCtx));
        assertEquals(1, loadedNode.getProps(sysCtx).size()); // serial number
    }

    @Test
    public void testLoadGetInstanceComplete() throws Exception
    {
        java.util.UUID nodeUuid = randomUUID();
        String nodeTestKey = "nodeTestKey";
        String nodeTestValue = "nodeTestValue";
        NodeId nodeId = new NodeId(13);

        NodeName nodeName2 = new NodeName("TestTargetNodeName");
        java.util.UUID node2Uuid = randomUUID();

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
        java.util.UUID conUuid = randomUUID();

        java.util.UUID volDfnUuid = randomUUID();
        VolumeNumber volDfnNr = new VolumeNumber(42);
        long volDfnSize = 5_000_000L;
        int volDfnMinorNr = 10;
        String volDfnTestKey = "volDfnTestKey";
        String volDfnTestValue = "volDfnTestValue";


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


        insertNode(con, nodeUuid, nodeName, NodeFlag.QIGNORE.getFlagValue(), NodeType.AUXILIARY);
        insertProp(con, PropsContainer.buildPath(nodeName), nodeTestKey, nodeTestValue);

        insertObjProt(con, ObjectProtection.buildPath(nodeName2), sysCtx);
        insertNode(con, node2Uuid, nodeName2, 0, NodeType.AUXILIARY);

        insertObjProt(con, ObjectProtection.buildPath(nodeName, netName), sysCtx);
        insertNetInterface(con, netIfUuid, nodeName, netName, netHost, netType);

        insertObjProt(con, ObjectProtection.buildPath(resName), sysCtx);
        insertResDfn(con, resDfnUuid, resName);

        insertConnDfn(con, conUuid, resName, nodeName, nodeName2);
        insertObjProt(con, ObjectProtection.buildPath(nodeName, resName), sysCtx);
        insertRes(con, resUuid, nodeName, resName, nodeId, Resource.RscFlags.CLEAN);
        insertProp(con, PropsContainer.buildPath(nodeName, resName), resTestKey, resTestValue);
        insertVolDfn(con, volDfnUuid, resName, volDfnNr, volDfnSize, volDfnMinorNr, VlmDfnFlags.REMOVE.flagValue);
        insertProp(con, PropsContainer.buildPath(resName, volDfnNr), volDfnTestKey, volDfnTestValue);
        insertVol(con, volUuid, nodeName, resName, volDfnNr, volTestBlockDev, Volume.VlmFlags.CLEAN);
        insertProp(con, PropsContainer.buildPath(nodeName, resName, volDfnNr), volTestKey, volTestValue);

        insertObjProt(con, ObjectProtection.buildPathSPD(poolName), sysCtx);
        insertStorPoolDfn(con, storPoolDfnId, poolName);

        insertObjProt(con, ObjectProtection.buildPathSP(poolName), sysCtx);
        insertStorPool(con, storPoolId, nodeName, poolName, driver);

        insertProp(con, storPoolPropsInstance, storPoolTestKey, storPoolTestValue);
        con.commit();

        DriverUtils.clearCaches(); // just to be sure

        NodeData loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(loadedNode);

        assertEquals(NodeFlag.QIGNORE.flagValue, loadedNode.getFlags().getFlagsBits(sysCtx));
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        {
            NetInterface netIf = loadedNode.getNetInterface(sysCtx, netName);
            assertNotNull(netIf);
            {
                InetAddress address = netIf.getAddress(sysCtx);
                assertNotNull(address);
                assertEquals(netHost, address.getHostAddress());
            }
            assertEquals(netName, netIf.getName());
            assertEquals(NetInterfaceType.byValue(netType), netIf.getNetInterfaceType(sysCtx));
            assertEquals(loadedNode, netIf.getNode());
            assertNotNull(netIf.getObjProt());
            assertEquals(netIfUuid, netIf.getUuid());
        }

        assertEquals(NodeType.AUXILIARY.getFlagValue(), loadedNode.getNodeTypes(sysCtx));
        assertNotNull(loadedNode.getObjProt());
        {
            Props nodeProps = loadedNode.getProps(sysCtx);
            assertNotNull(nodeProps);
            assertEquals(nodeTestValue, nodeProps.getProp(nodeTestKey));
            assertNotNull(nodeProps.getProp(SerialGenerator.KEY_SERIAL));
            assertEquals(2, nodeProps.size()); // serial number + testEntry
        }
        {
            Resource res = loadedNode.getResource(sysCtx, resName);
            assertNotNull(res);
            assertEquals(loadedNode, res.getAssignedNode());
            {
                ResourceDefinition resDfn = res.getDefinition();
                assertNotNull(resDfn);
//                ConnectionDefinition conDfn = resDfn.getConnectionDfn(sysCtx, nodeName, connNr);
//                assertNotNull(conDfn);
//                assertEquals(conUuid, conDfn.getUuid());
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
                Volume vol = res.getVolume(volDfnNr);
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
                assertEquals(res, vol.getResource());
                assertEquals(res.getDefinition(), vol.getResourceDfn());
                assertEquals(volUuid, vol.getUuid());
                {
                    VolumeDefinition volDfn = vol.getVolumeDfn();
                    assertTrue(volDfn.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
                    assertEquals(volDfnMinorNr, volDfn.getMinorNr(sysCtx).value);
                    {
                        Props volDfnProps = volDfn.getProps(sysCtx);
                        assertNotNull(volDfnProps);
                        assertEquals(volDfnTestValue, volDfnProps.getProp(volDfnTestKey));
                        assertNotNull(volDfnProps.getProp(SerialGenerator.KEY_SERIAL));
                        assertEquals(2, volDfnProps.size()); // serial number + testEntry
                    }
                    assertEquals(res.getDefinition(), volDfn.getResourceDfn());
                    assertEquals(volDfnUuid, volDfn.getUuid());
                    assertEquals(volDfnNr, volDfn.getVolumeNumber(sysCtx));
                    assertEquals(volDfnSize, volDfn.getVolumeSize(sysCtx));
                }
            }
        }

        {
            StorPool storPool = loadedNode.getStorPool(sysCtx, poolName);
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
        assertEquals(nodeUuid, loadedNode.getUuid());
    }

    @Test
    public void testDelete() throws Exception
    {
        dbDriver.create(con, node);
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(con, node);
        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        DriverUtils.satelliteMode();

    	SerialGenerator serGen = new TestSerialGenerator();
        NodeData nodeData = NodeData.getInstance(
            sysCtx,
            nodeName,
            null,
            null,
            serGen,
            null,
            true
        );

    	assertNotNull(nodeData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();

    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        DriverUtils.satelliteMode();

    	SerialGenerator serGen = new TestSerialGenerator();
    	NodeData nodeData = NodeData.getInstance(
    	    sysCtx,
    	    nodeName,
    	    null,
    	    null,
    	    serGen,
    	    null,
    	    false
	    );

    	assertNull(nodeData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        dbDriver.create(con, node);
        DriverUtils.clearCaches();

        NodeName halfValidName = new NodeName(node.getName().value);
        NodeDataDerbyDriver driver = new NodeDataDerbyDriver(sysCtx, halfValidName);
        NodeData loadedNode = driver.load(con, null, transMgr);

        assertNotNull(loadedNode);
        assertEquals(node.getName(), loadedNode.getName());
        assertEquals(node.getUuid(), loadedNode.getUuid());
    }
}
