package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.core.CoreUtils;
import com.linbit.drbdmanage.core.DrbdManage;
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
    private TransactionMgr transMgr;
    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private long initialFlags;
    private NodeType initialType;
    private NodeData node;

    public NodeDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals("NODES table's column count has changed. Update tests accordingly!", 6, TBL_COL_COUNT_NODES);

        dbDriver = DrbdManage.getNodeDataDatabaseDriver();
        transMgr = new TransactionMgr(getConnection());

        uuid = randomUUID();
        objProt = ObjectProtection.getInstance(sysCtx, ObjectProtection.buildPath(nodeName), true, transMgr);
        initialFlags = NodeFlag.QIGNORE.flagValue;
        initialType = NodeType.AUXILIARY;
        node = new NodeData(
            uuid,
            objProt,
            nodeName,
            initialType,
            initialFlags,
            null,
            transMgr
        );
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        node.initialized();
        dbDriver.create(node, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
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
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
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

        stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCES_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent resource", resultSet.next());
        resultSet.close();
        stmt.close();

        stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NET_INTERFACES_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent net interface", resultSet.next());
        resultSet.close();
        stmt.close();

        stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent net interface", resultSet.next());
        resultSet.close();
        stmt.close();

        ObjectProtection loadedObjProt = ObjectProtection.getInstance(
            sysCtx,
            ObjectProtection.buildPath(nodeName),
            false,
            transMgr
        );
        assertNotNull("Database did not persist objectProtection", loadedObjProt);

        stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_PROPS_FOR_NODE);
        stmt.setString(1, "NODES/" + nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent properties", resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdateFlags() throws Exception
    {
        insertNode(transMgr, uuid, nodeName, 0, NodeType.AUXILIARY);
        transMgr.commit();

        NodeData loaded = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(loaded);
        loaded.setConnection(transMgr);
        loaded.getFlags().enableFlags(sysCtx, NodeFlag.REMOVE);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
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
        dbDriver.create(node, transMgr);

        NodeData loaded = dbDriver.load(nodeName, null, transMgr);

        assertEquals(nodeName.value, loaded.getName().value);
        assertEquals(nodeName.displayValue, loaded.getName().displayValue);
        assertEquals(NodeFlag.QIGNORE.flagValue, loaded.getFlags().getFlagsBits(sysCtx));
        assertEquals(Node.NodeType.AUXILIARY, loaded.getNodeType(sysCtx));

    };

    @Test
    public void testLoadGetInstance() throws Exception
    {
        NodeData loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);
        assertNull(loadedNode);

        insertNode(transMgr, uuid, nodeName, 0, NodeType.AUXILIARY);
        transMgr.commit();

        loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);

        assertNotNull(loadedNode);
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        assertEquals(0, loadedNode.getFlags().getFlagsBits(sysCtx));
        assertEquals(NodeType.AUXILIARY, loadedNode.getNodeType(sysCtx));
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
        String resDfnTestKey = "resDfnTestKey";
        String resDfnTestValue = "resDfnTestValue";

        java.util.UUID resUuid = randomUUID();
        String resTestKey = "resTestKey";
        String resTestValue = "resTestValue";

        int conNr = 1;
        java.util.UUID conUuid = randomUUID();

        java.util.UUID volDfnUuid = randomUUID();
        VolumeNumber volDfnNr = new VolumeNumber(42);
        long volDfnSize = 5_000_000L;
        int volDfnMinorNr = 10;
        String volDfnTestKey = "volDfnTestKey";
        String volDfnTestValue = "volDfnTestValue";


        java.util.UUID volUuid = randomUUID();
        String volTestBlockDev = "/dev/do/not/use/me";
        String volTestMetaDisk = "/dev/do/not/use/me/neither";
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

        // node(1)'s objProt already created in startUp method
        insertNode(transMgr, nodeUuid, nodeName, NodeFlag.QIGNORE.getFlagValue(), NodeType.AUXILIARY);
        insertProp(transMgr, PropsContainer.buildPath(nodeName), nodeTestKey, nodeTestValue);

        insertObjProt(transMgr, ObjectProtection.buildPath(nodeName2), sysCtx);
        insertNode(transMgr, node2Uuid, nodeName2, 0, NodeType.AUXILIARY);

        insertObjProt(transMgr, ObjectProtection.buildPath(nodeName, netName), sysCtx);
        insertNetInterface(transMgr, netIfUuid, nodeName, netName, netHost, netType);

        insertObjProt(transMgr, ObjectProtection.buildPath(resName), sysCtx);
        insertResDfn(transMgr, resDfnUuid, resName, RscDfnFlags.REMOVE);
        insertProp(transMgr, PropsContainer.buildPath(resName), resDfnTestKey, resDfnTestValue);

        insertObjProt(transMgr, ObjectProtection.buildPath(resName, nodeName, nodeName2), sysCtx);
        insertConnDfn(transMgr, conUuid, resName, nodeName, nodeName2, conNr);

        insertObjProt(transMgr, ObjectProtection.buildPath(nodeName, resName), sysCtx);
        insertRes(transMgr, resUuid, nodeName, resName, nodeId, Resource.RscFlags.CLEAN);
        insertProp(transMgr, PropsContainer.buildPath(nodeName, resName), resTestKey, resTestValue);

        insertVolDfn(transMgr, volDfnUuid, resName, volDfnNr, volDfnSize, volDfnMinorNr, VlmDfnFlags.REMOVE.flagValue);
        insertProp(transMgr, PropsContainer.buildPath(resName, volDfnNr), volDfnTestKey, volDfnTestValue);

        insertVol(transMgr, volUuid, nodeName, resName, volDfnNr, volTestBlockDev, volTestMetaDisk, Volume.VlmFlags.CLEAN);
        insertProp(transMgr, PropsContainer.buildPath(nodeName, resName, volDfnNr), volTestKey, volTestValue);

        insertObjProt(transMgr, ObjectProtection.buildPathSPD(poolName), sysCtx);
        insertStorPoolDfn(transMgr, storPoolDfnId, poolName);

        insertObjProt(transMgr, ObjectProtection.buildPathSP(poolName), sysCtx);
        insertStorPool(transMgr, storPoolId, nodeName, poolName, driver);

        insertProp(transMgr, storPoolPropsInstance, storPoolTestKey, storPoolTestValue);
        transMgr.commit();

        clearCaches();

        NodeData loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, false);
        NodeData loadedNode2 = NodeData.getInstance(sysCtx, nodeName2, null, null, null, transMgr, false);

        assertNotNull(loadedNode);

        assertEquals(NodeFlag.QIGNORE.flagValue, loadedNode.getFlags().getFlagsBits(sysCtx));
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        {
            NetInterface netIf = loadedNode.getNetInterface(sysCtx, netName);
            assertNotNull(netIf);
            {
                DmIpAddress address = netIf.getAddress(sysCtx);
                assertNotNull(address);
                assertEquals(netHost, address.getAddress());
            }
            assertEquals(netName, netIf.getName());
            assertEquals(NetInterfaceType.byValue(netType), netIf.getNetInterfaceType(sysCtx));
            assertEquals(loadedNode, netIf.getNode());
            assertNotNull(netIf.getObjProt());
            assertEquals(netIfUuid, netIf.getUuid());
        }

        assertEquals(NodeType.AUXILIARY, loadedNode.getNodeType(sysCtx));
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
                {
                    ConnectionDefinition conDfn = resDfn.getConnectionDfn(sysCtx, nodeName, conNr);
                    assertNotNull(conDfn);
                    assertEquals(conUuid, conDfn.getUuid());
                    assertEquals(conNr, conDfn.getConnectionNumber(sysCtx));
                    assertEquals(resDfn, conDfn.getResourceDefinition(sysCtx));
                    assertEquals(loadedNode, conDfn.getSourceNode(sysCtx));
                    assertEquals(loadedNode2, conDfn.getTargetNode(sysCtx));
                }
                assertEquals(RscDfnFlags.REMOVE.flagValue, resDfn.getFlags().getFlagsBits(sysCtx));
                assertEquals(resName, resDfn.getName());
                assertNotNull(resDfn.getObjProt());
                {
                    Props resDfnProps = resDfn.getProps(sysCtx);
                    assertNotNull(resDfnProps);

                    assertEquals(resDfnTestValue, resDfnProps.getProp(resDfnTestKey));
                    assertNotNull(resDfnProps.getProp(SerialGenerator.KEY_SERIAL));
                    assertEquals(2, resDfnProps.size()); // serial number + testEntry
                }
                assertEquals(res, resDfn.getResource(sysCtx, nodeName));
                assertEquals(resDfnUuid, resDfn.getUuid());
                assertEquals(res.getVolume(volDfnNr).getVolumeDefinition(), resDfn.getVolumeDfn(sysCtx, volDfnNr));
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
                assertEquals(res.getDefinition(), vol.getResourceDefinition());
                assertEquals(volUuid, vol.getUuid());
                {
                    VolumeDefinition volDfn = vol.getVolumeDefinition();
                    assertTrue(volDfn.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
                    assertEquals(volDfnMinorNr, volDfn.getMinorNr(sysCtx).value);
                    {
                        Props volDfnProps = volDfn.getProps(sysCtx);
                        assertNotNull(volDfnProps);
                        assertEquals(volDfnTestValue, volDfnProps.getProp(volDfnTestKey));
                        assertNotNull(volDfnProps.getProp(SerialGenerator.KEY_SERIAL));
                        assertEquals(2, volDfnProps.size()); // serial number + testEntry
                    }
                    assertEquals(res.getDefinition(), volDfn.getResourceDefinition());
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
    public void testCache() throws Exception
    {
        dbDriver.create(node, transMgr);

        // no clearCaches

        assertEquals(node, dbDriver.load(nodeName, null, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        dbDriver.create(node, transMgr);
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(node, transMgr);
        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        CoreUtils.satelliteMode();

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

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();

    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        CoreUtils.satelliteMode();

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

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        dbDriver.create(node, transMgr);

        NodeName halfValidName = new NodeName(node.getName().value);
        NodeData loadedNode = dbDriver.load(halfValidName, null, transMgr);

        assertNotNull(loadedNode);
        assertEquals(node.getName(), loadedNode.getName());
        assertEquals(node.getUuid(), loadedNode.getUuid());
    }
}
