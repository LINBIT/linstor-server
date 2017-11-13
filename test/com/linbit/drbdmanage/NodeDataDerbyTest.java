package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.storage.LvmDriver;
import com.linbit.drbdmanage.storage.StorageDriver;

public class NodeDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_NODES =
        " SELECT " + NODE_NAME + ", " + NODE_DSP_NAME + ", " + NODE_FLAGS + ", " + NODE_TYPE +
        " FROM " + TBL_NODES;
    private static final String SELECT_ALL_RESOURCES_FOR_NODE =
        " SELECT RES_DEF." + RESOURCE_NAME + ", RES_DEF." + RESOURCE_DSP_NAME +
        " FROM " + TBL_RESOURCE_DEFINITIONS + " AS RES_DEF " +
        " RIGHT JOIN " + TBL_RESOURCES + " ON " +
        "     " + TBL_RESOURCES + "." + RESOURCE_NAME + " = RES_DEF." + RESOURCE_NAME +
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

    private NodeDataDerbyDriver dbDriver;
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
        assertEquals("NODES table's column count has changed. Update tests accordingly!", 5, TBL_COL_COUNT_NODES);

        dbDriver = (NodeDataDerbyDriver) DrbdManage.getNodeDataDatabaseDriver();
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
            transMgr,
            true,
            false
        );
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist NodeData instance", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(0, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
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

        NodeData loaded = NodeData.getInstance(sysCtx, nodeName, null, null, transMgr, false, false);

        assertNotNull(loaded);
        loaded.setConnection(transMgr);
        loaded.getFlags().enableFlags(sysCtx, NodeFlag.DELETE);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database deleted NodeData", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(NodeFlag.DELETE.flagValue, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
        assertFalse("Database contains too many datasets", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadSimple() throws Exception
    {
        dbDriver.create(node, transMgr);

        NodeData loaded = dbDriver.load(nodeName, true, transMgr);

        assertEquals(nodeName.value, loaded.getName().value);
        assertEquals(nodeName.displayValue, loaded.getName().displayValue);
        assertEquals(NodeFlag.QIGNORE.flagValue, loaded.getFlags().getFlagsBits(sysCtx));
        assertEquals(Node.NodeType.AUXILIARY, loaded.getNodeType(sysCtx));

    };

    @Test
    public void testLoadGetInstance() throws Exception
    {
        NodeData loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, transMgr, false, false);
        assertNull(loadedNode);

        insertNode(transMgr, uuid, nodeName, 0, NodeType.AUXILIARY);
        transMgr.commit();

        loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, transMgr, false, false);

        assertNotNull(loadedNode);
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        assertEquals(0, loadedNode.getFlags().getFlagsBits(sysCtx));
        assertEquals(NodeType.AUXILIARY, loadedNode.getNodeType(sysCtx));
        assertEquals(0, loadedNode.getProps(sysCtx).size()); // serial number
    }

    @Test
    public void testLoadGetInstanceComplete() throws Exception
    {
        // node1
        java.util.UUID node1Uuid;
        String node1TestKey = "nodeTestKey";
        String node1TestValue = "nodeTestValue";

        // node1 netName
        java.util.UUID netIfUuid;
        NetInterfaceName netName = new NetInterfaceName("TestNetName");
        String netHost = "127.0.0.1";
        String netType = "IP";
        int netPort = 9001;

        // node2
        NodeName nodeName2 = new NodeName("TestTargetNodeName");

        // resDfn
        ResourceName resName = new ResourceName("TestResName");
        java.util.UUID resDfnUuid;
        TcpPortNumber resPort = new TcpPortNumber(9001);
        String resDfnTestKey = "resDfnTestKey";
        String resDfnTestValue = "resDfnTestValue";

        // node1 res
        java.util.UUID res1Uuid;
        String res1TestKey = "res1TestKey";
        String res1TestValue = "res1TestValue";
        NodeId node1Id = new NodeId(13);

        // node2 res
        java.util.UUID res2Uuid;
        String res2TestKey = "res2TestKey";
        String res2TestValue = "res2TestValue";
        NodeId node2Id = new NodeId(14);

        // volDfn
        java.util.UUID volDfnUuid;
        VolumeNumber volDfnNr = new VolumeNumber(42);
        long volDfnSize = 5_000_000L;
        int volDfnMinorNr = 10;
        String volDfnTestKey = "volDfnTestKey";
        String volDfnTestValue = "volDfnTestValue";

        // node1 vol
        java.util.UUID vol1Uuid;
        String vol1TestBlockDev = "/dev/do/not/use/me1";
        String vol1TestMetaDisk = "/dev/do/not/use/me1/neither";
        String vol1TestKey = "vol1TestKey";
        String vol1TestValue = "vol1TestValue";

        // node1 vol
        java.util.UUID vol2Uuid;
        String vol2TestBlockDev = "/dev/do/not/use/me2";
        String vol2TestMetaDisk = "/dev/do/not/use/me2/neither";
        String vol2TestKey = "vol2TestKey";
        String vol2TestValue = "vol2TestValue";

        // storPoolDfn
        java.util.UUID storPoolDfnUuid;
        StorPoolName poolName = new StorPoolName("TestPoolName");

        // storPool
        String storPoolDriver1 = LvmDriver.class.getSimpleName();
        String storPool1TestKey = "storPool1TestKey";
        String storPool1TestValue = "storPool1TestValue";

        // storPool
        String storPoolDriver2 = LvmDriver.class.getSimpleName();
        String storPool2TestKey = "storPool2TestKey";
        String storPool2TestValue = "storPool2TestValue";

        // nodeCon
        java.util.UUID nodeConUuid;
        String nodeConTestKey = "nodeConTestKey";
        String nodeConTestValue = "nodeConTestValue";

        // resCon
        java.util.UUID resConUuid;
        String resConTestKey = "resConTestKey";
        String resConTestValue = "resConTestValue";

        // volCon
        java.util.UUID volConUuid;
        String volConTestKey = "volConTestKey";
        String volConTestValue = "volConTestValue";

        {
            // node1
            NodeData node1 = NodeData.getInstance(
                sysCtx,
                nodeName,
                NodeType.AUXILIARY,
                new NodeFlag[] {NodeFlag.QIGNORE},
                transMgr,
                true,
                true
            );
            node1.getProps(sysCtx).setProp(node1TestKey, node1TestValue);
            node1Uuid = node1.getUuid();

            // node1's netIface
            NetInterfaceData netIf = NetInterfaceData.getInstance(
                sysCtx,
                node1,
                netName,
                new DmIpAddress(netHost),
                netPort,
                NetInterfaceType.IP,
                transMgr,
                true,
                true
            );
            netIfUuid = netIf.getUuid();

            // node2
            NodeData node2 = NodeData.getInstance(
                sysCtx,
                nodeName2,
                NodeType.AUXILIARY,
                null,
                transMgr,
                true,
                true
            );

            // resDfn
            ResourceDefinitionData resDfn = ResourceDefinitionData.getInstance(
                sysCtx,
                resName,
                resPort,
                new RscDfnFlags[] {RscDfnFlags.DELETE},
                transMgr,
                true,
                true
            );
            resDfn.getProps(sysCtx).setProp(resDfnTestKey, resDfnTestValue);
            resDfnUuid = resDfn.getUuid();

            // volDfn
            VolumeDefinitionData volDfn = VolumeDefinitionData.getInstance(
                sysCtx,
                resDfn,
                volDfnNr,
                new MinorNumber(volDfnMinorNr),
                volDfnSize,
                new VlmDfnFlags[] {VlmDfnFlags.DELETE},
                transMgr,
                true,
                true
            );
            volDfn.getProps(sysCtx).setProp(volDfnTestKey, volDfnTestValue);
            volDfnUuid = volDfn.getUuid();

            // storPoolDfn
            StorPoolDefinitionData storPoolDfn = StorPoolDefinitionData.getInstance(
                sysCtx,
                poolName,
                transMgr,
                true,
                true
            );
            storPoolDfnUuid = storPoolDfn.getUuid();

            // node1 storPool
            StorPoolData storPool1 = StorPoolData.getInstance(
                sysCtx,
                node1,
                storPoolDfn,
                storPoolDriver1,
                transMgr,
                false,
                true,
                true
            );
            storPool1.getConfiguration(sysCtx).setProp(storPool1TestKey, storPool1TestValue);

            // node2 storPool
            StorPoolData storPool2 = StorPoolData.getInstance(
                sysCtx,
                node2,
                storPoolDfn,
                storPoolDriver2,
                transMgr,
                false,
                true,
                true
            );
            storPool2.getConfiguration(sysCtx).setProp(storPool2TestKey, storPool2TestValue);

            // node1 res
            ResourceData res1 = ResourceData.getInstance(
                sysCtx,
                resDfn,
                node1,
                node1Id,
                new RscFlags[] {RscFlags.CLEAN},
                transMgr,
                true,
                true
            );
            res1.getProps(sysCtx).setProp(res1TestKey, res1TestValue);
            res1Uuid = res1.getUuid();

            // node1 vol
            VolumeData vol1 = VolumeData.getInstance(
                sysCtx,
                res1,
                volDfn,
                storPool1,
                vol1TestBlockDev,
                vol1TestMetaDisk,
                new VlmFlags[] {VlmFlags.CLEAN},
                transMgr,
                true,
                true
            );
            vol1.getProps(sysCtx).setProp(vol1TestKey, vol1TestValue);
            vol1Uuid = vol1.getUuid();

            // node2 res
            ResourceData res2 = ResourceData.getInstance(
                sysCtx,
                resDfn,
                node2,
                node2Id,
                new RscFlags[] {RscFlags.CLEAN},
                transMgr,
                true,
                true
            );
            res2.getProps(sysCtx).setProp(res2TestKey, res2TestValue);
            res2Uuid = res2.getUuid();

            // node2 vol
            VolumeData vol2 = VolumeData.getInstance(
                sysCtx,
                res2,
                volDfn,
                storPool2,
                vol2TestBlockDev,
                vol2TestMetaDisk,
                new VlmFlags[] {VlmFlags.CLEAN},
                transMgr,
                true,
                true
            );
            vol2.getProps(sysCtx).setProp(vol2TestKey, vol2TestValue);
            vol2Uuid = vol2.getUuid();

            // nodeCon node1 <-> node2
            NodeConnectionData nodeCon = NodeConnectionData.getInstance(
                sysCtx,
                node1,
                node2,
                transMgr,
                true,
                true
            );
            nodeCon.getProps(sysCtx).setProp(nodeConTestKey, nodeConTestValue);
            nodeConUuid = nodeCon.getUuid();

            // resCon res1 <-> res2
            ResourceConnectionData resCon = ResourceConnectionData.getInstance(
                sysCtx,
                res1,
                res2,
                transMgr,
                true,
                true
            );
            resCon.getProps(sysCtx).setProp(resConTestKey, resConTestValue);
            resConUuid = resCon.getUuid();

            // volCon vol1 <-> vol2
            VolumeConnectionData volCon = VolumeConnectionData.getInstance(
                sysCtx,
                vol1,
                vol2,
                transMgr,
                true,
                true
            );
            volCon.getProps(sysCtx).setProp(volConTestKey, volConTestValue);
            volConUuid = volCon.getUuid();

            transMgr.commit();
        }
//        clearCaches();

        NodeData loadedNode = NodeData.getInstance(sysCtx, nodeName, null, null, transMgr, false, false);
        NodeData loadedNode2 = NodeData.getInstance(sysCtx, nodeName2, null, null, transMgr, false, false);

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
                assertEquals(netPort, netIf.getNetInterfacePort(sysCtx));
            }
            assertEquals(netName, netIf.getName());
            assertEquals(NetInterfaceType.byValue(netType), netIf.getNetInterfaceType(sysCtx));
            assertEquals(loadedNode, netIf.getNode());
            assertEquals(netIfUuid, netIf.getUuid());
        }

        assertEquals(NodeType.AUXILIARY, loadedNode.getNodeType(sysCtx));
        assertNotNull(loadedNode.getObjProt());
        {
            Props nodeProps = loadedNode.getProps(sysCtx);
            assertNotNull(nodeProps);
            assertEquals(node1TestValue, nodeProps.getProp(node1TestKey));
            assertEquals(1, nodeProps.size());
        }
        {
            Resource res = loadedNode.getResource(sysCtx, resName);
            assertNotNull(res);
            assertEquals(loadedNode, res.getAssignedNode());
            {
                ResourceDefinition resDfn = res.getDefinition();
                assertNotNull(resDfn);
                assertEquals(RscDfnFlags.DELETE.flagValue, resDfn.getFlags().getFlagsBits(sysCtx));
                assertEquals(resName, resDfn.getName());
                assertEquals(resPort, resDfn.getPort(sysCtx));
                assertNotNull(resDfn.getObjProt());
                {
                    Props resDfnProps = resDfn.getProps(sysCtx);
                    assertNotNull(resDfnProps);

                    assertEquals(resDfnTestValue, resDfnProps.getProp(resDfnTestKey));
                    assertEquals(1, resDfnProps.size());
                }
                assertEquals(res, resDfn.getResource(sysCtx, nodeName));
                assertEquals(resDfnUuid, resDfn.getUuid());
                assertEquals(res.getVolume(volDfnNr).getVolumeDefinition(), resDfn.getVolumeDfn(sysCtx, volDfnNr));
            }
            assertEquals(node1Id, res.getNodeId());
            assertNotNull(res.getObjProt());
            {
                Props resProps = res.getProps(sysCtx);
                assertNotNull(resProps);

                assertEquals(res1TestValue, resProps.getProp(res1TestKey));
                assertEquals(1, resProps.size());
            }
            {
                StateFlags<RscFlags> resStateFlags = res.getStateFlags();
                assertNotNull(resStateFlags);
                assertTrue(resStateFlags.isSet(sysCtx, RscFlags.CLEAN));
                assertFalse(resStateFlags.isSet(sysCtx, RscFlags.DELETE));
            }
            assertEquals(res1Uuid, res.getUuid());
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
                    assertEquals(vol1TestValue, volProps.getProp(vol1TestKey));
                    assertEquals(1, volProps.size());
                }
                assertEquals(res, vol.getResource());
                assertEquals(res.getDefinition(), vol.getResourceDefinition());
                assertEquals(vol1Uuid, vol.getUuid());
                {
                    VolumeDefinition volDfn = vol.getVolumeDefinition();
                    assertTrue(volDfn.getFlags().isSet(sysCtx, VlmDfnFlags.DELETE));
                    assertEquals(volDfnMinorNr, volDfn.getMinorNr(sysCtx).value);
                    {
                        Props volDfnProps = volDfn.getProps(sysCtx);
                        assertNotNull(volDfnProps);
                        assertEquals(volDfnTestValue, volDfnProps.getProp(volDfnTestKey));
                        assertEquals(1, volDfnProps.size());
                    }
                    assertEquals(res.getDefinition(), volDfn.getResourceDefinition());
                    assertEquals(volDfnUuid, volDfn.getUuid());
                    assertEquals(volDfnNr, volDfn.getVolumeNumber());
                    assertEquals(volDfnSize, volDfn.getVolumeSize(sysCtx));
                }
                {
                    Volume vol2 = loadedNode2.getResource(sysCtx, resName).getVolume(volDfnNr);
                    assertEquals(vol2Uuid, vol2.getUuid());
                    assertNotNull(vol2);

                    VolumeConnection volCon = vol.getVolumeConnection(sysCtx, vol2);
                    assertNotNull(volCon);

                    assertEquals(vol, volCon.getSourceVolume(sysCtx));
                    assertEquals(vol2, volCon.getTargetVolume(sysCtx));
                    assertEquals(volConUuid, volCon.getUuid());

                    Props volConProps = volCon.getProps(sysCtx);
                    assertNotNull(volConProps);
                    assertEquals(volConTestValue, volConProps.getProp(volConTestKey));
                    assertEquals(1, volConProps.size());
                }
            }
            {
                Resource res2 = loadedNode2.getResource(sysCtx, resName);
                assertNotNull(res2);
                assertEquals(res2Uuid, res2.getUuid());
                ResourceConnection resCon = res.getResourceConnection(sysCtx, res2);
                assertNotNull(resCon);

                assertEquals(res, resCon.getSourceResource(sysCtx));
                assertEquals(res2, resCon.getTargetResource(sysCtx));
                assertEquals(resConUuid, resCon.getUuid());

                Props resConProps = resCon.getProps(sysCtx);
                assertNotNull(resConProps);
                assertEquals(resConTestValue, resConProps.getProp(resConTestKey));
                assertEquals(1, resConProps.size());
            }
        }

        {
            StorPool storPool = loadedNode.getStorPool(sysCtx, poolName);
            assertNotNull(storPool);
            {
                Props storPoolConfig = storPool.getConfiguration(sysCtx);
                assertNotNull(storPoolConfig);
                assertEquals(storPool1TestValue, storPoolConfig.getProp(storPool1TestKey));
                assertEquals(1, storPoolConfig.size());
            }
            {
                StorPoolDefinition storPoolDefinition = storPool.getDefinition(sysCtx);
                assertNotNull(storPoolDefinition);
                assertEquals(poolName, storPoolDefinition.getName());
                assertNotNull(storPoolDefinition.getObjProt());
                assertEquals(storPoolDfnUuid, storPoolDefinition.getUuid());
            }
            {
                StorageDriver storageDriver = storPool.getDriver(sysCtx);
                assertNull(storageDriver);
                // in controller storDriver HAS to be null (as we are testing database, we have to be testing the controller)
            }
            assertEquals(storPoolDriver2, storPool.getDriverName());
            assertEquals(poolName, storPool.getName());
        }
        assertEquals(node1Uuid, loadedNode.getUuid());

        NodeConnection nodeCon = loadedNode.getNodeConnection(sysCtx, loadedNode2);
        assertNotNull(nodeCon);

        assertEquals(loadedNode, nodeCon.getSourceNode(sysCtx));
        assertEquals(loadedNode2, nodeCon.getTargetNode(sysCtx));
        assertEquals(nodeConUuid, nodeCon.getUuid());

        Props nodeConProps = nodeCon.getProps(sysCtx);
        assertNotNull(nodeConProps);
        assertEquals(nodeConTestValue, nodeConProps.getProp(nodeConTestKey));
        assertEquals(1, nodeConProps.size());
    }

    @Test
    public void testCache() throws Exception
    {
        dbDriver.create(node, transMgr);
        super.nodesMap.put(nodeName, node);
        // no clearCaches

        assertEquals(node, dbDriver.load(nodeName, true, transMgr));
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
        satelliteMode();

        NodeData nodeData = NodeData.getInstance(
            sysCtx,
            nodeName,
            null,
            null,
            null,
            true,
            false
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
        satelliteMode();

        NodeData nodeData = NodeData.getInstance(
            sysCtx,
            nodeName,
            null,
            null,
            null,
            false,
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
        NodeData loadedNode = dbDriver.load(halfValidName, true, transMgr);

        assertNotNull(loadedNode);
        assertEquals(node.getName(), loadedNode.getName());
        assertEquals(node.getUuid(), loadedNode.getUuid());
    }

    @Test
    public void testLoadAll() throws Exception
    {
        dbDriver.create(node, transMgr);
        NodeName nodeName2 = new NodeName("NodeName2");
        NodeData node2 = NodeData.getInstance(
            sysCtx,
            nodeName2,
            NodeType.CONTROLLER,
            null,
            transMgr,
            true,
            false
        );
        nodesMap.put(nodeName, node);
        nodesMap.put(nodeName2, node2);
        List<NodeData> allNodes = dbDriver.loadAll(transMgr);
        assertEquals(2, allNodes.size());

        clearCaches();
        allNodes = dbDriver.loadAll(transMgr);
        assertEquals(2, allNodes.size());

        assertEquals(node.getName().value, allNodes.get(0).getName().value);
        assertEquals(node.getName().displayValue, allNodes.get(0).getName().displayValue);
        assertEquals(node.getNodeType(sysCtx), allNodes.get(0).getNodeType(sysCtx));
        assertEquals(node.getUuid(), allNodes.get(0).getUuid());

        assertEquals(node2.getName().value, allNodes.get(1).getName().value);
        assertEquals(node2.getName().displayValue, allNodes.get(1).getName().displayValue);
        assertEquals(node2.getNodeType(sysCtx), allNodes.get(1).getNodeType(sysCtx));
        assertEquals(node2.getUuid(), allNodes.get(1).getUuid());
    }

    @Test (expected = DrbdDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        dbDriver.create(node, transMgr);

        NodeData.getInstance(sysCtx, nodeName, initialType, null, transMgr, false, true);
    }

}
