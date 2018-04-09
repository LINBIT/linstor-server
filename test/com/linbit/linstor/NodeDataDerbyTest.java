package com.linbit.linstor;

import javax.inject.Inject;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.linstor.storage.StorageDriver;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        " SELECT " + NODE_NET_NAME + ", " + NODE_NET_DSP_NAME + ", " + INET_ADDRESS +
        " FROM " + TBL_NODE_NET_INTERFACES +
        " WHERE " + NODE_NAME + " = ?";
    private static final String SELECT_ALL_STOR_POOLS_FOR_NODE =
        " SELECT SPD." + POOL_NAME + ", SPD." + POOL_DSP_NAME + ", NSP." + DRIVER_NAME + ", NSP." + UUID +
        " FROM " + TBL_STOR_POOL_DEFINITIONS + " AS SPD" +
        " RIGHT JOIN " + TBL_NODE_STOR_POOL + " AS NSP ON " +
        "     NSP." + POOL_NAME + " = SPD." + POOL_NAME +
        " WHERE " + NODE_NAME + " = ? AND " +
                    "SPD." + POOL_DSP_NAME + " <> '" + LinStor.DISKLESS_STOR_POOL_NAME + "'";
    private static final String SELECT_ALL_PROPS_FOR_NODE =
        " SELECT " + PROP_KEY + ", " + PROP_VALUE +
        " FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ?";

    private final NodeName nodeName;

    @Inject private NodeDataDerbyDriver dbDriver;
    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private long initialFlags;
    private NodeType initialType;
    private NodeData node;

    public NodeDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
    }

    @SuppressWarnings("checkstyle:magicnumber")

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            "NODES table's column count has changed. Update tests accordingly!",
            5,
            TBL_COL_COUNT_NODES
        );


        uuid = randomUUID();
        objProt = objectProtectionFactory.getInstance(
            SYS_CTX,
            ObjectProtection.buildPath(nodeName),
            true
        );
        initialFlags = NodeFlag.QIGNORE.flagValue;
        initialType = NodeType.AUXILIARY;
        node = new NodeData(
            SYS_CTX,
            uuid,
            objProt,
            nodeName,
            initialType,
            initialFlags,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        dbDriver.create(node);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODES);
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
        nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName,
            null,
            null,
            true,
            false
        );
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist NodeData instance", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(0, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.NodeType.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
        assertFalse("Database contains too many datasets", resultSet.next());

        resultSet.close();
        stmt.close();

        stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent resource", resultSet.next());
        resultSet.close();
        stmt.close();

        stmt = getConnection().prepareStatement(SELECT_ALL_NET_INTERFACES_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent net interface", resultSet.next());
        resultSet.close();
        stmt.close();

        stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOLS_FOR_NODE);
        stmt.setString(1, nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent stor pool", resultSet.next());
        resultSet.close();
        stmt.close();

        ObjectProtection loadedObjProt = objectProtectionFactory.getInstance(
            SYS_CTX,
            ObjectProtection.buildPath(nodeName),
            false
        );
        assertNotNull("Database did not persist objectProtection", loadedObjProt);

        stmt = getConnection().prepareStatement(SELECT_ALL_PROPS_FOR_NODE);
        stmt.setString(1, "NODES/" + nodeName.value);
        resultSet = stmt.executeQuery();
        assertFalse("Database persisted non existent properties", resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdateFlags() throws Exception
    {
        insertNode(uuid, nodeName, 0, NodeType.AUXILIARY);
        commit();

        NodeData loaded = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, false, false);

        assertNotNull(loaded);
        loaded.getFlags().enableFlags(SYS_CTX, NodeFlag.DELETE);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODES);
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
        dbDriver.create(node);

        NodeData loaded = dbDriver.load(nodeName, true);

        assertEquals(nodeName.value, loaded.getName().value);
        assertEquals(nodeName.displayValue, loaded.getName().displayValue);
        assertEquals(NodeFlag.QIGNORE.flagValue, loaded.getFlags().getFlagsBits(SYS_CTX));
        assertEquals(Node.NodeType.AUXILIARY, loaded.getNodeType(SYS_CTX));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        NodeData loadedNode = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, false, false);
        assertNull(loadedNode);

        insertNode(uuid, nodeName, 0, NodeType.AUXILIARY);
        commit();

        loadedNode = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, false, false);

        assertNotNull(loadedNode);
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        assertEquals(0, loadedNode.getFlags().getFlagsBits(SYS_CTX));
        assertEquals(NodeType.AUXILIARY, loadedNode.getNodeType(SYS_CTX));
        assertEquals(0, loadedNode.getProps(SYS_CTX).size()); // serial number
    }

    @SuppressWarnings({"checkstyle:variabledeclarationusagedistance", "checkstyle:magicnumber"})
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

        // node2
        NodeName nodeName2 = new NodeName("TestTargetNodeName");

        // resDfn
        ResourceName resName = new ResourceName("TestResName");
        java.util.UUID resDfnUuid;
        int resPort = 9001;
        String resDfnTestKey = "resDfnTestKey";
        String resDfnTestValue = "resDfnTestValue";
        TransportType transportType = TransportType.IP;

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
            NodeData node1 = nodeDataFactory.getInstance(
                SYS_CTX,
                nodeName,
                NodeType.AUXILIARY,
                new NodeFlag[] {NodeFlag.QIGNORE},
                true,
                true
            );
            node1.getProps(SYS_CTX).setProp(node1TestKey, node1TestValue);
            node1Uuid = node1.getUuid();

            nodesMap.put(node1.getName(), node1);

            // node1's netIface
            NetInterfaceData netIf = netInterfaceDataFactory.getInstance(
                SYS_CTX,
                node1,
                netName,
                new LsIpAddress(netHost),
                true,
                true
            );
            netIfUuid = netIf.getUuid();

            // node2
            NodeData node2 = nodeDataFactory.getInstance(
                SYS_CTX,
                nodeName2,
                NodeType.AUXILIARY,
                null,
                true,
                true
            );

            nodesMap.put(node2.getName(), node2);

            // resDfn
            ResourceDefinitionData resDfn = resourceDefinitionDataFactory.create(
                SYS_CTX,
                resName,
                resPort,
                new RscDfnFlags[] {RscDfnFlags.DELETE},
                "secret",
                transportType
            );
            resDfn.getProps(SYS_CTX).setProp(resDfnTestKey, resDfnTestValue);
            resDfnUuid = resDfn.getUuid();

            rscDfnMap.put(resDfn.getName(), resDfn);

            // volDfn
            VolumeDefinitionData volDfn = volumeDefinitionDataFactory.create(
                SYS_CTX,
                resDfn,
                volDfnNr,
                volDfnMinorNr,
                volDfnSize,
                new VlmDfnFlags[] {VlmDfnFlags.DELETE}
            );
            volDfn.getProps(SYS_CTX).setProp(volDfnTestKey, volDfnTestValue);
            volDfnUuid = volDfn.getUuid();

            // storPoolDfn
            StorPoolDefinitionData storPoolDfn = storPoolDefinitionDataFactory.getInstance(
                SYS_CTX,
                poolName,
                true,
                true
            );
            storPoolDfnUuid = storPoolDfn.getUuid();

            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

            // node1 storPool
            StorPoolData storPool1 = storPoolDataFactory.getInstance(
                SYS_CTX,
                node1,
                storPoolDfn,
                storPoolDriver1,
                true,
                true
            );
            storPool1.getProps(SYS_CTX).setProp(storPool1TestKey, storPool1TestValue);

            // node2 storPool
            StorPoolData storPool2 = storPoolDataFactory.getInstance(
                SYS_CTX,
                node2,
                storPoolDfn,
                storPoolDriver2,
                true,
                true
            );
            storPool2.getProps(SYS_CTX).setProp(storPool2TestKey, storPool2TestValue);

            // node1 res
            ResourceData res1 = resourceDataFactory.getInstance(
                SYS_CTX,
                resDfn,
                node1,
                node1Id,
                new RscFlags[] {RscFlags.CLEAN},
                true,
                true
            );
            res1.getProps(SYS_CTX).setProp(res1TestKey, res1TestValue);
            res1Uuid = res1.getUuid();

            // node1 vol
            VolumeData vol1 = volumeDataFactory.getInstance(
                SYS_CTX,
                res1,
                volDfn,
                storPool1,
                vol1TestBlockDev,
                vol1TestMetaDisk,
                new VlmFlags[] {VlmFlags.CLEAN},
                true,
                true
            );
            vol1.getProps(SYS_CTX).setProp(vol1TestKey, vol1TestValue);
            vol1Uuid = vol1.getUuid();

            // node2 res
            ResourceData res2 = resourceDataFactory.getInstance(
                SYS_CTX,
                resDfn,
                node2,
                node2Id,
                new RscFlags[] {RscFlags.CLEAN},
                true,
                true
            );
            res2.getProps(SYS_CTX).setProp(res2TestKey, res2TestValue);
            res2Uuid = res2.getUuid();

            // node2 vol
            VolumeData vol2 = volumeDataFactory.getInstance(
                SYS_CTX,
                res2,
                volDfn,
                storPool2,
                vol2TestBlockDev,
                vol2TestMetaDisk,
                new VlmFlags[] {VlmFlags.CLEAN},
                true,
                true
            );
            vol2.getProps(SYS_CTX).setProp(vol2TestKey, vol2TestValue);
            vol2Uuid = vol2.getUuid();

            // nodeCon node1 <-> node2
            NodeConnectionData nodeCon = nodeConnectionDataFactory.getInstance(
                SYS_CTX,
                node1,
                node2,
                true,
                true
            );
            nodeCon.getProps(SYS_CTX).setProp(nodeConTestKey, nodeConTestValue);
            nodeConUuid = nodeCon.getUuid();

            // resCon res1 <-> res2
            ResourceConnectionData resCon = resourceConnectionDataFactory.getInstance(
                SYS_CTX,
                res1,
                res2,
                true,
                true
            );
            resCon.getProps(SYS_CTX).setProp(resConTestKey, resConTestValue);
            resConUuid = resCon.getUuid();

            // volCon vol1 <-> vol2
            VolumeConnectionData volCon = volumeConnectionDataFactory.getInstance(
                SYS_CTX,
                vol1,
                vol2,
                true,
                true
            );
            volCon.getProps(SYS_CTX).setProp(volConTestKey, volConTestValue);
            volConUuid = volCon.getUuid();

            commit();
        }

        NodeData loadedNode = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, false, false);
        NodeData loadedNode2 = nodeDataFactory.getInstance(SYS_CTX, nodeName2, null, null, false, false);

        assertNotNull(loadedNode);

        assertEquals(NodeFlag.QIGNORE.flagValue, loadedNode.getFlags().getFlagsBits(SYS_CTX));
        assertEquals(nodeName, loadedNode.getName()); // NodeName class implements equals
        {
            NetInterface netIf = loadedNode.getNetInterface(SYS_CTX, netName);
            assertNotNull(netIf);
            {
                LsIpAddress address = netIf.getAddress(SYS_CTX);
                assertNotNull(address);
                assertEquals(netHost, address.getAddress());
            }
            assertEquals(netName, netIf.getName());
            assertEquals(loadedNode, netIf.getNode());
            assertEquals(netIfUuid, netIf.getUuid());
        }

        assertEquals(NodeType.AUXILIARY, loadedNode.getNodeType(SYS_CTX));
        assertNotNull(loadedNode.getObjProt());
        {
            Props nodeProps = loadedNode.getProps(SYS_CTX);
            assertNotNull(nodeProps);
            assertEquals(node1TestValue, nodeProps.getProp(node1TestKey));
            assertEquals(1, nodeProps.size());
        }
        {
            Resource res = loadedNode.getResource(SYS_CTX, resName);
            assertNotNull(res);
            assertEquals(loadedNode, res.getAssignedNode());
            {
                ResourceDefinition resDfn = res.getDefinition();
                assertNotNull(resDfn);
                assertEquals(RscDfnFlags.DELETE.flagValue, resDfn.getFlags().getFlagsBits(SYS_CTX));
                assertEquals(resName, resDfn.getName());
                assertEquals(resPort, resDfn.getPort(SYS_CTX).value);
                assertNotNull(resDfn.getObjProt());
                {
                    Props resDfnProps = resDfn.getProps(SYS_CTX);
                    assertNotNull(resDfnProps);

                    assertEquals(resDfnTestValue, resDfnProps.getProp(resDfnTestKey));
                    assertEquals(1, resDfnProps.size());
                }
                assertEquals(res, resDfn.getResource(SYS_CTX, nodeName));
                assertEquals(resDfnUuid, resDfn.getUuid());
                assertEquals(res.getVolume(volDfnNr).getVolumeDefinition(), resDfn.getVolumeDfn(SYS_CTX, volDfnNr));
            }
            assertEquals(node1Id, res.getNodeId());
            assertNotNull(res.getObjProt());
            {
                Props resProps = res.getProps(SYS_CTX);
                assertNotNull(resProps);

                assertEquals(res1TestValue, resProps.getProp(res1TestKey));
                assertEquals(1, resProps.size());
            }
            {
                StateFlags<RscFlags> resStateFlags = res.getStateFlags();
                assertNotNull(resStateFlags);
                assertTrue(resStateFlags.isSet(SYS_CTX, RscFlags.CLEAN));
                assertFalse(resStateFlags.isSet(SYS_CTX, RscFlags.DELETE));
            }
            assertEquals(res1Uuid, res.getUuid());
            {
                Volume vol = res.getVolume(volDfnNr);
                assertNotNull(vol);
                {
                    StateFlags<VlmFlags> flags = vol.getFlags();
                    assertNotNull(flags);
                    flags.isSet(SYS_CTX, Volume.VlmFlags.CLEAN);
                }
                {
                    Props volProps = vol.getProps(SYS_CTX);
                    assertNotNull(volProps);
                    assertEquals(vol1TestValue, volProps.getProp(vol1TestKey));
                    assertEquals(1, volProps.size());
                }
                assertEquals(res, vol.getResource());
                assertEquals(res.getDefinition(), vol.getResourceDefinition());
                assertEquals(vol1Uuid, vol.getUuid());
                {
                    VolumeDefinition volDfn = vol.getVolumeDefinition();
                    assertTrue(volDfn.getFlags().isSet(SYS_CTX, VlmDfnFlags.DELETE));
                    assertEquals(volDfnMinorNr, volDfn.getMinorNr(SYS_CTX).value);
                    {
                        Props volDfnProps = volDfn.getProps(SYS_CTX);
                        assertNotNull(volDfnProps);
                        assertEquals(volDfnTestValue, volDfnProps.getProp(volDfnTestKey));
                        assertEquals(1, volDfnProps.size());
                    }
                    assertEquals(res.getDefinition(), volDfn.getResourceDefinition());
                    assertEquals(volDfnUuid, volDfn.getUuid());
                    assertEquals(volDfnNr, volDfn.getVolumeNumber());
                    assertEquals(volDfnSize, volDfn.getVolumeSize(SYS_CTX));
                }
                {
                    Volume vol2 = loadedNode2.getResource(SYS_CTX, resName).getVolume(volDfnNr);
                    assertEquals(vol2Uuid, vol2.getUuid());
                    assertNotNull(vol2);

                    VolumeConnection volCon = vol.getVolumeConnection(SYS_CTX, vol2);
                    assertNotNull(volCon);

                    assertEquals(vol, volCon.getSourceVolume(SYS_CTX));
                    assertEquals(vol2, volCon.getTargetVolume(SYS_CTX));
                    assertEquals(volConUuid, volCon.getUuid());

                    Props volConProps = volCon.getProps(SYS_CTX);
                    assertNotNull(volConProps);
                    assertEquals(volConTestValue, volConProps.getProp(volConTestKey));
                    assertEquals(1, volConProps.size());
                }
            }
            {
                Resource res2 = loadedNode2.getResource(SYS_CTX, resName);
                assertNotNull(res2);
                assertEquals(res2Uuid, res2.getUuid());
                ResourceConnection resCon = res.getResourceConnection(SYS_CTX, res2);
                assertNotNull(resCon);

                assertEquals(res, resCon.getSourceResource(SYS_CTX));
                assertEquals(res2, resCon.getTargetResource(SYS_CTX));
                assertEquals(resConUuid, resCon.getUuid());

                Props resConProps = resCon.getProps(SYS_CTX);
                assertNotNull(resConProps);
                assertEquals(resConTestValue, resConProps.getProp(resConTestKey));
                assertEquals(1, resConProps.size());
            }
        }

        {
            StorPool storPool = loadedNode.getStorPool(SYS_CTX, poolName);
            assertNotNull(storPool);
            {
                Props storPoolConfig = storPool.getProps(SYS_CTX);
                assertNotNull(storPoolConfig);
                assertEquals(storPool1TestValue, storPoolConfig.getProp(storPool1TestKey));
                assertEquals(1, storPoolConfig.size());
            }
            {
                StorPoolDefinition storPoolDefinition = storPool.getDefinition(SYS_CTX);
                assertNotNull(storPoolDefinition);
                assertEquals(poolName, storPoolDefinition.getName());
                assertNotNull(storPoolDefinition.getObjProt());
                assertEquals(storPoolDfnUuid, storPoolDefinition.getUuid());
            }
            {
                StorageDriver storageDriver = storPool.getDriver(SYS_CTX, null, null, null);
                assertNull(storageDriver);
                // in controller storDriver HAS to be null (as we are testing database,
                // we have to be testing the controller)
            }
            assertEquals(storPoolDriver2, storPool.getDriverName());
            assertEquals(poolName, storPool.getName());
        }
        assertEquals(node1Uuid, loadedNode.getUuid());

        NodeConnection nodeCon = loadedNode.getNodeConnection(SYS_CTX, loadedNode2);
        assertNotNull(nodeCon);

        assertEquals(loadedNode, nodeCon.getSourceNode(SYS_CTX));
        assertEquals(loadedNode2, nodeCon.getTargetNode(SYS_CTX));
        assertEquals(nodeConUuid, nodeCon.getUuid());

        Props nodeConProps = nodeCon.getProps(SYS_CTX);
        assertNotNull(nodeConProps);
        assertEquals(nodeConTestValue, nodeConProps.getProp(nodeConTestKey));
        assertEquals(1, nodeConProps.size());
    }

    @Test
    public void testCache() throws Exception
    {
        dbDriver.create(node);
        nodesMap.put(nodeName, node);
        // no clearCaches

        assertEquals(node, dbDriver.load(nodeName, true));
    }

    @Test
    public void testDelete() throws Exception
    {
        dbDriver.create(node);
        commit();
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(node);
        commit();
        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        dbDriver.create(node);

        NodeName halfValidName = new NodeName(node.getName().value);
        NodeData loadedNode = dbDriver.load(halfValidName, true);

        assertNotNull(loadedNode);
        assertEquals(node.getName(), loadedNode.getName());
        assertEquals(node.getUuid(), loadedNode.getUuid());
    }

    @Test
    public void testLoadAll() throws Exception
    {
        dbDriver.create(node);
        NodeName nodeName2 = new NodeName("NodeName2");
        NodeData node2 = nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName2,
            NodeType.CONTROLLER,
            null,
            true,
            false
        );
        nodesMap.put(nodeName, node);
        nodesMap.put(nodeName2, node2);
        List<NodeData> allNodes = dbDriver.loadAll();
        assertEquals(2, allNodes.size());

        clearCaches();
        allNodes = dbDriver.loadAll();
        assertEquals(2, allNodes.size());

        assertEquals(node.getName().value, allNodes.get(0).getName().value);
        assertEquals(node.getName().displayValue, allNodes.get(0).getName().displayValue);
        assertEquals(node.getNodeType(SYS_CTX), allNodes.get(0).getNodeType(SYS_CTX));
        assertEquals(node.getUuid(), allNodes.get(0).getUuid());

        assertEquals(node2.getName().value, allNodes.get(1).getName().value);
        assertEquals(node2.getName().displayValue, allNodes.get(1).getName().displayValue);
        assertEquals(node2.getNodeType(SYS_CTX), allNodes.get(1).getNodeType(SYS_CTX));
        assertEquals(node2.getUuid(), allNodes.get(1).getUuid());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        dbDriver.create(node);

        nodeDataFactory.getInstance(SYS_CTX, nodeName, initialType, null, false, true);
    }

}
