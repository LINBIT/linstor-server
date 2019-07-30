package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterfaceData;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.NodeConnectionData;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.core.objects.NodeDataGenericDbDriver;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionData;
import com.linbit.linstor.core.objects.ResourceData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.core.objects.VolumeConnectionData;
import com.linbit.linstor.core.objects.VolumeData;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node.InitMaps;
import com.linbit.linstor.core.objects.Node.NodeFlag;
import com.linbit.linstor.core.objects.Node.NodeType;
import com.linbit.linstor.core.objects.Resource.RscFlags;
import com.linbit.linstor.core.objects.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.core.objects.Volume.VlmFlags;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeDataGenericDbDriverTest extends GenericDbBase
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

    @Inject private NodeDataGenericDbDriver dbDriver;
    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private long initialFlags;
    private NodeType initialType;
    private NodeData node;

    public NodeDataGenericDbDriverTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
    }

    @Before
    public void setUp() throws Exception
    {
        seedDefaultPeerRule.setDefaultPeerAccessContext(SYS_CTX);
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
        node = TestFactory.createNodeData(
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
        nodeDataFactory.create(
            SYS_CTX,
            nodeName,
            null,
            null
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

        Iterator<NodeData> nodeIt = dbDriver.loadAll().keySet().iterator();
        NodeData loaded = nodeIt.next();
        assertFalse(nodeIt.hasNext());

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
    @SuppressWarnings({"checkstyle:magicnumber", "checkstyle:variabledeclarationusagedistance"})
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
        Integer node1Id = 13;

        // node2 res
        java.util.UUID res2Uuid;
        String res2TestKey = "res2TestKey";
        String res2TestValue = "res2TestValue";
        Integer node2Id = 14;

        // volDfn
        java.util.UUID volDfnUuid;
        VolumeNumber volDfnNr = new VolumeNumber(42);
        long volDfnSize = 5_000_000L;
        int volDfnMinorNr = 10;
        String volDfnTestKey = "volDfnTestKey";
        String volDfnTestValue = "volDfnTestValue";

        // node1 vol
        java.util.UUID vol1Uuid;
        String vol1TestKey = "vol1TestKey";
        String vol1TestValue = "vol1TestValue";

        // node1 vol
        java.util.UUID vol2Uuid;
        String vol2TestKey = "vol2TestKey";
        String vol2TestValue = "vol2TestValue";

        // storPoolDfn
        java.util.UUID storPoolDfnUuid;
        StorPoolName poolName = new StorPoolName("TestPoolName");

        // storPool
        DeviceProviderKind storPoolDriver1 = DeviceProviderKind.LVM;
        String storPool1TestKey = "storPool1TestKey";
        String storPool1TestValue = "storPool1TestValue";

        // storPool
        DeviceProviderKind storPoolDriver2 = DeviceProviderKind.LVM;
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
            NodeData node1 = nodeDataFactory.create(
                SYS_CTX,
                nodeName,
                NodeType.COMBINED,
                new NodeFlag[] {NodeFlag.QIGNORE}
            );
            node1.getProps(SYS_CTX).setProp(node1TestKey, node1TestValue);
            node1Uuid = node1.getUuid();

            nodesMap.put(node1.getName(), node1);

            // node1's netIface
            NetInterfaceData netIf = netInterfaceDataFactory.create(
                SYS_CTX,
                node1,
                netName,
                new LsIpAddress(netHost),
                new TcpPortNumber(ApiConsts.DFLT_CTRL_PORT_PLAIN),
                EncryptionType.PLAIN
            );
            netIfUuid = netIf.getUuid();

            // node2
            NodeData node2 = nodeDataFactory.create(
                SYS_CTX,
                nodeName2,
                NodeType.COMBINED,
                null
            );

            nodesMap.put(node2.getName(), node2);

            // resDfn
            ResourceDefinitionData resDfn = resourceDefinitionDataFactory.create(
                SYS_CTX,
                resName,
                null,
                resPort,
                new RscDfnFlags[] {RscDfnFlags.DELETE},
                "secret",
                transportType,
                Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
                null
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
            StorPoolDefinitionData storPoolDfn = storPoolDefinitionDataFactory.create(
                SYS_CTX,
                poolName
            );
            storPoolDfnUuid = storPoolDfn.getUuid();

            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

            // node1 storPool
            StorPoolData storPool1 = storPoolDataFactory.create(
                SYS_CTX,
                node1,
                storPoolDfn,
                storPoolDriver1,
                getFreeSpaceMgr(storPoolDfn, node1)
            );
            storPool1.getProps(SYS_CTX).setProp(storPool1TestKey, storPool1TestValue);

            // node2 storPool
            StorPoolData storPool2 = storPoolDataFactory.create(
                SYS_CTX,
                node2,
                storPoolDfn,
                storPoolDriver2,
                getFreeSpaceMgr(storPoolDfn, node2)
            );
            storPool2.getProps(SYS_CTX).setProp(storPool2TestKey, storPool2TestValue);

            // node1 res
            ResourceData res1 = resourceDataFactory.create(
                SYS_CTX,
                resDfn,
                node1,
                node1Id,
                new RscFlags[] {RscFlags.CLEAN},
                Collections.emptyList()
            );
            res1.getProps(SYS_CTX).setProp(res1TestKey, res1TestValue);
            res1Uuid = res1.getUuid();

            // node1 vol
            VolumeData vol1 = volumeDataFactory.create(
                SYS_CTX,
                res1,
                volDfn,
                new VlmFlags[] {},
                Collections.singletonMap("", storPool1)
            );
            vol1.getProps(SYS_CTX).setProp(vol1TestKey, vol1TestValue);
            vol1Uuid = vol1.getUuid();

            // node2 res
            ResourceData res2 = resourceDataFactory.create(
                SYS_CTX,
                resDfn,
                node2,
                node2Id,
                new RscFlags[] {RscFlags.CLEAN},
                Collections.emptyList()
            );
            res2.getProps(SYS_CTX).setProp(res2TestKey, res2TestValue);
            res2Uuid = res2.getUuid();

            // node2 vol
            VolumeData vol2 = volumeDataFactory.create(
                SYS_CTX,
                res2,
                volDfn,
                new VlmFlags[] {},
                Collections.singletonMap("", storPool2)
            );
            vol2.getProps(SYS_CTX).setProp(vol2TestKey, vol2TestValue);
            vol2Uuid = vol2.getUuid();

            // nodeCon node1 <-> node2
            NodeConnectionData nodeCon = nodeConnectionDataFactory.create(
                SYS_CTX,
                node1,
                node2
            );
            nodeCon.getProps(SYS_CTX).setProp(nodeConTestKey, nodeConTestValue);
            nodeConUuid = nodeCon.getUuid();

            // resCon res1 <-> res2
            ResourceConnectionData resCon = resourceConnectionDataFactory.create(
                SYS_CTX,
                res1,
                res2,
                null
            );
            resCon.getProps(SYS_CTX).setProp(resConTestKey, resConTestValue);
            resConUuid = resCon.getUuid();

            // volCon vol1 <-> vol2
            VolumeConnectionData volCon = volumeConnectionDataFactory.create(
                SYS_CTX,
                vol1,
                vol2
            );
            volCon.getProps(SYS_CTX).setProp(volConTestKey, volConTestValue);
            volConUuid = volCon.getUuid();

            commit();
        }

        NodeData loadedNode = nodeRepository.get(SYS_CTX, nodeName);
        NodeData loadedNode2 = nodeRepository.get(SYS_CTX, nodeName2);

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

        assertEquals(NodeType.COMBINED, loadedNode.getNodeType(SYS_CTX));
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
                assertNotNull(storPool.getDeviceProviderKind());
                // in controller storDriver HAS to be null (as we are testing database, we
                // have to be testing the controller)
            }
            assertEquals(storPoolDriver2, storPool.getDeviceProviderKind());
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
    public void testLoadAll() throws Exception
    {
        dbDriver.create(node);
        NodeName nodeName2 = new NodeName("NodeName2");
        NodeData node2 = nodeDataFactory.create(
            SYS_CTX,
            nodeName2,
            NodeType.CONTROLLER,
            null
        );
        nodesMap.put(nodeName, node);
        nodesMap.put(nodeName2, node2);
        Map<NodeData, InitMaps> allNodes = dbDriver.loadAll();
        assertEquals(2, allNodes.size());

        Iterator<NodeData> loadedNodesIterator = allNodes.keySet().iterator();
        Node loadedNode0 = loadedNodesIterator.next();
        Node loadedNode1 = loadedNodesIterator.next();

        if (!loadedNode0.getName().equals(node.getName()))
        {
            Node tmp = loadedNode0;
            loadedNode0 = loadedNode1;
            loadedNode1 = tmp;
        }
        assertEquals(node.getName().value, loadedNode0.getName().value);
        assertEquals(node.getName().displayValue, loadedNode0.getName().displayValue);
        assertEquals(node.getNodeType(SYS_CTX), loadedNode0.getNodeType(SYS_CTX));
        assertEquals(node.getUuid(), loadedNode0.getUuid());

        assertEquals(node2.getName().value, loadedNode1.getName().value);
        assertEquals(node2.getName().displayValue, loadedNode1.getName().displayValue);
        assertEquals(node2.getNodeType(SYS_CTX), loadedNode1.getNodeType(SYS_CTX));
        assertEquals(node2.getUuid(), loadedNode1.getUuid());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        dbDriver.create(node);
        nodesMap.put(nodeName, node);

        nodeDataFactory.create(SYS_CTX, nodeName, initialType, null);
    }

}
