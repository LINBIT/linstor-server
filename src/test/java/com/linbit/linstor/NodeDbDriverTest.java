package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.NodeDbDriver;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscPayload;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Named;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeDbDriverTest extends GenericDbBase
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

    @Inject private NodeDbDriver dbDriver;
    @Inject
    @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef;
    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private long initialFlags;
    private Node.Type initialType;
    private Node node;
    private ResourceGroup dfltRscGrp;

    public NodeDbDriverTest() throws Exception
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
        initialFlags = Node.Flags.QIGNORE.flagValue;
        initialType = Node.Type.AUXILIARY;
        node = TestFactory.createNode(
            uuid,
            objProt,
            nodeName,
            initialType,
            initialFlags,
            ctrlConfRef,
            errorReporter,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        dfltRscGrp = createDefaultResourceGroup(SYS_CTX);
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
        assertEquals(Node.Flags.QIGNORE.flagValue, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.Type.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }


    @Test
    public void testPersistGetInstance() throws Exception
    {
        nodeFactory.create(
            SYS_CTX,
            nodeName,
            null,
            null
        );
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist Node instance", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(0, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.Type.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
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
            true
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
        insertNode(uuid, nodeName, 0, Node.Type.AUXILIARY);
        commit();

        Iterator<Node> nodeIt = dbDriver.loadAll(null).keySet().iterator();
        Node loaded = nodeIt.next();
        assertFalse(nodeIt.hasNext());

        assertNotNull(loaded);
        loaded.getFlags().enableFlags(SYS_CTX, Node.Flags.DELETE);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database deleted Node", resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(nodeName.displayValue, resultSet.getString(NODE_DSP_NAME));
        assertEquals(Node.Flags.DELETE.flagValue, resultSet.getLong(NODE_FLAGS));
        assertEquals(Node.Type.AUXILIARY.getFlagValue(), resultSet.getInt(NODE_TYPE));
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
        Set<Integer> resPorts = Collections.singleton(9001);
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
            Node node1 = nodeFactory.create(
                SYS_CTX,
                nodeName,
                Node.Type.COMBINED,
                new Node.Flags[] {Node.Flags.QIGNORE}
            );
            node1.getProps(SYS_CTX).setProp(node1TestKey, node1TestValue);
            node1Uuid = node1.getUuid();

            nodesMap.put(node1.getName(), node1);

            // node1's netIface
            NetInterface netIf = netInterfaceFactory.create(
                SYS_CTX,
                node1,
                netName,
                new LsIpAddress(netHost),
                new TcpPortNumber(ApiConsts.DFLT_CTRL_PORT_PLAIN),
                EncryptionType.PLAIN
            );
            netIfUuid = netIf.getUuid();

            // node2
            Node node2 = nodeFactory.create(
                SYS_CTX,
                nodeName2,
                Node.Type.COMBINED,
                null
            );

            nodesMap.put(node2.getName(), node2);

            // resDfn
            LayerPayload payload = new LayerPayload();
            DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
            drbdRscDfn.sharedSecret = "secret";
            drbdRscDfn.transportType = transportType;
            ResourceDefinition resDfn = resourceDefinitionFactory.create(
                SYS_CTX,
                resName,
                null,
                new ResourceDefinition.Flags[]
                {
                    ResourceDefinition.Flags.DELETE
                },
                Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
                payload,
                dfltRscGrp
            );
            resDfn.getProps(SYS_CTX).setProp(resDfnTestKey, resDfnTestValue);
            resDfnUuid = resDfn.getUuid();

            rscDfnMap.put(resDfn.getName(), resDfn);

            // volDfn
            VolumeDefinition volDfn = volumeDefinitionFactory.create(
                SYS_CTX,
                resDfn,
                volDfnNr,
                volDfnMinorNr,
                volDfnSize,
                new VolumeDefinition.Flags[] {VolumeDefinition.Flags.DELETE}
            );
            volDfn.getProps(SYS_CTX).setProp(volDfnTestKey, volDfnTestValue);
            volDfnUuid = volDfn.getUuid();

            // storPoolDfn
            StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(
                SYS_CTX,
                poolName
            );
            storPoolDfnUuid = storPoolDfn.getUuid();

            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

            // node1 storPool
            StorPool storPool1 = storPoolFactory.create(
                SYS_CTX,
                node1,
                storPoolDfn,
                storPoolDriver1,
                getFreeSpaceMgr(storPoolDfn, node1),
                false
            );
            storPool1.getProps(SYS_CTX).setProp(storPool1TestKey, storPool1TestValue);

            // node2 storPool
            StorPool storPool2 = storPoolFactory.create(
                SYS_CTX,
                node2,
                storPoolDfn,
                storPoolDriver2,
                getFreeSpaceMgr(storPoolDfn, node2),
                false
            );
            storPool2.getProps(SYS_CTX).setProp(storPool2TestKey, storPool2TestValue);

            // node1 res
            LayerPayload payload1 = new LayerPayload();
            DrbdRscPayload drbdRsc1 = payload1.getDrbdRsc();
            drbdRsc1.nodeId = node1Id;
            drbdRsc1.tcpPorts = resPorts;
            Resource res1 = resourceFactory.create(
                SYS_CTX,
                resDfn,
                node1,
                payload1,
                new Resource.Flags[]
                {
                    Resource.Flags.CLEAN
                },
                Collections.emptyList()
            );
            res1.getProps(SYS_CTX).setProp(res1TestKey, res1TestValue);
            res1Uuid = res1.getUuid();

            // node1 vol
            payload1.putStorageVlmPayload("", volDfn.getVolumeNumber().value, storPool1);
            Volume vol1 = volumeFactory.create(
                SYS_CTX,
                res1,
                volDfn,
                new Volume.Flags[]
                {},
                payload1,
                null,
                Collections.emptyMap(),
                null
            );
            vol1.getProps(SYS_CTX).setProp(vol1TestKey, vol1TestValue);
            vol1Uuid = vol1.getUuid();

            // node2 res
            LayerPayload payload2 = new LayerPayload();
            DrbdRscPayload drbdRsc2 = payload2.getDrbdRsc();
            drbdRsc2.nodeId = node2Id;
            drbdRsc2.tcpPorts = resPorts;
            Resource res2 = resourceFactory.create(
                SYS_CTX,
                resDfn,
                node2,
                payload2,
                new Resource.Flags[]
                {
                    Resource.Flags.CLEAN
                },
                Collections.emptyList()
            );
            res2.getProps(SYS_CTX).setProp(res2TestKey, res2TestValue);
            res2Uuid = res2.getUuid();

            // node2 vol
            payload2.putStorageVlmPayload("", volDfn.getVolumeNumber().value, storPool2);
            Volume vol2 = volumeFactory.create(
                SYS_CTX,
                res2,
                volDfn,
                new Volume.Flags[]
                {},
                payload2,
                null,
                Collections.emptyMap(),
                null
            );
            vol2.getProps(SYS_CTX).setProp(vol2TestKey, vol2TestValue);
            vol2Uuid = vol2.getUuid();

            // nodeCon node1 <-> node2
            NodeConnection nodeCon = nodeConnectionFactory.create(
                SYS_CTX,
                node1,
                node2
            );
            nodeCon.getProps(SYS_CTX).setProp(nodeConTestKey, nodeConTestValue);
            nodeConUuid = nodeCon.getUuid();

            // resCon res1 <-> res2
            ResourceConnection resCon = resourceConnectionFactory.create(
                SYS_CTX,
                res1,
                res2,
                null
            );
            resCon.getProps(SYS_CTX).setProp(resConTestKey, resConTestValue);
            resConUuid = resCon.getUuid();

            // volCon vol1 <-> vol2
            VolumeConnection volCon = volumeConnectionFactory.create(
                SYS_CTX,
                vol1,
                vol2
            );
            volCon.getProps(SYS_CTX).setProp(volConTestKey, volConTestValue);
            volConUuid = volCon.getUuid();

            commit();
        }

        Node loadedNode = nodeRepository.get(SYS_CTX, nodeName);
        Node loadedNode2 = nodeRepository.get(SYS_CTX, nodeName2);

        assertNotNull(loadedNode);

        assertEquals(Node.Flags.QIGNORE.flagValue, loadedNode.getFlags().getFlagsBits(SYS_CTX));
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

        assertEquals(Node.Type.COMBINED, loadedNode.getNodeType(SYS_CTX));
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
            assertEquals(loadedNode, res.getNode());
            {
                ResourceDefinition resDfn = res.getResourceDefinition();
                assertNotNull(resDfn);
                assertEquals(ResourceDefinition.Flags.DELETE.flagValue, resDfn.getFlags().getFlagsBits(SYS_CTX));
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
                StateFlags<Resource.Flags> resStateFlags = res.getStateFlags();
                assertNotNull(resStateFlags);
                assertTrue(resStateFlags.isSet(SYS_CTX, Resource.Flags.CLEAN));
                assertFalse(resStateFlags.isSet(SYS_CTX, Resource.Flags.DELETE));
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
                assertEquals(res, vol.getAbsResource());
                assertEquals(res.getResourceDefinition(), vol.getResourceDefinition());
                assertEquals(vol1Uuid, vol.getUuid());
                {
                    VolumeDefinition volDfn = vol.getVolumeDefinition();
                    assertTrue(volDfn.getFlags().isSet(SYS_CTX, VolumeDefinition.Flags.DELETE));
                    {
                        Props volDfnProps = volDfn.getProps(SYS_CTX);
                        assertNotNull(volDfnProps);
                        assertEquals(volDfnTestValue, volDfnProps.getProp(volDfnTestKey));
                        String dfltLvmExtentSize = "4096"; // from the storPool1
                        assertEquals(
                            dfltLvmExtentSize,
                            volDfnProps.getProp(
                                InternalApiConsts.ALLOCATION_GRANULARITY,
                                StorageConstants.NAMESPACE_INTERNAL
                            )
                        );
                        assertEquals(2, volDfnProps.size());
                    }
                    assertEquals(res.getResourceDefinition(), volDfn.getResourceDefinition());
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
                ResourceConnection resCon = res.getAbsResourceConnection(SYS_CTX, res2);
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
                String dfltLvmExtentSize = "4096"; // due to LVM
                assertEquals(
                    dfltLvmExtentSize,
                    storPoolConfig.getProp(
                        InternalApiConsts.ALLOCATION_GRANULARITY,
                        StorageConstants.NAMESPACE_INTERNAL
                    )
                );
                assertEquals(2, storPoolConfig.size());
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
        Node node2 = nodeFactory.create(
            SYS_CTX,
            nodeName2,
            Node.Type.CONTROLLER,
            null
        );
        nodesMap.put(nodeName, node);
        nodesMap.put(nodeName2, node2);
        Map<Node, Node.InitMaps> allNodes = dbDriver.loadAll(null);
        assertEquals(2, allNodes.size());

        Iterator<Node> loadedNodesIterator = allNodes.keySet().iterator();
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

        nodeFactory.create(SYS_CTX, nodeName, initialType, null);
    }

}
