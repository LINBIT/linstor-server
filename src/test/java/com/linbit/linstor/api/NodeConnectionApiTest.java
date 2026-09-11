package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeConnectionApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apis.NodeConnectionApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class NodeConnectionApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_NODE_B = "TestNodeB";
    private static final String TEST_NET_IF = "eth0";
    private static final String AUX_KEY = ApiConsts.NAMESPC_AUXILIARY + "/test";

    @Inject
    private Provider<CtrlNodeConnectionApiCallHandler> nodeConnApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer mockSatelliteA;

    @Mock
    protected Peer mockSatelliteB;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeAName;
    private final NodeName testNodeBName;

    private Node testNodeA;
    private Node testNodeB;

    public NodeConnectionApiTest() throws Exception
    {
        testNodeAName = new NodeName(TEST_NODE_A);
        testNodeBName = new NodeName(TEST_NODE_B);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(mockExtToolsMgr.getSupportedLayers())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceLayerKind.values())));
        Mockito.when(mockExtToolsMgr.getSupportedProviders())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceProviderKind.values())));

        stubSatellitePeer(mockSatelliteA, mockExtToolsMgr, new SatelliteState(), true);
        stubSatellitePeer(mockSatelliteB, mockExtToolsMgr, new SatelliteState(), true);

        testNodeA = nodeFactory.create(testNodeAName, Node.Type.SATELLITE, null);
        testNodeA.setPeer(mockSatelliteA);
        nodesMap.put(testNodeAName, testNodeA);

        testNodeB = nodeFactory.create(testNodeBName, Node.Type.SATELLITE, null);
        testNodeB.setPeer(mockSatelliteB);
        nodesMap.put(testNodeBName, testNodeB);

        leaveScope();
    }

    private NodeConnection getNodeConn()
    {
        return NodeConnection.get(testNodeA, testNodeB);
    }

    private void createNetInterfaces() throws Exception
    {
        enterScope();
        netInterfaceFactory.create(
            testNodeA,
            new NetInterfaceName(TEST_NET_IF),
            new LsIpAddress("10.0.0.1"),
            null,
            null
        );
        netInterfaceFactory.create(
            testNodeB,
            new NetInterfaceName(TEST_NET_IF),
            new LsIpAddress("10.0.0.2"),
            null,
            null
        );
        leaveScope();
    }

    @Test
    public void modSetPropCreatesConnection() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        NodeConnection nodeConn = getNodeConn();
        assertThat(nodeConn).isNotNull();
        assertThat(nodeConn.getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modDeleteLastPropRemovesConnection() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(AUX_KEY)
        );

        // an empty node connection is cleaned up automatically
        assertThat(getNodeConn()).isNull();
    }

    @Test
    public void modDeletePropKeepsConnectionWithRemainingProps() throws Exception
    {
        String otherAuxKey = ApiConsts.NAMESPC_AUXILIARY + "/other";
        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
                .overrideProps(otherAuxKey, "otherValue")
        );

        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED,
                ApiConsts.MODIFIED
            )
                .deleteProp(AUX_KEY)
        );

        NodeConnection nodeConn = getNodeConn();
        assertThat(nodeConn).isNotNull();
        assertThat(nodeConn.getProps().getProp(AUX_KEY)).isNull();
        assertThat(nodeConn.getProps().getProp(otherAuxKey)).isEqualTo("otherValue");
    }

    @Test
    public void modDeleteNamespaceRemovesConnection() throws Exception
    {
        String otherAuxKey = ApiConsts.NAMESPC_AUXILIARY + "/other";
        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
                .overrideProps(otherAuxKey, "otherValue")
        );

        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.MODIFIED)
                .deleteNamespace(ApiConsts.NAMESPC_AUXILIARY)
        );

        // deleting the last props empties the connection, which is then cleaned up
        assertThat(getNodeConn()).isNull();
    }

    @Test
    public void modNonExistentWithoutProps() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.WARN_NOT_FOUND)
        );

        assertThat(getNodeConn()).isNull();
    }

    @Test
    public void modUnknownNodeWithoutProps() throws Exception
    {
        // without override props no connection is created; the unknown node simply
        // results in the connection not being found
        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.WARN_NOT_FOUND)
                .setNodeName1("UnknownNode")
        );
    }

    @Test
    public void modUnknownNodeWithProps() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName1("UnknownNode")
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modSameNodeWithProps() throws Exception
    {
        // characterization: a connection between a node and itself is only caught by an
        // ImplementationError deep in NodeConnection.createWithSorting instead of a
        // user-friendly validation error
        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_IMPL_ERROR)
                .setNodeName2(TEST_NODE_A)
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modWrongUuid() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_UUID_NODE_CONN)
                .nodeConnUuid(randomUUID())
                .overrideProps(AUX_KEY, "otherValue")
        );

        assertThat(getNodeConn().getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );

        // the implicitly created connection is rolled back
        assertThat(getNodeConn()).isNull();
    }

    @Test
    public void modPathProp() throws Exception
    {
        createNetInterfaces();

        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(pathKey(TEST_NODE_A), TEST_NET_IF)
                .overrideProps(pathKey(TEST_NODE_B), TEST_NET_IF)
        );

        NodeConnection nodeConn = getNodeConn();
        assertThat(nodeConn).isNotNull();
        assertThat(nodeConn.getProps().getProp(pathKey(TEST_NODE_A))).isEqualTo(TEST_NET_IF);
        assertThat(nodeConn.getProps().getProp(pathKey(TEST_NODE_B))).isEqualTo(TEST_NET_IF);
    }

    @Test
    public void modPathPropUnknownNode() throws Exception
    {
        createNetInterfaces();

        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(pathKey("UnknownNode"), TEST_NET_IF)
        );
    }

    @Test
    public void modPathPropUnknownNetIf() throws Exception
    {
        createNetInterfaces();

        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(pathKey(TEST_NODE_A), "unknownNetIf")
        );
    }

    @Test
    public void modPathPropMalformedKey() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(ApiConsts.NAMESPC_CONNECTION_PATHS + "/incomplete", TEST_NET_IF)
        );
    }

    @Test
    public void listNodeConnections() throws Exception
    {
        evaluateTest(
            new ModifyNodeConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        enterScope();
        CtrlNodeConnectionApiCallHandler handler = nodeConnApiCallHandlerProvider.get();

        Collection<NodeConnectionApi> all = handler.listNodeConnections(null, null);
        assertThat(all).hasSize(1);
        NodeConnectionApi conn = all.iterator().next();
        assertThat(conn.getProps()).containsEntry(AUX_KEY, "value");

        Collection<NodeConnectionApi> byNode = handler.listNodeConnections(TEST_NODE_A, null);
        assertThat(byNode).hasSize(1);

        Collection<NodeConnectionApi> byPair = handler.listNodeConnections(TEST_NODE_A, TEST_NODE_B);
        assertThat(byPair).hasSize(1);
        assertThat(byPair.iterator().next().getUuid()).isEqualTo(getNodeConn().getUuid());
    }

    @Test
    public void listNodeConnectionsEmpty() throws Exception
    {
        enterScope();
        CtrlNodeConnectionApiCallHandler handler = nodeConnApiCallHandlerProvider.get();

        assertThat(handler.listNodeConnections(null, null)).isEmpty();

        // a pair without a connection returns a placeholder pojo without uuid
        Collection<NodeConnectionApi> byPair = handler.listNodeConnections(TEST_NODE_A, TEST_NODE_B);
        assertThat(byPair).hasSize(1);
        NodeConnectionApi conn = byPair.iterator().next();
        assertThat(conn.getUuid()).isNull();
        assertThat(conn.getProps()).isEmpty();
    }

    private static String pathKey(String nodeName)
    {
        return ApiConsts.NAMESPC_CONNECTION_PATHS + "/path1/" + nodeName;
    }

    private class ModifyNodeConnCall extends AbsApiCallTester
    {
        private java.util.UUID nodeConnUuid;
        private String nodeName1;
        private String nodeName2;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deleteNamespaces;

        ModifyNodeConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_NODE_CONN,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            nodeConnUuid = null; // default: do not check against uuid
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deleteNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            nodeConnApiCallHandlerProvider.get().modifyNodeConn(
                nodeConnUuid,
                nodeName1,
                nodeName2,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        ModifyNodeConnCall nodeConnUuid(java.util.UUID uuid)
        {
            nodeConnUuid = uuid;
            return this;
        }

        ModifyNodeConnCall setNodeName1(String nodeNameRef)
        {
            nodeName1 = nodeNameRef;
            return this;
        }

        ModifyNodeConnCall setNodeName2(String nodeNameRef)
        {
            nodeName2 = nodeNameRef;
            return this;
        }

        ModifyNodeConnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyNodeConnCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyNodeConnCall deleteNamespace(String namespace)
        {
            deleteNamespaces.add(namespace);
            return this;
        }
    }
}
