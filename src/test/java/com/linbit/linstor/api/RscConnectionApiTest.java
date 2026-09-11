package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.event.EventSerializerDescriptor;
import com.linbit.linstor.event.WatchStore;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class RscConnectionApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_NODE_B = "TestNodeB";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_NET_IF = "eth0";
    private static final String AUX_KEY = ApiConsts.NAMESPC_AUXILIARY + "/test";

    @Inject
    private Provider<CtrlApiCallHandler> ctrlApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    // dependencies of the CtrlApiCallHandler facade that no test module provides
    @Bind
    @Mock
    protected WatchStore watchStore;

    @Bind
    protected Map<String, EventSerializer> eventSerializers = Collections.emptyMap();

    @Bind
    protected Map<String, EventSerializerDescriptor> eventSerializerDescriptors = Collections.emptyMap();

    @Mock
    protected Peer mockSatelliteA;

    @Mock
    protected Peer mockSatelliteB;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeAName;
    private final NodeName testNodeBName;
    private final ResourceName testRscName;
    private final StorPoolName testStorPoolName;

    private Node testNodeA;
    private Node testNodeB;

    public RscConnectionApiTest() throws Exception
    {
        testNodeAName = new NodeName(TEST_NODE_A);
        testNodeBName = new NodeName(TEST_NODE_B);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        stubAllExtToolsSupported(mockExtToolsMgr);

        stubSatellitePeer(mockSatelliteA, mockExtToolsMgr, new SatelliteState(), true);
        stubSatellitePeer(mockSatelliteB, mockExtToolsMgr, new SatelliteState(), true);

        testNodeA = createSatelliteNode(testNodeAName, mockSatelliteA, "10.0.0.1");
        testNodeB = createSatelliteNode(testNodeBName, mockSatelliteB, "10.0.0.2");

        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);

        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        leaveScope();
    }

    private Node createSatelliteNode(NodeName nodeName, Peer peer, String ipAddr) throws Exception
    {
        Node node = nodeFactory.create(nodeName, Node.Type.SATELLITE, null);
        node.setPeer(peer);
        nodesMap.put(nodeName, node);

        netInterfaceFactory.create(
            node,
            new NetInterfaceName(TEST_NET_IF),
            new LsIpAddress(ipAddr),
            null,
            null
        );

        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(testStorPoolName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
            storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        }
        FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(
            new SharedStorPoolName(nodeName, testStorPoolName)
        );
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            DeviceProviderKind.LVM,
            fsm,
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        return node;
    }

    private void createResourceOnNode(String nodeName) throws Exception
    {
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, TEST_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeName,
            TEST_RSC_NAME,
            0L,
            rscProps,
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            Resource.DiskfulBy.USER,
            false
        );
    }

    private ResourceConnection getRscConn() throws Exception
    {
        Resource rscA = testNodeA.getResource(testRscName);
        Resource rscB = testNodeB.getResource(testRscName);
        return rscA.getAbsResourceConnection(rscB);
    }

    /*
     * create tests (eagerly executed, need the testScope)
     */

    @Test
    public void crtSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscConnCall(ApiConsts.CREATED)
        );

        assertThat(getRscConn()).isNotNull();
    }

    @Test
    public void crtWithProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.CREATED
            )
                .setProp(AUX_KEY, "value")
        );

        ResourceConnection rscConn = getRscConn();
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void crtSecondExists() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscConnCall(ApiConsts.CREATED)
        );
        evaluateTest(
            new CreateRscConnCall(ApiConsts.FAIL_EXISTS_RSC_CONN)
        );
    }

    @Test
    public void crtUnknownNode() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscConnCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName1("UnknownNode")
        );
    }

    @Test
    public void crtUnknownRsc() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscConnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
        );
    }

    /*
     * modify tests (flux based, must not run within the testScope)
     */

    @Test
    public void modSetPropsCreatesConnection() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        // the connection is created implicitly by modify
        ResourceConnection rscConn = getRscConn();
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modDeleteProp() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyRscConnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(AUX_KEY)
        );

        // unlike node connections, resource connections are not cleaned up when empty
        ResourceConnection rscConn = getRscConn();
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().getProp(AUX_KEY)).isNull();
    }

    @Test
    public void modDeleteNamespace() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
                .overrideProps(ApiConsts.NAMESPC_AUXILIARY + "/other", "otherValue")
        );

        evaluateTest(
            new ModifyRscConnCall(ApiConsts.MODIFIED)
                .deleteNamespace(ApiConsts.NAMESPC_AUXILIARY)
        );

        ResourceConnection rscConn = getRscConn();
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().isEmpty()).isTrue();
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );

        // the implicitly created connection is rolled back
        assertThat(getRscConn()).isNull();
    }

    @Test
    public void modWrongUuid() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyRscConnCall(ApiConsts.FAIL_UUID_RSC_CONN)
                .rscConnUuid(randomUUID())
                .overrideProps(AUX_KEY, "otherValue")
        );

        assertThat(getRscConn().getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modUnknownNode() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName1("UnknownNode")
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modPathProp() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(pathKey(TEST_NODE_A), TEST_NET_IF)
                .overrideProps(pathKey(TEST_NODE_B), TEST_NET_IF)
        );

        ResourceConnection rscConn = getRscConn();
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().getProp(pathKey(TEST_NODE_A))).isEqualTo(TEST_NET_IF);
        assertThat(rscConn.getProps().getProp(pathKey(TEST_NODE_B))).isEqualTo(TEST_NET_IF);
    }

    @Test
    public void modPathPropUnknownNode() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(pathKey("UnknownNode"), TEST_NET_IF)
        );
    }

    @Test
    public void modPathPropUnknownNetIf() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(pathKey(TEST_NODE_A), "unknownNetIf")
        );
    }

    @Test
    public void modPathPropMalformedKey() throws Exception
    {
        evaluateTest(
            new ModifyRscConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(ApiConsts.NAMESPC_CONNECTION_PATHS + "/incomplete", TEST_NET_IF)
        );
    }

    /*
     * delete tests (eagerly executed, need the testScope)
     */

    @Test
    public void delSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscConnCall(ApiConsts.CREATED)
        );

        evaluateTest(
            new DeleteRscConnCall(ApiConsts.DELETED)
        );

        assertThat(getRscConn()).isNull();
    }

    @Test
    public void delNonExistent() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteRscConnCall(ApiConsts.FAIL_NOT_FOUND_RSC_CONN)
        );
    }

    @Test
    public void delUnknownRsc() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteRscConnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
        );
    }

    private static String pathKey(String nodeName)
    {
        return ApiConsts.NAMESPC_CONNECTION_PATHS + "/path1/" + nodeName;
    }

    private class CreateRscConnCall extends AbsApiCallTester
    {
        private String nodeName1;
        private String nodeName2;
        private String rscName;
        private final Map<String, String> props;

        CreateRscConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC_CONN,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            rscName = TEST_RSC_NAME;
            props = new TreeMap<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            ctrlApiCallHandlerProvider.get().createResourceConnection(
                nodeName1,
                nodeName2,
                rscName,
                props
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        CreateRscConnCall setNodeName1(String nodeNameRef)
        {
            nodeName1 = nodeNameRef;
            return this;
        }

        CreateRscConnCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        CreateRscConnCall setProp(String key, String value)
        {
            props.put(key, value);
            return this;
        }
    }

    private class ModifyRscConnCall extends AbsApiCallTester
    {
        private java.util.UUID rscConnUuid;
        private String nodeName1;
        private String nodeName2;
        private String rscName;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deleteNamespaces;

        ModifyRscConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC_CONN,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            rscConnUuid = null; // default: do not check against uuid
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            rscName = TEST_RSC_NAME;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deleteNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            ctrlApiCallHandlerProvider.get().modifyRscConn(
                rscConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        ModifyRscConnCall rscConnUuid(java.util.UUID uuid)
        {
            rscConnUuid = uuid;
            return this;
        }

        ModifyRscConnCall setNodeName1(String nodeNameRef)
        {
            nodeName1 = nodeNameRef;
            return this;
        }

        ModifyRscConnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyRscConnCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyRscConnCall deleteNamespace(String namespace)
        {
            deleteNamespaces.add(namespace);
            return this;
        }
    }

    private class DeleteRscConnCall extends AbsApiCallTester
    {
        private String nodeName1;
        private String nodeName2;
        private String rscName;

        DeleteRscConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC_CONN,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            rscName = TEST_RSC_NAME;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            ctrlApiCallHandlerProvider.get().deleteResourceConnection(
                nodeName1,
                nodeName2,
                rscName
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        DeleteRscConnCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }
    }
}
