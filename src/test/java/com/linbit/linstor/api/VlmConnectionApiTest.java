package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
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
public class VlmConnectionApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_NODE_B = "TestNodeB";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final int TEST_VLM_NR = 0;
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

    public VlmConnectionApiTest() throws Exception
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

        testNodeA = createSatelliteNode(testNodeAName, mockSatelliteA);
        testNodeB = createSatelliteNode(testNodeBName, mockSatelliteB);

        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, TEST_VLM_NR)
            .setSize(100 * 1024L)
            .build();

        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        leaveScope();
    }

    private Node createSatelliteNode(NodeName nodeName, Peer peer) throws Exception
    {
        Node node = nodeFactory.create(nodeName, Node.Type.SATELLITE, null);
        node.setPeer(peer);
        nodesMap.put(nodeName, node);

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

    private VolumeConnection getVlmConn() throws Exception
    {
        Volume vlmA = testNodeA.getResource(testRscName).getVolume(new VolumeNumber(TEST_VLM_NR));
        Volume vlmB = testNodeB.getResource(testRscName).getVolume(new VolumeNumber(TEST_VLM_NR));
        return VolumeConnection.get(vlmA, vlmB);
    }

    /*
     * create tests
     */

    @Test
    public void crtSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.CREATED)
        );

        assertThat(getVlmConn()).isNotNull();
    }

    @Test
    public void crtWithProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.CREATED
            )
                .setProp(AUX_KEY, "value")
        );

        VolumeConnection vlmConn = getVlmConn();
        assertThat(vlmConn).isNotNull();
        assertThat(vlmConn.getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void crtSecondExists() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.CREATED)
        );
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.FAIL_EXISTS_VLM_CONN)
        );
    }

    @Test
    public void crtUnknownNode() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName1("UnknownNode")
        );
    }

    @Test
    public void crtUnknownVlmNr() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.FAIL_NOT_FOUND_VLM)
                .setVlmNr(4)
        );
    }

    /*
     * modify tests
     */

    @Test
    public void modSetProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.CREATED)
        );

        evaluateTest(
            new ModifyVlmConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        assertThat(getVlmConn().getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modDeleteProp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.CREATED
            )
                .setProp(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyVlmConnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(AUX_KEY)
        );

        assertThat(getVlmConn().getProps().getProp(AUX_KEY)).isNull();
    }

    @Test
    public void modDeleteNamespace() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.CREATED
            )
                .setProp(AUX_KEY, "value")
                .setProp(ApiConsts.NAMESPC_AUXILIARY + "/other", "otherValue")
        );

        evaluateTest(
            new ModifyVlmConnCall(ApiConsts.MODIFIED)
                .deleteNamespace(ApiConsts.NAMESPC_AUXILIARY)
        );

        assertThat(getVlmConn().getProps().isEmpty()).isTrue();
    }

    @Test
    public void modWrongUuid() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.CREATED)
        );

        evaluateTest(
            new ModifyVlmConnCall(ApiConsts.FAIL_UUID_VLM_CONN)
                .vlmConnUuid(randomUUID())
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.CREATED)
        );

        evaluateTest(
            new ModifyVlmConnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );

        assertThat(getVlmConn().getProps().isEmpty()).isTrue();
    }

    @Test
    public void modNonExistentConn() throws Exception
    {
        // unlike resource connections, volume connections are not created implicitly by modify
        enterScope();
        evaluateTest(
            new ModifyVlmConnCall(ApiConsts.FAIL_NOT_FOUND_VLM_CONN)
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modUnknownNode() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyVlmConnCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName1("UnknownNode")
                .overrideProps(AUX_KEY, "value")
        );
    }

    /*
     * delete tests
     */

    @Test
    public void delSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmConnCall(ApiConsts.CREATED)
        );

        evaluateTest(
            new DeleteVlmConnCall(ApiConsts.DELETED)
        );

        assertThat(getVlmConn()).isNull();
    }

    @Test
    public void delNonExistent() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteVlmConnCall(ApiConsts.WARN_NOT_FOUND)
        );
    }

    @Test
    public void delUnknownNode() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteVlmConnCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName1("UnknownNode")
        );
    }

    private class CreateVlmConnCall extends AbsApiCallTester
    {
        private String nodeName1;
        private String nodeName2;
        private String rscName;
        private int vlmNr;
        private final Map<String, String> props;

        CreateVlmConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM_CONN,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            rscName = TEST_RSC_NAME;
            vlmNr = TEST_VLM_NR;
            props = new TreeMap<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().createVolumeConnection(
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                props
            );
        }

        CreateVlmConnCall setNodeName1(String nodeNameRef)
        {
            nodeName1 = nodeNameRef;
            return this;
        }

        CreateVlmConnCall setVlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
            return this;
        }

        CreateVlmConnCall setProp(String key, String value)
        {
            props.put(key, value);
            return this;
        }
    }

    private class ModifyVlmConnCall extends AbsApiCallTester
    {
        private java.util.UUID vlmConnUuid;
        private String nodeName1;
        private String nodeName2;
        private String rscName;
        private int vlmNr;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deleteNamespaces;

        ModifyVlmConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM_CONN,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            vlmConnUuid = null; // default: do not check against uuid
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            rscName = TEST_RSC_NAME;
            vlmNr = TEST_VLM_NR;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deleteNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().modifyVlmConn(
                vlmConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            );
        }

        ModifyVlmConnCall vlmConnUuid(java.util.UUID uuid)
        {
            vlmConnUuid = uuid;
            return this;
        }

        ModifyVlmConnCall setNodeName1(String nodeNameRef)
        {
            nodeName1 = nodeNameRef;
            return this;
        }

        ModifyVlmConnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyVlmConnCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyVlmConnCall deleteNamespace(String namespace)
        {
            deleteNamespaces.add(namespace);
            return this;
        }
    }

    private class DeleteVlmConnCall extends AbsApiCallTester
    {
        private String nodeName1;
        private String nodeName2;
        private String rscName;
        private int vlmNr;

        DeleteVlmConnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM_CONN,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName1 = TEST_NODE_A;
            nodeName2 = TEST_NODE_B;
            rscName = TEST_RSC_NAME;
            vlmNr = TEST_VLM_NR;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().deleteVolumeConnection(
                nodeName1,
                nodeName2,
                rscName,
                vlmNr
            );
        }

        DeleteVlmConnCall setNodeName1(String nodeNameRef)
        {
            nodeName1 = nodeNameRef;
            return this;
        }
    }
}
