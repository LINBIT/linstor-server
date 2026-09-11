package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeLostApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class NodeLostApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";

    @Inject
    private Provider<CtrlNodeLostApiCallHandler> nodeLostApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    // instantiating the PingTask registers it with the ReconnectorTask, which lostNode uses to
    // deregister the peer of the lost node
    @Inject
    private PingTask pingTask;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeName;
    private final ResourceName testRscName;
    private final StorPoolName testStorPoolName;

    public NodeLostApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        stubSatellitePeer(mockSatellite, mockExtToolsMgr, new SatelliteState(), false);
        stubAllExtToolsSupported(mockExtToolsMgr);

        leaveScope();
    }

    @Test
    public void lostUnknownNode() throws Exception
    {
        evaluateTest(
            new LostNodeCall(
                "UnknownNode",
                ApiConsts.WARN_NOT_FOUND
            )
        );
    }

    @Test
    public void lostOfflineNodeWithoutResources() throws Exception
    {
        createSatelliteNode();

        evaluateTest(
            new LostNodeCall(
                TEST_NODE_NAME,
                ApiConsts.DELETED
            )
        );

        assertThat(nodesMap.get(testNodeName)).isNull();
    }

    @Test
    public void lostOnlineNodeRejected() throws Exception
    {
        createSatelliteNode();
        Mockito.when(mockSatellite.isOnline()).thenReturn(true);

        evaluateTest(
            new LostNodeCall(
                TEST_NODE_NAME,
                ApiConsts.FAIL_EXISTS_NODE_CONN
            )
        );

        // node must still be registered
        assertThat(nodesMap.get(testNodeName)).isNotNull();
    }

    @Test
    public void lostNodeWithStorPool() throws Exception
    {
        createSatelliteNode();
        createStorPoolOnNode();

        evaluateTest(
            new LostNodeCall(
                TEST_NODE_NAME,
                ApiConsts.DELETED
            )
        );

        assertThat(nodesMap.get(testNodeName)).isNull();
        // the storage pool definition survives, but no storage pool references it anymore
        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(testStorPoolName);
        assertThat(storPoolDfn).isNotNull();
        assertThat(storPoolDfn.iterateStorPools().hasNext()).isFalse();
    }

    @Test
    public void lostNodeWithResource() throws Exception
    {
        createSatelliteNode();
        createResourceOnNode();

        evaluateTest(
            new LostNodeCall(
                TEST_NODE_NAME,
                ApiConsts.DELETED
            )
        );

        // the resource of the lost node is force-deleted together with the node
        assertThat(nodesMap.get(testNodeName)).isNull();
        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        assertThat(rscDfn).isNotNull();
        assertThat(rscDfn.getResourceCount()).isEqualTo(0);
    }

    private Node createSatelliteNode() throws Exception
    {
        enterScope();

        Node node = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        node.setPeer(mockSatellite);
        nodesMap.put(testNodeName, node);

        leaveScope();

        return node;
    }

    private StorPool createStorPoolOnNode() throws Exception
    {
        enterScope();

        Node node = nodesMap.get(testNodeName);

        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            DeviceProviderKind.LVM,
            getFreeSpaceMgr(storPoolDfn, node),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        leaveScope();

        return storPool;
    }

    private void createResourceOnNode() throws Exception
    {
        createStorPoolOnNode();

        enterScope();

        LayerPayload payload = new LayerPayload();
        payload.getDrbdRscDfn().sharedSecret = "NotTellingYou";
        payload.getDrbdRscDfn().transportType = TransportType.IP;
        ResourceDefinition rscDfn = resourceDefinitionFactory.create(
            testRscName,
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup()
        );
        rscDfnMap.put(testRscName, rscDfn);

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, TEST_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            TEST_NODE_NAME,
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

        leaveScope();
    }

    private class LostNodeCall extends AbsApiCallTester
    {
        private final String nodeName;

        LostNodeCall(String nodeNameRef, long... expectedRcs)
        {
            super(
                ApiConsts.MASK_NODE,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName = nodeNameRef;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            nodeLostApiCallHandlerProvider.get().lostNode(nodeName)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
