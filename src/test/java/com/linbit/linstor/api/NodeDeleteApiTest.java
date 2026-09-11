package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class NodeDeleteApiTest extends ApiTestBase
{
    @Inject
    private Provider<CtrlNodeDeleteApiCallHandler> nodeDeleteApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

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

    private SatelliteState satelliteState;

    public NodeDeleteApiTest() throws Exception
    {
        testNodeName = new NodeName("TestSatellite");
        testRscName = new ResourceName("TestRsc");
        testStorPoolName = new StorPoolName("TestStorPool");
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        satelliteState = new SatelliteState();

        stubSatellitePeer(mockSatellite, mockExtToolsMgr, satelliteState, false);
        stubAllExtToolsSupported(mockExtToolsMgr);

        commitAndCleanUp(true);
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void deleteOfflineNodeWithoutResources() throws Exception
    {
        createSatelliteNode();

        evaluateTest(
            new DeleteNodeCall(
                testNodeName.displayValue,
                ApiConsts.DELETED
            )
        );

        assertThat(nodesMap.get(testNodeName)).isNull();
        Mockito.verify(mockSatellite).closeConnection();
    }

    @Test
    public void deleteUnknownNode() throws Exception
    {
        evaluateTest(
            new DeleteNodeCall(
                "UnknownNode",
                ApiConsts.WARN_NOT_FOUND
            )
        );
    }

    @Test
    public void deleteEvictedNode() throws Exception
    {
        createSatelliteNode(Node.Flags.EVICTED);

        evaluateTest(
            new DeleteNodeCall(
                testNodeName.displayValue,
                ApiConsts.WARN_NODE_EVICTED
            )
        );

        // an evicted node can only be removed with "node lost"
        assertThat(nodesMap.get(testNodeName)).isNotNull();
    }

    @Test
    public void deleteNodeWithResource() throws Exception
    {
        createSatelliteNode();
        createResourceOnNode();

        evaluateTest(
            new DeleteNodeCall(
                testNodeName.displayValue,
                // node marked for deletion
                ApiConsts.DELETED,
                // satellite is offline
                ApiConsts.WARN_NOT_CONNECTED,
                // resource deletion notification
                ApiConsts.MODIFIED,
                // node deleted after last resource was removed
                ApiConsts.DELETED
            )
        );

        assertThat(nodesMap.get(testNodeName)).isNull();
        assertThat(rscDfnMap.get(testRscName).getResourceCount()).isEqualTo(0);
    }

    @Test
    public void deleteNodeWithResourceInUse() throws Exception
    {
        createSatelliteNode();
        createResourceOnNode();

        satelliteState.setOnResource(testRscName, SatelliteResourceState::setInUse, Boolean.TRUE);

        evaluateTest(
            new DeleteNodeCall(
                testNodeName.displayValue,
                ApiConsts.FAIL_IN_USE,
                ApiConsts.FAIL_NODE_HAS_USED_RSC
            )
        );

        Node node = nodesMap.get(testNodeName);
        assertThat(node).isNotNull();
        assertThat(node.getFlags().isSet(Node.Flags.DELETE)).isFalse();
        assertThat(rscDfnMap.get(testRscName).getResourceCount()).isEqualTo(1);
    }

    private Node createSatelliteNode(Node.Flags... flags) throws Exception
    {
        enterScope();

        Node node = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            flags.length == 0 ? null : flags
        );
        node.setPeer(mockSatellite);
        nodesMap.put(testNodeName, node);

        commitAndCleanUp(true);

        return node;
    }

    private void createResourceOnNode() throws Exception
    {
        enterScope();

        Node node = nodesMap.get(testNodeName);

        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(
            new SharedStorPoolName(testNodeName, testStorPoolName)
        );
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            DeviceProviderKind.LVM,
            fsm,
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = "NotTellingYou";
        drbdRscDfn.transportType = TransportType.IP;
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
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, testStorPoolName.displayValue);
        ctrlRscCrtApiHelper.createResourceDb(
            testNodeName.displayValue,
            testRscName.displayValue,
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

        commitAndCleanUp(true);
    }

    private class DeleteNodeCall extends AbsApiCallTester
    {
        private final String nodeName;

        DeleteNodeCall(String nodeNameRef, long... expectedRcs)
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
            nodeDeleteApiCallHandlerProvider.get().deleteNode(nodeName)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
