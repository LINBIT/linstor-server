package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerClosingConnectionException;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class RscDeleteApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_NODE_2_NAME = "TestSatellite2";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_DISKLESS_SP_NAME = "TestDisklessPool";

    @Inject
    private Provider<CtrlRscDeleteApiCallHandler> rscDeleteApiCallHandlerProvider;
    @Inject
    private Provider<CtrlRscDfnDeleteApiCallHandler> rscDfnDeleteApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected Peer mockSatellite2;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeName;
    private final NodeName testNode2Name;
    private final ResourceName testRscName;
    private final StorPoolName testStorPoolName;
    private final StorPoolName testDisklessStorPoolName;

    private Node testSatelliteNode;
    private ResourceDefinition testRscDfn;
    private SatelliteState satelliteState;

    public RscDeleteApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
        testNode2Name = new NodeName(TEST_NODE_2_NAME);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
        testDisklessStorPoolName = new StorPoolName(TEST_DISKLESS_SP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        satelliteState = new SatelliteState();

        stubSatellitePeer(mockSatellite, mockExtToolsMgr, satelliteState, false);
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), false);
        stubAllExtToolsSupported(mockExtToolsMgr);

        testSatelliteNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testSatelliteNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testSatelliteNode);

        LayerPayload payload = new LayerPayload();
        payload.getDrbdRscDfn().sharedSecret = "NotTellingYou";
        payload.getDrbdRscDfn().transportType = TransportType.IP;
        testRscDfn = resourceDefinitionFactory.create(
            testRscName,
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup()
        );
        rscDfnMap.put(testRscName, testRscDfn);

        volumeDefinitionFactory.create(
            testRscDfn,
            new VolumeNumber(0),
            1000,
            100 * 1024L,
            null
        );

        ctrlConf.setProp(
            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
            randomUUID().toString(),
            ApiConsts.NAMESPC_CLUSTER
        );

        leaveScope();
    }

    /*
     * resource delete tests
     */

    @Test
    public void deleteRscUnknownNode() throws Exception
    {
        evaluateTest(
            new DeleteRscCall(ApiConsts.WARN_NOT_FOUND)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void deleteRscUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new DeleteRscCall(ApiConsts.WARN_NOT_FOUND)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void deleteRscNotDeployedOnNode() throws Exception
    {
        // node and resource definition exist, but the resource was never deployed on the node
        evaluateTest(
            new DeleteRscCall(ApiConsts.WARN_NOT_FOUND)
        );
    }

    @Test
    public void deleteRscInUse() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        satelliteState.setOnResource(testRscName, SatelliteResourceState::setInUse, Boolean.TRUE);

        evaluateTest(
            new DeleteRscCall(ApiConsts.FAIL_IN_USE)
        );

        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
    }

    @Test
    public void deleteRscOfflineSatellite() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new DeleteRscCall(
                // resource preparing for deletion (DRBD cleanup step)
                ApiConsts.DELETED,
                // satellite is offline
                ApiConsts.WARN_NOT_CONNECTED,
                // "Preparing deletion of resource on ..."
                ApiConsts.MODIFIED,
                // resource marked for deletion
                ApiConsts.DELETED,
                // satellite is still offline, resource data must not be removed
                ApiConsts.WARN_NOT_CONNECTED,
                // updated resync-after entries (auto helper)
                ApiConsts.MASK_INFO,
                // updated resync-after entries (resource definition props update)
                ApiConsts.MASK_INFO
            )
        );

        // the satellite never confirmed the undeploy, so the resource is only marked for deletion
        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isTrue();
        Iterator<Volume> vlmIt = rsc.iterateVolumes();
        assertThat(vlmIt.hasNext()).isTrue();
        assertThat(vlmIt.next().getFlags().isSet(Volume.Flags.DELETE)).isTrue();
    }

    @Test
    public void deleteRscOnlineSatellite() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new DeleteRscCall(
                // resource preparing for deletion (DRBD cleanup step)
                ApiConsts.DELETED,
                // "Preparing deletion of resource on ..."
                ApiConsts.MODIFIED,
                // resource marked for deletion
                ApiConsts.DELETED,
                // "Cleaning up ... on ..."
                ApiConsts.MODIFIED,
                // resource deletion complete
                ApiConsts.DELETED,
                // updated resync-after entries (three auto helper / props update runs)
                ApiConsts.MASK_INFO,
                ApiConsts.MASK_INFO,
                ApiConsts.MASK_INFO
            )
        );

        assertThat(testSatelliteNode.getResource(testRscName)).isNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(0);
    }

    @Test
    public void reconnectResumesDrbdDeleteMarkedResource() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        // the satellite connection dies while the DRBD-delete update is in flight, taking the
        // in-memory continuation of the deletion with it
        Mockito.when(mockSatellite.apiCall(Mockito.anyString(), Mockito.any()))
            .thenReturn(Flux.error(new PeerClosingConnectionException()));

        evaluateTest(new DeleteRscCall(), false);

        // the deletion is now stuck in the intermediate DRBD_DELETE state
        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DRBD_DELETE)).isTrue();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
        Iterator<Volume> vlmIt = rsc.iterateVolumes();
        assertThat(vlmIt.hasNext()).isTrue();
        assertThat(vlmIt.next().getFlags().isSet(Volume.Flags.DRBD_DELETE)).isTrue();

        // satellite reconnects and processes updates again
        Mockito.when(mockSatellite.apiCall(Mockito.anyString(), Mockito.any())).thenReturn(Flux.empty());

        Collection<Flux<ApiCallRc>> fluxes = rscDeleteApiCallHandlerProvider.get()
            .resourceDefinitionConnected(testRscDfn, makeDeleteRscContext());
        assertThat(fluxes).isNotEmpty();
        for (Flux<ApiCallRc> flux : fluxes)
        {
            collect(flux);
        }

        // the re-driven deletion sequence must complete
        assertThat(testSatelliteNode.getResource(testRscName)).isNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(0);
    }

    @Test
    public void reconnectResumesDeleteMarkedResource() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        // satellite offline: the delete only reaches the DELETE state (see deleteRscOfflineSatellite)
        evaluateTest(new DeleteRscCall(), false);

        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isTrue();

        // satellite reconnects and processes updates again
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);

        Collection<Flux<ApiCallRc>> fluxes = rscDeleteApiCallHandlerProvider.get()
            .resourceDefinitionConnected(testRscDfn, makeDeleteRscContext());
        assertThat(fluxes).isNotEmpty();
        for (Flux<ApiCallRc> flux : fluxes)
        {
            collect(flux);
        }

        // the re-driven deletion sequence must complete
        assertThat(testSatelliteNode.getResource(testRscName)).isNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(0);
    }

    @Test
    public void undeleteWhileDrbdDeletePendingCancelsDeletion() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);
        UUID rscUuid = testSatelliteNode.getResource(testRscName).getUuid();

        // while the satellite processes the DRBD_DELETE update, a concurrent actor (e.g. BalanceResourcesTask or
        // a user's "r c" on the same node) re-creates the resource, which un-deletes it instead of failing
        stubUndeleteOnSatelliteUpdate(
            rsc -> rsc.getStateFlags().isSet(Resource.Flags.DRBD_DELETE) &&
                rsc.getStateFlags().isUnset(Resource.Flags.DELETE)
        );

        evaluateTest(
            new DeleteRscCall(
                // resource preparing for deletion (DRBD cleanup step)
                ApiConsts.DELETED,
                // "Preparing deletion of resource on ..."
                ApiConsts.MODIFIED,
                // "Deleting resource" stage finds the DRBD_DELETE flag gone and cancels
                ApiConsts.MASK_WARN,
                // updated resync-after entries (auto helper of the prepare stage)
                ApiConsts.MASK_INFO
            )
        );

        assertUndeletedResourceIntact(rscUuid);
    }

    @Test
    public void undeleteWhileDeletePendingKeepsResource() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);
        UUID rscUuid = testSatelliteNode.getResource(testRscName).getUuid();

        // the DRBD cleanup went through; while the satellite processes the DELETE update, a concurrent actor
        // re-creates the resource. The final "delete data" stage must not remove the un-deleted resource
        // from the database (which would leave the satellites with a resource the controller no longer knows)
        stubUndeleteOnSatelliteUpdate(rsc -> rsc.getStateFlags().isSet(Resource.Flags.DELETE));

        evaluateTest(
            new DeleteRscCall(
                // resource preparing for deletion (DRBD cleanup step)
                ApiConsts.DELETED,
                // "Preparing deletion of resource on ..."
                ApiConsts.MODIFIED,
                // resource marked for deletion
                ApiConsts.DELETED,
                // "Cleaning up ... on ..."
                ApiConsts.MODIFIED,
                // "Delete resource data" stage finds the DELETE flag gone and cancels
                ApiConsts.MASK_WARN,
                // updated resync-after entries (auto helper / props update runs)
                ApiConsts.MASK_INFO,
                ApiConsts.MASK_INFO
            )
        );

        assertUndeletedResourceIntact(rscUuid);
    }

    @Test
    public void deleteLastDiskfulWithDisklessAttachedRejected() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_2_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new DeleteRscCall(ApiConsts.FAIL_IN_USE)
        );

        // the diskful resource must not be touched while diskless resources depend on it
        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(2);
    }

    /*
     * resource definition delete tests
     */

    @Test
    public void deleteRscDfnUnknown() throws Exception
    {
        evaluateTest(
            new DeleteRscDfnCall(ApiConsts.WARN_NOT_FOUND)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void deleteRscDfnWithoutResources() throws Exception
    {
        evaluateTest(
            new DeleteRscDfnCall(ApiConsts.DELETED)
        );

        assertThat(rscDfnMap.get(testRscName)).isNull();
    }

    @Test
    public void deleteRscDfnWithResourceOfflineSatellite() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new DeleteRscDfnCall(
                // resource definition marked for deletion
                ApiConsts.DELETED,
                // satellite is offline
                ApiConsts.WARN_NOT_CONNECTED
            )
        );

        // resources could not be undeployed, the resource definition remains marked for deletion
        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        assertThat(rscDfn).isNotNull();
        assertThat(rscDfn.getFlags().isSet(ResourceDefinition.Flags.DELETE)).isTrue();
        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isTrue();
    }

    @Test
    public void deleteRscDfnWithResourceOnlineSatellite() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new DeleteRscDfnCall(
                // resource definition marked for deletion
                ApiConsts.DELETED,
                // resource marked for deletion
                ApiConsts.MODIFIED,
                // updated resync-after entries
                ApiConsts.MASK_INFO,
                // resource definition deleted
                ApiConsts.DELETED
            )
        );

        assertThat(rscDfnMap.get(testRscName)).isNull();
        assertThat(testSatelliteNode.getResource(testRscName)).isNull();
    }

    /*
     * helpers
     */

    /**
     * Stubs the satellite so that the first resource update whose in-memory resource matches the given
     * predicate triggers a concurrent "resource create" on the same node. Since the resource still exists
     * (flagged for deletion), {@link CtrlRscCrtApiHelper#createResourceDb} re-uses it and clears its
     * DELETE / DRBD_DELETE flags, i.e. un-deletes it while the delete flux is still running.
     */
    private void stubUndeleteOnSatelliteUpdate(Predicate<Resource> triggerRef)
    {
        AtomicBoolean undeleted = new AtomicBoolean(false);
        Mockito.when(mockSatellite.apiCall(Mockito.eq(InternalApiConsts.API_CHANGED_RSC), Mockito.any()))
            .thenAnswer(invocation ->
                // some update stages build the peer call while still inside their LinStorScope; defer the
                // concurrent modification to subscription time, which happens after that scope was left
                Flux.defer(() ->
                {
                    Resource rsc = testSatelliteNode.getResource(testRscName);
                    if (rsc != null && !undeleted.get() && triggerRef.test(rsc))
                    {
                        undeleted.set(true);
                        try
                        {
                            createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);
                        }
                        catch (Exception exc)
                        {
                            return Flux.error(exc);
                        }
                    }
                    return Flux.empty();
                })
            );
    }

    private void assertUndeletedResourceIntact(UUID expectedUuid)
    {
        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.isDeleted()).isFalse();
        assertThat(rsc.getUuid()).isEqualTo(expectedUuid);
        assertThat(rsc.getStateFlags().isUnset(Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE)).isTrue();
        Iterator<Volume> vlmIt = rsc.iterateVolumes();
        assertThat(vlmIt.hasNext()).isTrue();
        assertThat(vlmIt.next().getFlags().isUnset(Volume.Flags.DELETE, Volume.Flags.DRBD_DELETE)).isTrue();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(1);
    }

    private ResponseContext makeDeleteRscContext()
    {
        return CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeDeleteOperation(),
            TEST_NODE_NAME,
            TEST_RSC_NAME
        );
    }

    private Node createSecondNode() throws Exception
    {
        enterScope();

        Node node = nodeFactory.create(
            testNode2Name,
            Node.Type.SATELLITE,
            null
        );
        node.setPeer(mockSatellite2);
        nodesMap.put(testNode2Name, node);

        leaveScope();

        return node;
    }

    private StorPool createStorPool(Node node, StorPoolName storPoolName, DeviceProviderKind kind) throws Exception
    {
        enterScope();

        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(storPoolName);
            storPoolDfnMap.put(storPoolName, storPoolDfn);
        }
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            kind,
            getFreeSpaceMgr(storPoolDfn, node),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        leaveScope();

        return storPool;
    }

    private void createRscOnNode(String nodeNameStr, long flags, String storPoolNameStr) throws Exception
    {
        enterScope();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeNameStr,
            TEST_RSC_NAME,
            flags,
            rscProps,
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            null,
            null
        );

        leaveScope();
    }

    private class DeleteRscCall extends AbsApiCallTester
    {
        private String nodeName;
        private String rscName;

        DeleteRscCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            rscName = TEST_RSC_NAME;
        }

        DeleteRscCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        DeleteRscCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscDeleteApiCallHandlerProvider.get().deleteResource(nodeName, rscName)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }

    private class DeleteRscDfnCall extends AbsApiCallTester
    {
        private String rscName;

        DeleteRscDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC_DFN,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            rscName = TEST_RSC_NAME;
        }

        DeleteRscDfnCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscDfnDeleteApiCallHandlerProvider.get().deleteResourceDefinition(rscName)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
