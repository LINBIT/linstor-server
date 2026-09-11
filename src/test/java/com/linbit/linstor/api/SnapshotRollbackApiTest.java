package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRollbackApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.mgr.SnapshotRollbackManager;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

@SuppressWarnings("checkstyle:magicnumber")
public class SnapshotRollbackApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_NODE_2_NAME = "TestSatellite2";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_SNAP_NAME = "snap1";
    private static final long TEST_VLM_SIZE = 100 * 1024L;
    private static final String SHARED_SP_NAME = "SharedPool";
    private static final String SHARED_SPACE_NAME = "SharedSpace";

    @Inject
    private Provider<CtrlSnapshotCrtApiCallHandler> snapCrtApiCallHandlerProvider;
    @Inject
    private Provider<CtrlSnapshotRollbackApiCallHandler> snapRollbackApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    @Inject
    private SnapshotRollbackManager snapRollbackMgr;
    @Inject
    private BackupInfoManager backupInfoMgr;
    @Inject
    private ResourceStateEvent resourceStateEvent;

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
    private final SnapshotName testSnapName;
    private final StorPoolName testStorPoolName;

    private Node testNode;
    private StorPool testStorPool;

    private final AtomicInteger minorNrGenerator = new AtomicInteger(1000);

    public SnapshotRollbackApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
        testNode2Name = new NodeName(TEST_NODE_2_NAME);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testSnapName = new SnapshotName(TEST_SNAP_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(minorNrPoolMock.autoAllocate())
            .thenAnswer(ignoredContext -> minorNrGenerator.getAndIncrement());

        stubSatellitePeer(mockSatellite, mockExtToolsMgr, new SatelliteState(), false);
        stubAllExtToolsSupported(mockExtToolsMgr);
        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenReturn(Mono.just(Collections.emptyMap()));

        testNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testNode);

        leaveScope();
    }

    /*
     * rollback via clone strategy (safety-snapshot, delete, restore, recreate), used for all
     * non-ZFS resources
     */

    @Test
    public void rollbackCloneStrategySuccess() throws Exception
    {
        createDeployedSnapshot(DeviceProviderKind.LVM_THIN);

        evaluateTest(
            new RollbackSnapshotCall(cloneRollbackRcs())
        );

        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        // the safety snapshot was deleted again, only the user snapshot remains
        assertThat(rscDfn.getSnapshotDfns()).hasSize(1);
        assertThat(rscDfn.getSnapshotDfn(testSnapName)).isNotNull();
        // the resource was restored on the original node
        Resource rsc = rscDfn.getResource(testNodeName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
        assertThat(rscDfn.getVolumeDfnCount()).isEqualTo(1);
    }

    @Test
    public void rollbackCloneStrategyResetsVolumeDefinitions() throws Exception
    {
        createDeployedSnapshot(DeviceProviderKind.LVM_THIN);

        // resize the snapshotted volume definition and add a new one (including its volume on the
        // deployed resource) after the snapshot was taken
        enterScope();
        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        rscDfn.getVolumeDfn(new VolumeNumber(0)).setVolumeSize(TEST_VLM_SIZE * 2);
        VolumeDefinition newVlmDfn = volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 1)
            .setSize(TEST_VLM_SIZE)
            .build();
        LayerPayload payload = new LayerPayload();
        payload.putStorageVlmPayload(RscLayerSuffixes.SUFFIX_DATA, 1, testStorPool);
        volumeFactory.create(
            rscDfn.getResource(testNodeName),
            newVlmDfn,
            null,
            payload,
            null,
            Collections.emptyMap(),
            null
        );
        leaveScope();

        evaluateTest(
            new RollbackSnapshotCall(cloneRollbackRcs())
        );

        // the rollback restored the volume definitions to the state of the snapshot
        assertThat(rscDfn.getVolumeDfnCount()).isEqualTo(1);
        assertThat(rscDfn.getVolumeDfn(new VolumeNumber(0)).getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
        assertThat(rscDfn.getVolumeDfn(new VolumeNumber(1))).isNull();
    }

    @Test
    public void rollbackCloneStrategySharedSpSkipsInactiveCopy() throws Exception
    {
        // shared-SP resource active on the first node, an INACTIVE copy on the second, the snapshot
        // only exists on the active node: the rollback must not fail trying to make the resource
        // available on the second node (that would move the activation away from the snapshots);
        // the non-participating copy is simply not recreated
        deploySharedResource();
        createSharedSnapshot();

        ApiCallRc rollbackRc = collect(
            snapRollbackApiCallHandlerProvider.get().rollbackSnapshot(TEST_RSC_NAME, TEST_SNAP_NAME, null)
        );
        assertThat(rollbackRc).noneMatch(entry -> entry.isError());

        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        // the safety snapshot was deleted again, only the user snapshot remains
        assertThat(rscDfn.getSnapshotDfns()).hasSize(1);
        assertThat(rscDfn.getSnapshotDfn(testSnapName)).isNotNull();
        Resource rsc = rscDfn.getResource(testNodeName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rscDfn.getResource(testNode2Name)).isNull();
    }

    @Test
    public void rollbackCloneStrategySharedSpAllInactiveActivatesHolder() throws Exception
    {
        // with every copy of the shared storage pool inactive, the rollback has to activate the node
        // holding the snapshots first: both the safety snapshot and the rollback itself are only
        // performed by the node using the shared data
        deploySharedResource();
        createSharedSnapshot();

        enterScope();
        rscDfnMap.get(testRscName).getResource(testNodeName).getStateFlags()
            .enableFlags(Resource.Flags.INACTIVE);
        leaveScope();

        ApiCallRc rollbackRc = collect(
            snapRollbackApiCallHandlerProvider.get().rollbackSnapshot(TEST_RSC_NAME, TEST_SNAP_NAME, null)
        );
        assertThat(rollbackRc).noneMatch(entry -> entry.isError());

        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        assertThat(rscDfn.getSnapshotDfns()).hasSize(1);
        Resource rsc = rscDfn.getResource(testNodeName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rscDfn.getResource(testNode2Name)).isNull();
    }

    /*
     * rollback via "zfs rollback" strategy, only applicable if everything is ZFS
     */

    @Test
    public void rollbackZfsRollbackStrategySuccess() throws Exception
    {
        createDeployedSnapshot(DeviceProviderKind.ZFS_THIN);
        simulateRollbackResponses();

        evaluateTest(
            new RollbackSnapshotCall(
                // resource definition marked down for rollback
                ApiConsts.MODIFIED,
                // deactivated resource on satellite
                ApiConsts.MODIFIED,
                // volume definitions reset to snapshot state
                ApiConsts.MODIFIED,
                // rolled resource back on satellite
                ApiConsts.MODIFIED,
                // re-activated resource after rollback
                ApiConsts.MODIFIED
            )
        );

        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        Resource rsc = rscDfn.getResource(testNodeName);
        assertThat(rsc).isNotNull();
        // the rollback target property was removed after the successful rollback
        assertThat(rsc.getProps().getProp(ApiConsts.KEY_RSC_ROLLBACK_TARGET)).isNull();
        // the DRBD "down" flag set for the rollback is cleared (and committed) again
        Map<String, DrbdRscDfnData<Resource>> drbdRscDfnDataMap = rscDfn.getLayerData(DeviceLayerKind.DRBD);
        assertThat(drbdRscDfnDataMap).isNotEmpty();
        for (DrbdRscDfnData<Resource> drbdRscDfnData : drbdRscDfnDataMap.values())
        {
            assertThat(drbdRscDfnData.isDown()).isFalse();
        }
    }

    /**
     * The "zfs rollback" strategy has to wait for the rolled-back resources to become ready
     * again (analogous to the clone strategy, whose restore runs through deployResources and
     * with that through waitResourcesReady) instead of completing as soon as the satellites
     * confirm the re-activation update.
     */
    @Test
    public void rollbackZfsRollbackStrategyWaitsForResourcesReady() throws Exception
    {
        // second satellite so that the rolled-back resources have a DRBD peer whose
        // ready-state has to be awaited
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), false);

        enterScope();
        Node testNode2 = nodeFactory.create(testNode2Name, Node.Type.SATELLITE, null);
        testNode2.setPeer(mockSatellite2);
        nodesMap.put(testNode2Name, testNode2);
        leaveScope();

        createRscDfnWithVlmDfn(TEST_RSC_NAME);

        enterScope();
        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        for (Node node : Arrays.asList(testNode, testNode2))
        {
            StorPool storPool = storPoolFactory.create(
                node,
                storPoolDfn,
                DeviceProviderKind.ZFS_THIN,
                getFreeSpaceMgr(storPoolDfn, node),
                false
            );
            storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);
        }

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, TEST_SP_NAME);
        for (String nodeNameStr : Arrays.asList(TEST_NODE_NAME, TEST_NODE_2_NAME))
        {
            ctrlRscCrtApiHelper.createResourceDb(
                nodeNameStr,
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
        leaveScope();

        satelliteOnline();
        setSatelliteOnline(mockSatellite2, true);
        Mockito.when(mockPeer.isOnline()).thenReturn(true);

        // take the snapshot on both satellites
        ApiCallRc snapRc = collect(
            snapCrtApiCallHandlerProvider.get()
                .createSnapshot(Collections.emptyList(), TEST_RSC_NAME, TEST_SNAP_NAME, Collections.emptyMap())
        );
        assertThat(snapRc).noneMatch(entry -> entry.isError());
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(testSnapName).getAllSnapshots()).hasSize(2);

        simulateRollbackResponses(mockSatellite, testNodeName);
        simulateRollbackResponses(mockSatellite2, testNode2Name);

        // subscribe the rollback flux WITHOUT any resource state events - the ready-wait
        // has nothing to report yet, so the flux must stay incomplete
        List<ApiCallRc.RcEntry> entries = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(1);
        snapRollbackApiCallHandlerProvider.get()
            .rollbackSnapshot(TEST_RSC_NAME, TEST_SNAP_NAME, ApiConsts.VAL_STOR_POOL_ZFS_ROLLBACK_STRAT_ROLLBACK)
            .contextWrite(contextWrite())
            .subscribe(
                apiCallRc -> apiCallRc.forEach(entries::add),
                error::set,
                completed::countDown
            );

        // the rollback itself is done (satellites rolled back and confirmed the re-activation),
        // but the flux has to wait for the resources to become ready
        assertThat(completed.await(1, TimeUnit.SECONDS))
            .as("rollback flux must not complete before the resources are ready")
            .isFalse();
        assertThat(error.get()).isNull();

        // report the rolled-back resources as ready; covering all possible peer node ids keeps
        // the test independent of the node id allocation order
        Map<Integer, Boolean> allPeersConnected = new TreeMap<>();
        for (int nodeId = 0; nodeId < 8; nodeId++)
        {
            allPeersConnected.put(nodeId, true);
        }
        ResourceState readyState = new ResourceState(
            true,
            Collections.singletonMap(new VolumeNumber(0), allPeersConnected),
            false,
            true,
            null,
            null
        );
        for (NodeName nodeName : Arrays.asList(testNodeName, testNode2Name))
        {
            resourceStateEvent.get().triggerEvent(
                ObjectIdentifier.resource(nodeName, testRscName),
                readyState
            );
        }

        // now the ready-wait completes and the flux runs through
        assertThat(completed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();
        assertThat(entries)
            .extracting(entry -> entry.getMessage())
            .filteredOn(message -> message.endsWith("' ready"))
            .containsExactlyInAnyOrder(
                "Resource '" + TEST_RSC_NAME + "' on '" + TEST_NODE_NAME + "' ready",
                "Resource '" + TEST_RSC_NAME + "' on '" + TEST_NODE_2_NAME + "' ready"
            );
    }

    @Test
    public void rollbackZfsRollbackStrategyNotMostRecentRejected() throws Exception
    {
        createDeployedSnapshot(DeviceProviderKind.ZFS_THIN);
        // a newer snapshot exists, "zfs rollback" would have to destroy it
        createSnapDfn(TEST_RSC_NAME, "snap2", SnapshotDefinition.Flags.SUCCESSFUL);

        evaluateTest(
            new RollbackSnapshotCall(ApiConsts.FAIL_DEPENDEND_BACKUP)
                .setZfsStrategy(ApiConsts.VAL_STOR_POOL_ZFS_ROLLBACK_STRAT_ROLLBACK)
        );
    }

    @Test
    public void rollbackZfsRollbackStrategyVolumeWithoutSnapshotRejected() throws Exception
    {
        createDeployedSnapshot(DeviceProviderKind.ZFS_THIN);

        // add a volume that is not captured by the snapshot
        enterScope();
        ResourceDefinition rscDfn = rscDfnMap.get(testRscName);
        VolumeDefinition newVlmDfn = volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 1)
            .setSize(TEST_VLM_SIZE)
            .build();
        LayerPayload payload = new LayerPayload();
        payload.putStorageVlmPayload(RscLayerSuffixes.SUFFIX_DATA, 1, testStorPool);
        volumeFactory.create(
            rscDfn.getResource(testNodeName),
            newVlmDfn,
            null,
            payload,
            null,
            Collections.emptyMap(),
            null
        );
        leaveScope();

        evaluateTest(
            new RollbackSnapshotCall(ApiConsts.FAIL_NOT_FOUND_SNAPSHOT)
                .setZfsStrategy(ApiConsts.VAL_STOR_POOL_ZFS_ROLLBACK_STRAT_ROLLBACK)
        );
    }

    @Test
    public void rollbackWhileRollbackInProgressRejected() throws Exception
    {
        createDeployedSnapshot(DeviceProviderKind.ZFS_THIN);

        // simulate a rollback that is already in progress for this resource definition
        snapRollbackMgr.prepareFlux(
            rscDfnMap.get(testRscName),
            Collections.singleton(testNodeName)
        );

        evaluateTest(
            new RollbackSnapshotCall(
                // resource definition marked down for rollback
                ApiConsts.MODIFIED,
                // deactivated resource on satellite
                ApiConsts.MODIFIED,
                // volume definitions reset to snapshot state
                ApiConsts.MODIFIED,
                // second rollback rejected
                ApiConsts.FAIL_SNAPSHOT_ROLLBACK_IN_PROGRESS
            )
        );
    }

    @Test
    public void rollbackBackupRestoreRunningRejected() throws Exception
    {
        deployTestResource(DeviceProviderKind.LVM_THIN);
        createSnapDfn(TEST_RSC_NAME, TEST_SNAP_NAME, SnapshotDefinition.Flags.SUCCESSFUL);

        enterScope();
        backupInfoMgr.addAllRestoreEntries(
            rscDfnMap.get(testRscName),
            "dummy.meta",
            TEST_RSC_NAME,
            Collections.emptyList(),
            Collections.emptyMap(),
            new RemoteName("dummyremote")
        );
        leaveScope();

        evaluateTest(
            new RollbackSnapshotCall(ApiConsts.FAIL_IN_USE)
        );
    }

    /*
     * helpers
     */

    /**
     * Response sequence of a successful rollback via the clone strategy (safety-snapshot, truncate,
     * restore, recreate). Unless noted otherwise the entries carry the snapshot/modify context masks
     * of the rollback api call.
     */
    private long[] cloneRollbackRcs()
    {
        return new long[]
        {
            // safety snapshot: suspended IO
            ApiConsts.MODIFIED,
            // safety snapshot: took snapshot
            ApiConsts.MODIFIED,
            // safety snapshot: resumed IO
            ApiConsts.MODIFIED,
            // truncate: resource definition marked for deletion
            ApiConsts.DELETED,
            // truncate: resource marked for deletion on satellite
            ApiConsts.MODIFIED,
            // truncate: updated resync-after entries
            ApiConsts.MASK_INFO,
            // restore: resource restored from the snapshot (emitted with the restore's resource context)
            ApiConsts.CREATED | ApiConsts.MASK_RSC | ApiConsts.MASK_CRT,
            // restore: resource deployed on satellite
            ApiConsts.MODIFIED,
            // restore: updated resync-after entries
            ApiConsts.MASK_INFO,
            // safety snapshot marked for deletion
            ApiConsts.DELETED,
            // safety snapshot deleted on satellite
            ApiConsts.MODIFIED,
            // safety snapshot deleted on all nodes
            ApiConsts.DELETED,
            // safety snapshot definition deleted
            ApiConsts.DELETED,
            // make available: resource already deployed as requested
            ApiConsts.MASK_RSC | ApiConsts.MASK_CRT
        };
    }

    private void satelliteOnline()
    {
        setSatelliteOnline(mockSatellite, true);
    }

    /**
     * Simulates the satellite's NotifyRollbackDone answer: whenever the controller sends an update to
     * the mocked satellite, a background thread reports the rollback as successful. The report thread
     * blocks in {@link SnapshotRollbackManager#handle} until the rollback flux is subscribed, hence
     * a background thread instead of answering inline.
     */
    private void simulateRollbackResponses()
    {
        simulateRollbackResponses(mockSatellite, testNodeName);
    }

    private void simulateRollbackResponses(Peer peerMock, NodeName nodeName)
    {
        Mockito.when(peerMock.apiCall(anyString(), any())).thenAnswer(
            ignored ->
            {
                Thread notifier = new Thread(
                    () -> snapRollbackMgr.handle(nodeName, testRscName, true)
                );
                notifier.setDaemon(true);
                notifier.start();
                return Flux.empty();
            }
        );
    }

    private void createRscDfnWithVlmDfn(String rscNameStr) throws Exception
    {
        enterScope();

        rscDfnMap.put(
            new ResourceName(rscNameStr),
            resourceDefinitionTestFactory.builder(rscNameStr)
                .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
                .build()
        );
        volumeDefinitionTestFactory.builder(rscNameStr, 0)
            .setSize(TEST_VLM_SIZE)
            .build();

        leaveScope();
    }

    private void deployTestResource(DeviceProviderKind providerKind) throws Exception
    {
        createRscDfnWithVlmDfn(TEST_RSC_NAME);

        enterScope();

        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        testStorPool = storPoolFactory.create(
            testNode,
            storPoolDfn,
            providerKind,
            getFreeSpaceMgr(storPoolDfn, testNode),
            false
        );
        testStorPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

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

    private SnapshotDefinition createSnapDfn(
        String rscNameStr,
        String snapNameStr,
        SnapshotDefinition.Flags... flags
    )
        throws Exception
    {
        enterScope();

        SnapshotDefinition snapDfn = snapshotDefinitionFactory.create(
            rscDfnMap.get(new ResourceName(rscNameStr)),
            new SnapshotName(snapNameStr),
            flags
        );

        leaveScope();

        return snapDfn;
    }

    /**
     * Creates a second node, a shared storage pool on both nodes and a STORAGE-only
     * {@link #TEST_RSC_NAME} resource on both: the copy on the first node stays active, the one on
     * the second node is flagged INACTIVE. Both satellites are online afterwards.
     */
    private void deploySharedResource() throws Exception
    {
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), false);

        enterScope();

        Node testNode2 = nodeFactory.create(testNode2Name, Node.Type.SATELLITE, null);
        testNode2.setPeer(mockSatellite2);
        nodesMap.put(testNode2Name, testNode2);

        StorPoolName sharedSpName = new StorPoolName(SHARED_SP_NAME);
        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(sharedSpName);
        storPoolDfnMap.put(sharedSpName, storPoolDfn);
        FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(new SharedStorPoolName(SHARED_SPACE_NAME));
        for (Node node : Arrays.asList(testNode, testNode2))
        {
            StorPool storPool = storPoolFactory.create(
                node,
                storPoolDfn,
                DeviceProviderKind.LVM,
                fsm,
                false
            );
            storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);
        }

        rscDfnMap.put(
            testRscName,
            resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
                .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
                .build()
        );
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 0)
            .setSize(TEST_VLM_SIZE)
            .build();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, SHARED_SP_NAME);
        for (String nodeNameStr : Arrays.asList(TEST_NODE_NAME, TEST_NODE_2_NAME))
        {
            ctrlRscCrtApiHelper.createResourceDb(
                nodeNameStr,
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
        // the copy on the second node is just a registered, unused copy of the shared data
        rscDfnMap.get(testRscName).getResource(testNode2Name)
            .getStateFlags().enableFlags(Resource.Flags.INACTIVE);

        leaveScope();

        satelliteOnline();
        setSatelliteOnline(mockSatellite2, true);
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
    }

    /**
     * Takes {@link #TEST_SNAP_NAME} of the shared resource deployed by {@link #deploySharedResource()}
     * and asserts that the snapshot objects were registered on both copies.
     */
    private void createSharedSnapshot() throws Exception
    {
        ApiCallRc snapRc = collect(
            snapCrtApiCallHandlerProvider.get()
                .createSnapshot(Collections.emptyList(), TEST_RSC_NAME, TEST_SNAP_NAME, Collections.emptyMap())
        );
        assertThat(snapRc).noneMatch(entry -> entry.isError());

        SnapshotDefinition snapDfn = rscDfnMap.get(testRscName).getSnapshotDfn(testSnapName);
        assertThat(snapDfn.getFlags().isSet(SnapshotDefinition.Flags.SUCCESSFUL)).isTrue();
        assertThat(snapDfn.getAllSnapshots()).hasSize(2);
        assertThat(snapDfn.getSnapshot(testNodeName)).isNotNull();
        assertThat(snapDfn.getSnapshot(testNode2Name)).isNotNull();
    }

    private void createDeployedSnapshot(DeviceProviderKind providerKind) throws Exception
    {
        deployTestResource(providerKind);
        satelliteOnline();
        Mockito.when(mockPeer.isOnline()).thenReturn(true);

        // the mocked satellite reports no DRBD states, which counts as UpToDate, and immediately
        // "responds" to all updates, so the snapshot create flux runs through all of its stages
        evaluateTest(
            new CreateSnapshotCall(
                // snapshot registered
                ApiConsts.CREATED,
                // suspended IO
                ApiConsts.MODIFIED,
                // took snapshot
                ApiConsts.MODIFIED,
                // resumed IO
                ApiConsts.MODIFIED
            )
        );
    }

    private class CreateSnapshotCall extends AbsApiCallTester
    {
        CreateSnapshotCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_SNAPSHOT,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return collect(
                snapCrtApiCallHandlerProvider.get().createSnapshot(
                    Collections.emptyList(),
                    TEST_RSC_NAME,
                    TEST_SNAP_NAME,
                    Collections.emptyMap()
                )
            );
        }
    }

    private class RollbackSnapshotCall extends AbsApiCallTester
    {
        private String rscName;
        private String snapName;
        private String zfsStrategy;

        RollbackSnapshotCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_SNAPSHOT,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            rscName = TEST_RSC_NAME;
            snapName = TEST_SNAP_NAME;
            zfsStrategy = null;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return collect(
                snapRollbackApiCallHandlerProvider.get().rollbackSnapshot(
                    rscName,
                    snapName,
                    zfsStrategy
                )
            );
        }

        public RollbackSnapshotCall setZfsStrategy(String zfsStrategyRef)
        {
            zfsStrategy = zfsStrategyRef;
            return this;
        }
    }
}
