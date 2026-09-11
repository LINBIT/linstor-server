package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRestoreApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.event.EventSerializerDescriptor;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchStore;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
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
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("checkstyle:magicnumber")
public class SnapshotRestoreApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_NODE_2_NAME = "TestSatellite2";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_TARGET_RSC_NAME = "TargetRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_SNAP_NAME = "snap1";
    private static final String AUX_KEY = ApiConsts.NAMESPC_AUXILIARY + "/test";
    private static final long TEST_VLM_SIZE = 100 * 1024L;

    @Inject
    private Provider<CtrlSnapshotCrtApiCallHandler> snapCrtApiCallHandlerProvider;
    @Inject
    private Provider<CtrlSnapshotRestoreApiCallHandler> snapRestoreApiCallHandlerProvider;
    @Inject
    private Provider<CtrlApiCallHandler> ctrlApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    @Inject
    private BackupInfoManager backupInfoMgr;
    @Inject
    private ResourceStateEvent resourceStateEvent;

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
    protected Peer mockSatellite;

    @Mock
    protected Peer mockSatellite2;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeName;
    private final ResourceName testRscName;
    private final ResourceName testTargetRscName;
    private final SnapshotName testSnapName;
    private final StorPoolName testStorPoolName;

    private Node testNode;

    private final AtomicInteger minorNrGenerator = new AtomicInteger(1000);

    public SnapshotRestoreApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testTargetRscName = new ResourceName(TEST_TARGET_RSC_NAME);
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
     * restore volume definition tests (synchronous api)
     */

    @Test
    public void restoreVlmDfnSuccess() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);

        evaluateTest(
            // the handler reports the successful restore with a DELETED return code
            // (arguably it should be CREATED, the message says "restored")
            new RestoreVlmDfnCall(ApiConsts.DELETED)
        );

        ResourceDefinition targetRscDfn = rscDfnMap.get(testTargetRscName);
        assertThat(targetRscDfn.getVolumeDfnCount()).isEqualTo(1);
        VolumeDefinition vlmDfn = targetRscDfn.getVolumeDfn(new VolumeNumber(0));
        assertThat(vlmDfn).isNotNull();
        assertThat(vlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
    }

    @Test
    public void restoreVlmDfnIntoDeployedRscDfn() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        deployResource(TEST_TARGET_RSC_NAME);

        evaluateTest(
            new RestoreVlmDfnCall(ApiConsts.DELETED)
        );

        ResourceDefinition targetRscDfn = rscDfnMap.get(testTargetRscName);
        assertThat(targetRscDfn.getVolumeDfnCount()).isEqualTo(1);
        // the volume was also created on the already deployed resource
        Resource targetRsc = targetRscDfn.getResource(testNodeName);
        assertThat(targetRsc).isNotNull();
        assertThat(targetRsc.getVolume(new VolumeNumber(0))).isNotNull();
    }

    @Test
    public void restoreVlmDfnUnknownFromRscDfn() throws Exception
    {
        createRscDfn(TEST_TARGET_RSC_NAME);

        evaluateTest(
            new RestoreVlmDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setFromRscName("UnknownRsc")
        );
    }

    @Test
    public void restoreVlmDfnUnknownSnapshot() throws Exception
    {
        createRscDfnWithVlmDfn(TEST_RSC_NAME);
        createRscDfn(TEST_TARGET_RSC_NAME);

        evaluateTest(
            new RestoreVlmDfnCall(ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN)
        );
    }

    @Test
    public void restoreVlmDfnUnknownTargetRscDfn() throws Exception
    {
        createDeployedSnapshot();

        evaluateTest(
            new RestoreVlmDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setToRscName("UnknownRsc")
        );
    }

    @Test
    public void restoreVlmDfnConflictingVlmNr() throws Exception
    {
        createDeployedSnapshot();
        // the target already has a volume definition with the same volume number as the snapshot
        createRscDfnWithVlmDfn(TEST_TARGET_RSC_NAME);

        evaluateTest(
            new RestoreVlmDfnCall(ApiConsts.FAIL_EXISTS_VLM_DFN)
        );
    }

    @Test
    public void restoreVlmDfnBackupRestoreRunningRejected() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        markBackupRestoreRunning(testTargetRscName);

        evaluateTest(
            new RestoreVlmDfnCall(ApiConsts.FAIL_IN_USE)
        );
    }

    /*
     * restore snapshot tests (flux based)
     */

    @Test
    public void restoreSnapshotSuccess() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);

        evaluateTest(
            new RestoreSnapshotCall(
                // resource restored
                ApiConsts.CREATED,
                // resource deployed on satellite (emitted with the snapshot context of the restore)
                ApiConsts.MODIFIED | ApiConsts.MASK_SNAPSHOT | ApiConsts.MASK_CRT,
                // updated resync-after entries
                ApiConsts.MASK_INFO | ApiConsts.MASK_SNAPSHOT | ApiConsts.MASK_CRT
            )
        );

        ResourceDefinition targetRscDfn = rscDfnMap.get(testTargetRscName);
        Resource targetRsc = targetRscDfn.getResource(testNodeName);
        assertThat(targetRsc).isNotNull();
        assertThat(targetRsc.getStateFlags().isSet(Resource.Flags.RESTORE_FROM_SNAPSHOT)).isTrue();

        // the rscDfn props of the snapshot were copied to the target rscDfn
        assertThat(targetRscDfn.getProps().getProp(AUX_KEY)).isEqualTo("value");

        VolumeDefinition targetVlmDfn = targetRscDfn.getVolumeDfn(new VolumeNumber(0));
        // regular snapshots are restored with healthy DRBD meta-data
        assertThat(targetVlmDfn.getFlags().isSet(VolumeDefinition.Flags.DRBD_INITIALIZED)).isTrue();
        assertThat(targetVlmDfn.getProps().getProp(InternalApiConsts.KEY_LINSTOR_DRBD_INITIAL_UPTODATE_ON))
            .isEqualTo(TEST_NODE_NAME.toUpperCase());

        // the restore-source properties were cleaned up after the deployment
        Volume targetVlm = targetRsc.getVolume(new VolumeNumber(0));
        assertThat(targetVlm).isNotNull();
        assertThat(targetVlm.getProps().getProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE)).isNull();
        assertThat(targetVlm.getProps().getProp(ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT)).isNull();
    }

    @Test
    public void restoreSnapshotOnExplicitNode() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);

        evaluateTest(
            new RestoreSnapshotCall(
                // resource restored
                ApiConsts.CREATED,
                // resource deployed on satellite (emitted with the snapshot context of the restore)
                ApiConsts.MODIFIED | ApiConsts.MASK_SNAPSHOT | ApiConsts.MASK_CRT,
                // updated resync-after entries
                ApiConsts.MASK_INFO | ApiConsts.MASK_SNAPSHOT | ApiConsts.MASK_CRT
            )
                .setNodeNames(Collections.singletonList(TEST_NODE_NAME))
        );

        assertThat(rscDfnMap.get(testTargetRscName).getResource(testNodeName)).isNotNull();
    }

    /**
     * Restoring onto more than one node must run through (and actually wait for) the
     * "waitResourcesReady" stage of the deploy flux. The restore flux is subscribed without any
     * resource state events: it must NOT complete on its own. Only after the events report the
     * DRBD resources as ready may the flux emit the per-satellite "Resource ... ready" entries
     * and run its remaining stages (auto helper, balancing, property cleanup) to completion.
     */
    @Test
    public void restoreSnapshotWaitsForResourcesReady() throws Exception
    {
        // second satellite so that the restored resources have a DRBD peer to wait for
        // (a resource definition with a single resource skips the ready-wait)
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), false);
        NodeName testNode2Name = new NodeName(TEST_NODE_2_NAME);

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
                DeviceProviderKind.LVM_THIN,
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

        // take the snapshot on both satellites
        ApiCallRc snapRc = collect(
            snapCrtApiCallHandlerProvider.get()
                .createSnapshot(Collections.emptyList(), TEST_RSC_NAME, TEST_SNAP_NAME, Collections.emptyMap())
        );
        assertThat(snapRc).noneMatch(entry -> entry.isError());
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(testSnapName).getAllSnapshots()).hasSize(2);

        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);

        // subscribe the restore flux WITHOUT any resource state events - the ready-wait stage
        // has nothing to report yet, so the flux must stay incomplete
        List<ApiCallRc.RcEntry> entries = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(1);
        snapRestoreApiCallHandlerProvider.get()
            .restoreSnapshot(
                Collections.emptyList(),
                TEST_RSC_NAME,
                TEST_SNAP_NAME,
                TEST_TARGET_RSC_NAME,
                Collections.emptyMap()
            )
            .contextWrite(contextWrite())
            .subscribe(
                apiCallRc -> apiCallRc.forEach(entries::add),
                error::set,
                completed::countDown
            );

        // the restore itself is done (resources restored + deployed to the satellites), but the
        // flux waits for the resources to become ready
        assertThat(completed.await(1, TimeUnit.SECONDS)).isFalse();
        assertThat(error.get()).isNull();
        assertThat(entries).noneMatch(entry -> entry.getMessage().endsWith("' ready"));

        // report the restored resources as ready; covering all possible peer node ids keeps the
        // test independent of the node id allocation order
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
                ObjectIdentifier.resource(nodeName, testTargetRscName),
                readyState
            );
        }

        // now the ready-wait stage completes and the remaining stages run through
        assertThat(completed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();
        assertThat(entries)
            .extracting(entry -> entry.getMessage())
            .filteredOn(message -> message.endsWith("' ready"))
            .containsExactlyInAnyOrder(
                "Resource '" + TEST_TARGET_RSC_NAME + "' on '" + TEST_NODE_NAME + "' ready",
                "Resource '" + TEST_TARGET_RSC_NAME + "' on '" + TEST_NODE_2_NAME + "' ready"
            );
    }

    @Test
    public void restoreSnapshotUnknownNode() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);

        evaluateTest(
            new RestoreSnapshotCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeNames(Collections.singletonList("UnknownNode"))
        );
    }

    @Test
    public void restoreSnapshotVlmSizeMismatch() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        // volume definition sizes must match the snapshot volume sizes exactly
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE * 2);

        evaluateTest(
            new RestoreSnapshotCall(ApiConsts.FAIL_INVLD_VLM_SIZE)
        );
    }

    @Test
    public void restoreSnapshotMissingSnapshotVlmDfn() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);
        // the snapshot has no volume 1, so the restore cannot fill this volume definition
        createVlmDfn(TEST_TARGET_RSC_NAME, 1, TEST_VLM_SIZE);

        evaluateTest(
            new RestoreSnapshotCall(ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN)
        );
    }

    @Test
    public void restoreSnapshotWithoutVolumeDefinitions() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);

        evaluateTest(
            new RestoreSnapshotCall(
                // no volume definitions in the target resource definition
                ApiConsts.WARN_NOT_FOUND,
                // resource restored (without volumes)
                ApiConsts.CREATED,
                // resource deployed on satellite (emitted with the snapshot context of the restore)
                ApiConsts.MODIFIED | ApiConsts.MASK_SNAPSHOT | ApiConsts.MASK_CRT,
                // no volumes have been defined for the restored resource
                ApiConsts.WARN_NOT_FOUND,
                // updated resync-after entries
                ApiConsts.MASK_INFO | ApiConsts.MASK_SNAPSHOT | ApiConsts.MASK_CRT
            )
        );

        Resource targetRsc = rscDfnMap.get(testTargetRscName).getResource(testNodeName);
        assertThat(targetRsc).isNotNull();
        assertThat(targetRsc.iterateVolumes().hasNext()).isFalse();
    }

    @Test
    public void restoreSnapshotShippingInProgressRejected() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);

        // mark the snapshot definition as an in-progress backup shipping target
        enterScope();
        rscDfnMap.get(testRscName).getSnapshotDfn(testSnapName).getSnapDfnProps().setProp(
            BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC + "/" + InternalApiConsts.KEY_SHIPPING_STATUS,
            InternalApiConsts.VALUE_SHIPPING
        );
        leaveScope();

        evaluateTest(
            new RestoreSnapshotCall(ApiConsts.FAIL_EXISTS_SNAPSHOT_SHIPPING)
        );
    }

    @Test
    public void restoreSnapshotBackupRestoreRunningRejected() throws Exception
    {
        createDeployedSnapshot();
        createRscDfn(TEST_TARGET_RSC_NAME);
        createVlmDfn(TEST_TARGET_RSC_NAME, 0, TEST_VLM_SIZE);
        markBackupRestoreRunning(testTargetRscName);

        evaluateTest(
            new RestoreSnapshotCall(ApiConsts.FAIL_IN_USE)
        );
    }

    /*
     * helpers
     */

    private void satelliteOnline()
    {
        setSatelliteOnline(mockSatellite, true);
    }

    private void markBackupRestoreRunning(ResourceName rscName) throws Exception
    {
        enterScope();

        backupInfoMgr.addAllRestoreEntries(
            rscDfnMap.get(rscName),
            "dummy.meta",
            rscName.displayValue,
            Collections.emptyList(),
            Collections.emptyMap(),
            new RemoteName("dummyremote")
        );

        leaveScope();
    }

    @Test
    public void restoreSnapshotPicksExecutorPerSharedSpace() throws Exception
    {
        // two copies of the resource on two DIFFERENT shared spaces: their snapshots refer to
        // different shared data, so a restore without given nodes has to pick one restore source
        // per shared space instead of a single one across all shared snapshots
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), false);
        NodeName testNode2Name = new NodeName(TEST_NODE_2_NAME);

        enterScope();
        Node testNode2 = nodeFactory.create(testNode2Name, Node.Type.SATELLITE, null);
        testNode2.setPeer(mockSatellite2);
        nodesMap.put(testNode2Name, testNode2);

        rscDfnMap.put(
            testRscName,
            resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
                .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
                .build()
        );
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 0)
            .setSize(TEST_VLM_SIZE)
            .build();

        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        int sharedSpaceNr = 0;
        for (Node node : Arrays.asList(testNode, testNode2))
        {
            StorPool storPool = storPoolFactory.create(
                node,
                storPoolDfn,
                DeviceProviderKind.LVM,
                freeSpaceMgrFactory.getInstance(new SharedStorPoolName("SharedSpace" + sharedSpaceNr)),
                false
            );
            storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);
            sharedSpaceNr++;
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

        // both copies are the active one of their own shared space, so both take the snapshot
        ApiCallRc snapRc = collect(
            snapCrtApiCallHandlerProvider.get()
                .createSnapshot(Collections.emptyList(), TEST_RSC_NAME, TEST_SNAP_NAME, Collections.emptyMap())
        );
        assertThat(snapRc).noneMatch(entry -> entry.isError());
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(testSnapName).getAllSnapshots()).hasSize(2);

        enterScope();
        rscDfnMap.put(
            testTargetRscName,
            resourceDefinitionTestFactory.builder(TEST_TARGET_RSC_NAME)
                .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
                .build()
        );
        volumeDefinitionTestFactory.builder(TEST_TARGET_RSC_NAME, 0)
            .setSize(TEST_VLM_SIZE)
            .build();
        leaveScope();

        ApiCallRc restoreRc = collect(
            snapRestoreApiCallHandlerProvider.get().restoreSnapshot(
                Collections.emptyList(),
                TEST_RSC_NAME,
                TEST_SNAP_NAME,
                TEST_TARGET_RSC_NAME,
                Collections.emptyMap()
            )
        );
        assertThat(restoreRc).noneMatch(entry -> entry.isError());

        // one restore source per shared space: both nodes end up with a restored resource
        ResourceDefinition targetRscDfn = rscDfnMap.get(testTargetRscName);
        assertThat(targetRscDfn.getResource(testNodeName)).isNotNull();
        assertThat(targetRscDfn.getResource(testNode2Name)).isNotNull();
    }

    private void createRscDfn(String rscNameStr) throws Exception
    {
        enterScope();

        rscDfnMap.put(
            new ResourceName(rscNameStr),
            resourceDefinitionTestFactory.builder(rscNameStr)
                .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
                .build()
        );

        leaveScope();
    }

    private void createVlmDfn(String rscNameStr, int vlmNr, long size) throws Exception
    {
        enterScope();

        volumeDefinitionTestFactory.builder(rscNameStr, vlmNr)
            .setSize(size)
            .build();

        leaveScope();
    }

    private void createRscDfnWithVlmDfn(String rscNameStr) throws Exception
    {
        createRscDfn(rscNameStr);
        createVlmDfn(rscNameStr, 0, TEST_VLM_SIZE);
    }

    private void deployResource(String rscNameStr) throws Exception
    {
        enterScope();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, TEST_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            TEST_NODE_NAME,
            rscNameStr,
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

    private void deployTestResource() throws Exception
    {
        createRscDfnWithVlmDfn(TEST_RSC_NAME);

        enterScope();

        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        StorPool storPool = storPoolFactory.create(
            testNode,
            storPoolDfn,
            DeviceProviderKind.LVM_THIN,
            getFreeSpaceMgr(storPoolDfn, testNode),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        // an auxiliary property that must be captured by the snapshot and restored to the target
        rscDfnMap.get(testRscName).getProps().setProp(AUX_KEY, "value");

        leaveScope();

        deployResource(TEST_RSC_NAME);
    }

    private void createDeployedSnapshot() throws Exception
    {
        deployTestResource();
        satelliteOnline();

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

    private class RestoreSnapshotCall extends AbsApiCallTester
    {
        private List<String> nodeNames;
        private String fromRscName;
        private String snapName;
        private String toRscName;

        RestoreSnapshotCall(long... expectedRcs)
        {
            super(
                // exceptions of the restore handler are converted with a resource ("restoring") context
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            nodeNames = Collections.emptyList();
            fromRscName = TEST_RSC_NAME;
            snapName = TEST_SNAP_NAME;
            toRscName = TEST_TARGET_RSC_NAME;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return collect(
                snapRestoreApiCallHandlerProvider.get().restoreSnapshot(
                    nodeNames,
                    fromRscName,
                    snapName,
                    toRscName,
                    Collections.emptyMap()
                )
            );
        }

        public RestoreSnapshotCall setNodeNames(List<String> nodeNamesRef)
        {
            nodeNames = nodeNamesRef;
            return this;
        }
    }

    private class RestoreVlmDfnCall extends AbsApiCallTester
    {
        private String fromRscName;
        private String snapName;
        private String toRscName;

        RestoreVlmDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_SNAPSHOT,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            fromRscName = TEST_RSC_NAME;
            snapName = TEST_SNAP_NAME;
            toRscName = TEST_TARGET_RSC_NAME;
        }

        @Override
        public ApiCallRc executeApiCall() throws Exception
        {
            // synchronous api call, needs an explicitly entered scope
            enterScope();
            ApiCallRc apiCallRc = ctrlApiCallHandlerProvider.get().restoreVlmDfn(
                fromRscName,
                snapName,
                toRscName
            );
            leaveScope();
            return apiCallRc;
        }

        public RestoreVlmDfnCall setFromRscName(String fromRscNameRef)
        {
            fromRscName = fromRscNameRef;
            return this;
        }

        public RestoreVlmDfnCall setToRscName(String toRscNameRef)
        {
            toRscName = toRscNameRef;
            return this;
        }
    }
}
