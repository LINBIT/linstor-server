package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscMakeAvailableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("checkstyle:magicnumber")
public class RscMakeAvailableApiTest extends ApiTestBase
{
    private static final String PROP_KEY_TWO_PRIMARIES = "allow-two-primaries";
    private static final String PROP_KEY_PROTOCOL = "protocol";
    private static final String SHARED_RSC_NAME = "SharedRsc";
    private static final String SHARED_SP_NAME = "SharedPool";
    private static final String SHARED_SPACE_NAME = "SharedSpace";

    @Inject
    private Provider<CtrlRscMakeAvailableApiCallHandler> rscMakeAvailableApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    @Inject
    private SnapshotDefinitionControllerFactory snapshotDefinitionFactory;
    @Inject
    private SnapshotControllerFactory snapshotFactory;

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
    private SatelliteState satelliteState2;

    public RscMakeAvailableApiTest() throws Exception
    {
        testNodeName = new NodeName("TestSatellite");
        testNode2Name = new NodeName("TestSatellite2");
        testRscName = new ResourceName("TestRsc");
        testStorPoolName = new StorPoolName("TestStorPool");
        testDisklessStorPoolName = new StorPoolName("TestDisklessPool");
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenReturn(Mono.just(Collections.emptyMap()));

        satelliteState = new SatelliteState();
        satelliteState2 = new SatelliteState();
        stubSatellitePeer(mockSatellite, mockExtToolsMgr, satelliteState, true);
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, satelliteState2, true);
        stubAllExtToolsSupported(mockExtToolsMgr);

        testSatelliteNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testSatelliteNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testSatelliteNode);

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = "NotTellingYou";
        drbdRscDfn.transportType = TransportType.IP;
        testRscDfn = resourceDefinitionFactory.create(
            testRscName,
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup()
        );
        rscDfnMap.put(testRscName, testRscDfn);

        ctrlConf.setProp(
            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
            randomUUID().toString(),
            ApiConsts.NAMESPC_CLUSTER
        );

        commitAndCleanUp(true);
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void makeAvailableUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void makeAvailableUnknownNode() throws Exception
    {
        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void makeAvailableAlreadyDeployed() throws Exception
    {
        addStorPool();
        createResourceOnNode();

        evaluateTest(
            new MakeAvailableCall(
                // "Resource already deployed as requested"
                ApiConsts.MASK_SUCCESS
            )
        );

        assertThat(testSatelliteNode.getResource(testRscName)).isNotNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(1);
    }

    @Test
    public void makeAvailableLayerStackMismatch() throws Exception
    {
        addStorPool();
        createResourceOnNode();

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_INVLD_LAYER_STACK)
                .setLayerStack("storage")
        );
    }

    @Test
    public void makeAvailableNoStorPoolFound() throws Exception
    {
        // no storage pool exists on the target node, so the autoplacer cannot place the resource
        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_NOT_FOUND_STOR_POOL)
        );

        assertThat(testRscDfn.getResourceCount()).isEqualTo(0);
    }

    @Test
    public void makeAvailableDeployNewResource() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        addStorPool();

        evaluateTest(
            new MakeAvailableCall(
                // StorPoolName property set on the new resource
                ApiConsts.CREATED,
                // Registered
                ApiConsts.CREATED,
                // Deployed
                ApiConsts.MODIFIED,
                // No volumes => WARN_NOT_FOUND response
                ApiConsts.WARN_NOT_FOUND,
                // updated resync-after entries
                ApiConsts.MASK_INFO
            )
        );

        assertThat(testSatelliteNode.getResource(testRscName)).isNotNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(1);
    }

    @Test
    public void makeAvailableRevertsDeleteFlags() throws Exception
    {
        addStorPool();
        createResourceOnNode();

        enterScope();
        Resource rsc = testSatelliteNode.getResource(testRscName);
        rsc.getStateFlags().enableFlags(Resource.Flags.DELETE);
        commitAndCleanUp(true);

        evaluateTest(
            new MakeAvailableCall(
                // "Resource already deployed as requested"
                ApiConsts.MASK_SUCCESS
            )
        );

        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
    }

    /*
     * shared storage pool tests: STORAGE-only layer stack, a single diskful copy in a shared storage
     * pool (mirrors the CloudStack setup)
     */

    @Test
    public void makeAvailableActivatesInactiveSharedStorPoolRsc() throws Exception
    {
        // resource deactivated - make-available on the same node has to reactivate it
        Resource rsc = createInactiveSharedStorPoolRsc();

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME),
            false
        );

        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
    }

    @Test
    public void makeAvailableDualPrimaryActivatesInactiveSharedStorPoolRsc() throws Exception
    {
        // the resource is not in use (active) anywhere, so there is no live migration to prepare and
        // the resource is simply made available, i.e. reactivated - clients that cannot tell a
        // live-migration attach from a plain attach always set auto_manage_dual_primary
        Resource rsc = createInactiveSharedStorPoolRsc();

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setAutoManageDualPrimary(true),
            false
        );

        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
    }

    @Test
    public void makeAvailableCreatesSharedStorPoolRscWhenNoActiveCopy() throws Exception
    {
        // the single copy on the first node is INACTIVE - make-available on the second node has to
        // create the resource there reusing the shared data, ending with a usable (active) resource
        Resource rsc = createInactiveSharedStorPoolRsc();

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void makeAvailableDualPrimaryCreatesSharedStorPoolRscWhenNoActiveCopy() throws Exception
    {
        // same as above but with auto_manage_dual_primary: nothing is in use, so this is a plain
        // attach on the second node
        Resource rsc = createInactiveSharedStorPoolRsc();

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void makeAvailableSharedStorPoolRscIgnoresFreeSpace() throws Exception
    {
        // the shared LV already exists, so creating another leg for it does not allocate any new
        // space: a full shared storage pool must not fail the free-space check
        Resource rsc = createInactiveSharedStorPoolRsc();

        enterScope();
        nodesMap.get(testNode2Name)
            .getStorPool(new StorPoolName(SHARED_SP_NAME))
            .getFreeSpaceTracker()
            .setCapacityInfo(0, 10_000_000);
        commitAndCleanUp(true);

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void makeAvailableRefusesSharedRscCreationWhenSnapshotMissingOnNode() throws Exception
    {
        // moving the active copy of a shared resource to a node that does not hold its snapshots is
        // allowed: the snapshot data lives once on the shared pool, so the new copy receives the
        // per-node snapshot objects and can manage the snapshots' shared backing data
        Resource rsc = createInactiveSharedStorPoolRsc();
        SnapshotDefinition snapDfn = createSnapshotOnNode(rsc, "snap1");

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
        assertThat(snapDfn.getSnapshot(testNode2Name)).isNotNull();
    }

    @Test
    public void makeAvailableDualPrimarySharedRscCreationWithSnapshotObjects() throws Exception
    {
        // same as above: nothing is in use, so auto_manage_dual_primary degrades to a plain attach;
        // the new copy receives the snapshot objects as well
        Resource rsc = createInactiveSharedStorPoolRsc();
        SnapshotDefinition snapDfn = createSnapshotOnNode(rsc, "snap1");

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(snapDfn.getSnapshot(testNode2Name)).isNotNull();
    }

    @Test
    public void makeAvailableActivationSelfHealsSnapshotObjects() throws Exception
    {
        // both copies exist and are INACTIVE, but only the first node holds the snapshot objects
        // (legacy state): reactivating on the second node creates the missing objects there
        Resource rsc = createInactiveSharedStorPoolRsc();
        Resource rsc2 = createSharedStorPoolRscOnNode(testNode2Name, true);
        SnapshotDefinition snapDfn = createSnapshotOnNode(rsc, "snap1");

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue),
            false
        );

        assertThat(rsc2.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
        assertThat(snapDfn.getSnapshot(testNode2Name)).isNotNull();
    }

    @Test
    public void makeAvailableActivatesInactiveSharedStorPoolRscWithSnapshotsOnSameNode() throws Exception
    {
        // reactivating on the node that holds the snapshots stays allowed
        Resource rsc = createInactiveSharedStorPoolRsc();
        createSnapshotOnNode(rsc, "snap1");

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME),
            false
        );

        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
    }

    @Test
    public void createResourceDbCreatesSnapshotObjectsForSharedRsc() throws Exception
    {
        // the plain resource-create path also propagates the snapshot objects to the new copy of the
        // shared storage pool: the snapshot data lives once on the shared pool, so every copy holds it
        Resource rsc = createInactiveSharedStorPoolRsc();
        SnapshotDefinition snapDfn = createSnapshotOnNode(rsc, "snap1");

        enterScope();
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, SHARED_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            testNode2Name.displayValue,
            SHARED_RSC_NAME,
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

        assertThat(nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME))).isNotNull();
        assertThat(snapDfn.getSnapshot(testNode2Name)).isNotNull();
    }

    @Test
    public void createResourceDbDoesNotPropagateForeignSnapshots() throws Exception
    {
        // mixed setup: the existing copy and its snapshot live on a standalone (non-shared) storage
        // pool of the first node. A new copy on a shared pool of the second node has no access to
        // that snapshot's data, so it must not receive the snapshot objects
        Node node2 = createSecondNode();
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(
            node2,
            new StorPoolName(SHARED_SP_NAME),
            DeviceProviderKind.LVM,
            new SharedStorPoolName(SHARED_SPACE_NAME)
        );

        enterScope();
        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder(SHARED_RSC_NAME)
            .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);
        volumeDefinitionTestFactory.builder(SHARED_RSC_NAME, 0)
            .setSize(100 * 1024L)
            .build();
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, testStorPoolName.displayValue);
        ctrlRscCrtApiHelper.createResourceDb(
            testNodeName.displayValue,
            SHARED_RSC_NAME,
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

        Resource standaloneRsc = testSatelliteNode.getResource(new ResourceName(SHARED_RSC_NAME));
        SnapshotDefinition snapDfn = createSnapshotOnNode(standaloneRsc, "snap1");

        enterScope();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, SHARED_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            testNode2Name.displayValue,
            SHARED_RSC_NAME,
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

        assertThat(nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME))).isNotNull();
        // the snapshot's data is not on the shared space, so no objects were propagated
        assertThat(snapDfn.getSnapshot(testNode2Name)).isNull();
    }

    @Test
    public void makeAvailableDualPrimarySharedRefusedWhileResizing() throws Exception
    {
        // counterpart of the resize guard (a resize is refused while the resource is active on two
        // nodes): the dual-active window must also not open while a volume is still being resized
        Resource rsc = createInactiveSharedStorPoolRsc();

        enterScope();
        rsc.getStateFlags().disableFlags(Resource.Flags.INACTIVE);
        rscDfnMap.get(new ResourceName(SHARED_RSC_NAME))
            .getVolumeDfn(new VolumeNumber(0))
            .getFlags()
            .enableFlags(VolumeDefinition.Flags.RESIZE);
        commitAndCleanUp(true);

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_IN_USE)
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true)
        );

        assertThat(nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME))).isNull();
    }

    @Test
    public void makeAvailableDualPrimarySharedRefusedWithSnapshots() throws Exception
    {
        // opening the dual-active window is refused while the migration source still has snapshots
        Resource rsc = createInactiveSharedStorPoolRsc();
        createSnapshotOnNode(rsc, "snap1");

        enterScope();
        rsc.getStateFlags().disableFlags(Resource.Flags.INACTIVE);
        commitAndCleanUp(true);

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_EXISTS_SNAPSHOT)
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true)
        );

        assertThat(nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME))).isNull();
    }

    @Test
    public void makeAvailableDualPrimarySharedRefusedWithSnapshotsOnOtherNodeOnly() throws Exception
    {
        // the snapshot data lives once on the shared pool, so the dual-active window stays refused
        // even if the migration source is missing the per-node snapshot objects (legacy state, see
        // makeAvailableActivationSelfHealsSnapshotObjects)
        Resource rsc = createInactiveSharedStorPoolRsc();
        Resource rsc2 = createSharedStorPoolRscOnNode(testNode2Name, false);
        createSnapshotOnNode(rsc, "snap1");

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_EXISTS_SNAPSHOT)
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNodeName.displayValue)
                .setAutoManageDualPrimary(true)
        );

        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
        assertThat(rsc2.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
    }

    @Test
    public void makeAvailableDualPrimarySharedRefusedWhileCloning() throws Exception
    {
        // an in-progress clone reads from a snapshot of the shared data, so the source satellite must
        // keep its copy exclusively locked: the dual-active window must not open until the clone is
        // finished
        Resource rsc = createInactiveSharedStorPoolRsc();

        enterScope();
        rsc.getStateFlags().disableFlags(Resource.Flags.INACTIVE);
        rsc.getProps().setProp(InternalApiConsts.CLONE_PROP_PREFIX + "clonedRsc", "clonedRsc");
        commitAndCleanUp(true);

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_IN_USE)
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true)
        );

        assertThat(nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME))).isNull();
    }

    /*
     * shared storage pool with external locking (e.g. lvmlockd) tests
     */

    @Test
    public void makeAvailableExternalLockingDeactivatesActiveCopy() throws Exception
    {
        // without auto_manage_dual_primary an externally locked shared pool behaves like any other
        // shared pool: the active copy is deactivated before the new leg takes over the shared data
        Resource rsc = createSharedStorPoolRsc(DeviceProviderKind.LVM, true, false);

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void makeAvailableDualPrimaryExternalLockingDualActive() throws Exception
    {
        // live migration on an externally locked shared pool: the migration source stays active and
        // the new leg is created INACTIVE first - giving the source satellite the chance to downgrade
        // its LV locks to shared locks - and activated afterwards, ending with both legs active
        Resource rsc = createSharedStorPoolRsc(DeviceProviderKind.LVM, true, false);

        evaluateTest(
            new MakeAvailableCall()
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );

        Resource newRsc = nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME));
        assertThat(newRsc).isNotNull();
        assertThat(newRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        // the dual-active window is not managed via DRBD net options
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE)).isNull();
    }

    @Test
    public void makeAvailableDualPrimaryExternalLockingUnsupportedProvider() throws Exception
    {
        // a dual-active window on an externally locked pool requires the backing volumes to support
        // shared locks, which ZFS volumes do not
        createSharedStorPoolRsc(DeviceProviderKind.ZFS, true, false);

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_INVLD_PROVIDER)
                .setRscName(SHARED_RSC_NAME)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true)
        );

        assertThat(nodesMap.get(testNode2Name).getResource(new ResourceName(SHARED_RSC_NAME))).isNull();
    }

    /*
     * auto_manage_dual_primary (live migration) tests
     */

    @Test
    public void makeAvailableDualPrimaryWithoutDrbd() throws Exception
    {
        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_INVLD_LAYER_STACK)
                .setLayerStack("storage")
                .setAutoManageDualPrimary(true)
        );
    }

    @Test
    public void makeAvailableDualPrimaryNoSourcePlainAttach() throws Exception
    {
        // the resource is not in use anywhere, so there is no live migration to prepare and the
        // resource is simply made available: clients that cannot tell a live-migration attach from
        // a plain attach (e.g. Proxmox) always set the option
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        addStorPool();

        evaluateTest(
            new MakeAvailableCall(
                // StorPoolName property set on the new resource
                ApiConsts.CREATED,
                // Registered
                ApiConsts.CREATED,
                // Deployed
                ApiConsts.MODIFIED,
                // No volumes => WARN_NOT_FOUND response
                ApiConsts.WARN_NOT_FOUND,
                // updated resync-after entries
                ApiConsts.MASK_INFO,
                // not in use on another node, no dual-primary preparation needed
                ApiConsts.MASK_INFO
            )
                .setAutoManageDualPrimary(true)
        );

        assertThat(testSatelliteNode.getResource(testRscName)).isNotNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(1);
        // nothing was armed for dual-primary
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE)).isNull();
        assertThat(testRscDfn.getProps().getProp(PROP_KEY_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isNull();
    }

    @Test
    public void makeAvailableDualPrimaryTargetAlreadyInUse() throws Exception
    {
        addStorPool();
        createResourceOnNode();
        setInUse(satelliteState, Boolean.TRUE);

        evaluateTest(
            new MakeAvailableCall(
                // "Resource already deployed as requested"
                ApiConsts.MASK_SUCCESS,
                // already in use on the target node, no dual-primary preparation needed
                ApiConsts.MASK_INFO
            )
                .setAutoManageDualPrimary(true)
        );

        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE)).isNull();
    }

    @Test
    public void makeAvailableDualPrimarySetsConnProps() throws Exception
    {
        addStorPool();
        createResourceOnNode();
        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(testNode2Name, Resource.Flags.DRBD_DISKLESS.flagValue, testDisklessStorPoolName);

        // the workload runs on the first node, the resource is migrated to the second one
        setInUse(satelliteState, Boolean.TRUE);

        evaluateTest(
            new MakeAvailableCall()
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );

        ResourceConnection rscConn = getRscConn(testSatelliteNode, node2);
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().getProp(PROP_KEY_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isEqualTo("yes");
        // the effective protocol already was C, nothing to override
        assertThat(rscConn.getProps().getProp(PROP_KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS)).isNull();

        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE))
            .isEqualTo(testNodeName.displayValue);
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE))
            .isEqualTo(testNode2Name.displayValue);
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_PROTOCOL_SET_ON)).isNull();

        // an identical second call is an idempotent re-apply
        evaluateTest(
            new MakeAvailableCall()
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );
        assertThat(rscConn.getProps().getProp(PROP_KEY_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isEqualTo("yes");
    }

    @Test
    public void makeAvailableDualPrimaryEnforcesProtocolC() throws Exception
    {
        addStorPool();
        createResourceOnNode();
        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(testNode2Name, Resource.Flags.DRBD_DISKLESS.flagValue, testDisklessStorPoolName);

        enterScope();
        testRscDfn.getProps().setProp(PROP_KEY_PROTOCOL, "A", ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        commitAndCleanUp(true);

        setInUse(satelliteState, Boolean.TRUE);

        evaluateTest(
            new MakeAvailableCall()
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );

        ResourceConnection rscConn = getRscConn(testSatelliteNode, node2);
        assertThat(rscConn).isNotNull();
        assertThat(rscConn.getProps().getProp(PROP_KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isEqualTo("C");
        // the user-set protocol of the rsc-dfn must not be touched
        assertThat(testRscDfn.getProps().getProp(PROP_KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isEqualTo("A");
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_PROTOCOL_SET_ON))
            .isEqualTo(InternalApiConsts.SET_ON_RSC_CONN);
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_PREV_PROTOCOL)).isNull();
    }

    @Test
    public void makeAvailableDualPrimaryBothDisklessUsesRscDfn() throws Exception
    {
        // between two diskless resources no DRBD connection section is generated, so the props have
        // to be set on rsc-dfn level instead of the rsc-conn
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(testNodeName, Resource.Flags.DRBD_DISKLESS.flagValue, testDisklessStorPoolName);
        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(testNode2Name, Resource.Flags.DRBD_DISKLESS.flagValue, testDisklessStorPoolName);

        setInUse(satelliteState, Boolean.TRUE);

        evaluateTest(
            new MakeAvailableCall()
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true),
            false
        );

        assertThat(testRscDfn.getProps().getProp(PROP_KEY_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isEqualTo("yes");
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE))
            .isEqualTo(testNodeName.displayValue);
    }

    @Test
    public void makeAvailableDualPrimaryConflictingMigration() throws Exception
    {
        addStorPool();
        createResourceOnNode();
        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(testNode2Name, Resource.Flags.DRBD_DISKLESS.flagValue, testDisklessStorPoolName);
        setInUse(satelliteState, Boolean.TRUE);

        // another migration (different target) is already prepared
        enterScope();
        testRscDfn.getProps().setProp(
            InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE,
            testNodeName.displayValue,
            InternalApiConsts.NAMESPC_LIVE_MIGRATE
        );
        testRscDfn.getProps().setProp(
            InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE,
            "SomeOtherTarget",
            InternalApiConsts.NAMESPC_LIVE_MIGRATE
        );
        commitAndCleanUp(true);

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_EXISTS_LIVE_MIGRATE)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true)
        );
    }

    @Test
    public void makeAvailableDualPrimaryMultipleInUse() throws Exception
    {
        addStorPool();
        createResourceOnNode();
        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(testNode2Name, Resource.Flags.DRBD_DISKLESS.flagValue, testDisklessStorPoolName);

        // without live-migrate markers an in-use on two nodes leaves the migration source undecidable
        setInUse(satelliteState, Boolean.TRUE);
        setInUse(satelliteState2, Boolean.TRUE);

        evaluateTest(
            new MakeAvailableCall(ApiConsts.FAIL_EXISTS_LIVE_MIGRATE)
                .setNodeName(testNode2Name.displayValue)
                .setAutoManageDualPrimary(true)
        );
    }

    /*
     * helpers
     */

    /**
     * Creates a second node, a shared storage pool on both nodes, a STORAGE-only rscDfn
     * {@link #SHARED_RSC_NAME} with one volume definition and a single diskful resource on the first
     * node, flagged INACTIVE.
     */
    private Resource createInactiveSharedStorPoolRsc() throws Exception
    {
        return createSharedStorPoolRsc(DeviceProviderKind.LVM, false, true);
    }

    /**
     * Creates a second node, a shared storage pool of the given kind on both nodes, a STORAGE-only
     * rscDfn {@link #SHARED_RSC_NAME} with one volume definition and a single diskful resource on the
     * first node.
     */
    private Resource createSharedStorPoolRsc(
        DeviceProviderKind kind,
        boolean externalLocking,
        boolean inactive
    )
        throws Exception
    {
        StorPoolName sharedSpName = new StorPoolName(SHARED_SP_NAME);
        SharedStorPoolName sharedSpaceName = new SharedStorPoolName(SHARED_SPACE_NAME);

        Node node2 = createSecondNode();
        createStorPool(testSatelliteNode, sharedSpName, kind, sharedSpaceName, externalLocking);
        createStorPool(node2, sharedSpName, kind, sharedSpaceName, externalLocking);

        enterScope();
        ResourceDefinition sharedRscDfn = resourceDefinitionTestFactory.builder(SHARED_RSC_NAME)
            .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(sharedRscDfn.getName(), sharedRscDfn);
        volumeDefinitionTestFactory.builder(SHARED_RSC_NAME, 0)
            .setSize(100 * 1024L)
            .build();
        commitAndCleanUp(true);

        return createSharedStorPoolRscOnNode(testNodeName, inactive);
    }

    /**
     * Creates the {@link #SHARED_RSC_NAME} resource on the given node, optionally flagging it
     * INACTIVE. The rsc-dfn and the shared storage pools have to exist already (see
     * {@link #createSharedStorPoolRsc(DeviceProviderKind, boolean, boolean)}).
     */
    private Resource createSharedStorPoolRscOnNode(NodeName nodeName, boolean inactive) throws Exception
    {
        enterScope();
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, SHARED_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeName.displayValue,
            SHARED_RSC_NAME,
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

        Resource rsc = nodesMap.get(nodeName).getResource(new ResourceName(SHARED_RSC_NAME));
        if (inactive)
        {
            enterScope();
            rsc.getStateFlags().enableFlags(Resource.Flags.INACTIVE);
            commitAndCleanUp(true);
        }

        return rsc;
    }

    /**
     * Creates a snapshot definition with a single snapshot (including its snapshot volume, so the
     * snapshot records the storage pools its data lives on) on the given resource's node.
     */
    private SnapshotDefinition createSnapshotOnNode(Resource rsc, String snapNameRef) throws Exception
    {
        enterScope();
        SnapshotDefinition snapDfn = snapshotDefinitionFactory.create(
            rsc.getResourceDefinition(),
            new SnapshotName(snapNameRef),
            new SnapshotDefinition.Flags[] {SnapshotDefinition.Flags.SUCCESSFUL}
        );
        SnapshotVolumeDefinition snapVlmDfn = snapshotVolumeDefinitionFactory.create(
            snapDfn,
            rsc.getResourceDefinition().getVolumeDfn(new VolumeNumber(0)),
            100 * 1024L,
            new SnapshotVolumeDefinition.Flags[0]
        );
        Snapshot snap = snapshotFactory.create(rsc, snapDfn, new Snapshot.Flags[] {});
        snapshotVolumeFactory.create(rsc, snap, snapVlmDfn);
        commitAndCleanUp(true);

        return snapDfn;
    }

    private void addStorPool() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
    }

    private StorPool createStorPool(Node node, StorPoolName storPoolName, DeviceProviderKind kind) throws Exception
    {
        return createStorPool(node, storPoolName, kind, new SharedStorPoolName(node.getName(), storPoolName), false);
    }

    private StorPool createStorPool(
        Node node,
        StorPoolName storPoolName,
        DeviceProviderKind kind,
        SharedStorPoolName sharedStorPoolName
    )
        throws Exception
    {
        return createStorPool(node, storPoolName, kind, sharedStorPoolName, false);
    }

    private StorPool createStorPool(
        Node node,
        StorPoolName storPoolName,
        DeviceProviderKind kind,
        SharedStorPoolName sharedStorPoolName,
        boolean externalLocking
    )
        throws Exception
    {
        enterScope();

        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(storPoolName);
            storPoolDfnMap.put(storPoolName, storPoolDfn);
        }
        FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(sharedStorPoolName);
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            kind,
            fsm,
            externalLocking
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        commitAndCleanUp(true);

        return storPool;
    }

    private void createResourceOnNode() throws Exception
    {
        createRscOnNode(testNodeName, 0L, testStorPoolName);
    }

    private void createRscOnNode(NodeName nodeName, long flags, StorPoolName storPoolName) throws Exception
    {
        enterScope();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolName.displayValue);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeName.displayValue,
            testRscName.displayValue,
            flags,
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

        commitAndCleanUp(true);

        return node;
    }

    private void setInUse(SatelliteState stltStateRef, @Nullable Boolean inUse)
    {
        stltStateRef.setOnResource(testRscName, SatelliteResourceState::setInUse, inUse);
    }

    private @Nullable String getLiveMigrateMarker(String key) throws Exception
    {
        return testRscDfn.getProps().getProp(key, InternalApiConsts.NAMESPC_LIVE_MIGRATE);
    }

    private @Nullable ResourceConnection getRscConn(Node nodeA, Node nodeB)
    {
        Resource rscA = nodeA.getResource(testRscName);
        Resource rscB = nodeB.getResource(testRscName);
        ResourceConnection rscConn = null;
        if (rscA != null && rscB != null)
        {
            rscConn = rscA.getAbsResourceConnection(rscB);
        }
        return rscConn;
    }

    private class MakeAvailableCall extends AbsApiCallTester
    {
        private String nodeName;
        private String rscName;
        private List<String> layerStack;
        private boolean diskful;
        private boolean autoManageDualPrimary;

        MakeAvailableCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            nodeName = testNodeName.displayValue;
            rscName = testRscName.displayValue;
            layerStack = new ArrayList<>();
            diskful = false;
            autoManageDualPrimary = false;
        }

        MakeAvailableCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        MakeAvailableCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        MakeAvailableCall setLayerStack(String... layers)
        {
            layerStack = Arrays.asList(layers);
            return this;
        }

        MakeAvailableCall setDiskful(boolean diskfulRef)
        {
            diskful = diskfulRef;
            return this;
        }

        MakeAvailableCall setAutoManageDualPrimary(boolean autoManageDualPrimaryRef)
        {
            autoManageDualPrimary = autoManageDualPrimaryRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscMakeAvailableApiCallHandlerProvider.get().makeResourceAvailable(
                nodeName,
                rscName,
                layerStack,
                diskful,
                null,
                false,
                Collections.emptyList(),
                autoManageDualPrimary
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
