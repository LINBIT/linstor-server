package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscDfnInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("checkstyle:magicnumber")
public class RscDfnCloneApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_NODE_B = "TestNodeB";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String CLONE_RSC_NAME = "ClonedRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String FILE_SP_NAME = "FileStorPool";
    private static final String SRC_RSC_GRP = "SrcRscGrp";
    private static final String FILE_RSC_GRP = "FileRscGrp";
    private static final String NO_SP_RSC_GRP = "NoStorPoolGrp";
    private static final String TEST_NET_IF = "eth0";
    private static final long TEST_VLM_SIZE = 100 * 1024L;
    private static final String SHARED_RSC_NAME = "SharedRsc";
    private static final String SHARED_SP_NAME = "SharedPool";
    private static final String SHARED_SPACE_NAME = "SharedSpace";

    private static final String UP_TO_DATE = "UpToDate";

    @Inject
    private Provider<CtrlRscDfnApiCallHandler> rscDfnApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    @Inject
    private Provider<CtrlSnapshotDeleteApiCallHandler> snapDeleteHandlerProvider;
    @Inject
    private Provider<RscDfnInternalCallHandler> rscDfnInternalCallHandlerProvider;
    @Inject
    private Provider<CtrlRscDfnDeleteApiCallHandler> rscDfnDeleteHandlerProvider;
    @Inject
    private ScopeRunner scopeRunner;
    @Inject
    private LockGuardFactory lockGuardFactory;
    @Inject
    private CtrlTransactionHelper ctrlTransactionHelper;

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
    private final ResourceName testRscName;
    private final ResourceName cloneRscName;
    private final StorPoolName testStorPoolName;
    private final StorPoolName fileStorPoolName;

    private Node testNodeA;
    private Node testNodeB;
    private ResourceDefinition srcRscDfn;

    private final SatelliteState stltStateA = new SatelliteState();
    private final SatelliteState stltStateB = new SatelliteState();

    private final AtomicInteger minorNrGenerator = new AtomicInteger(2000);

    public RscDfnCloneApiTest() throws Exception
    {
        testNodeAName = new NodeName(TEST_NODE_A);
        testNodeBName = new NodeName(TEST_NODE_B);
        testRscName = new ResourceName(TEST_RSC_NAME);
        cloneRscName = new ResourceName(CLONE_RSC_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
        fileStorPoolName = new StorPoolName(FILE_SP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(minorNrPoolMock.autoAllocate())
            .thenAnswer(ignoredContext -> minorNrGenerator.getAndIncrement());

        stubAllExtToolsSupported(mockExtToolsMgr);

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenReturn(Mono.just(Collections.emptyMap()));

        stubSatellitePeer(mockSatelliteA, mockExtToolsMgr, stltStateA, true);
        stubSatellitePeer(mockSatelliteB, mockExtToolsMgr, stltStateB, true);

        // resource groups restricting the autoplacer to a specific storage pool, so that the
        // clone's storage pool selection is deterministic
        createRscGrp(SRC_RSC_GRP, TEST_SP_NAME);
        createRscGrp(FILE_RSC_GRP, FILE_SP_NAME);
        createRscGrp(NO_SP_RSC_GRP, "NoSuchStorPool");

        testNodeA = createSatelliteNode(testNodeAName, mockSatelliteA, "10.0.0.1");
        testNodeB = createSatelliteNode(testNodeBName, mockSatelliteB, "10.0.0.2");

        srcRscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .setRscGroupName(SRC_RSC_GRP)
            .build();
        rscDfnMap.put(srcRscDfn.getName(), srcRscDfn);

        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 0)
            .setSize(TEST_VLM_SIZE)
            .build();

        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        deploySharedResource();

        // the satellites report the DRBD volumes as UpToDate; without a reported disk state the
        // clone API refuses to clone
        setDiskState(stltStateA, UP_TO_DATE);
        setDiskState(stltStateB, UP_TO_DATE);

        leaveScope();
    }

    private void createRscGrp(String rscGrpName, String storPoolName) throws Exception
    {
        ResourceGroup rscGrp = resourceGroupTestFactory.builder(rscGrpName)
            .setAutoPlaceStorPoolList(new ArrayList<>(Collections.singletonList(storPoolName)))
            .build();
        rscGrpMap.put(new ResourceGroupName(rscGrpName), rscGrp);
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

        createStorPool(node, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(node, fileStorPoolName, DeviceProviderKind.FILE);

        return node;
    }

    private void createStorPool(Node node, StorPoolName storPoolName, DeviceProviderKind kind) throws Exception
    {
        createStorPool(node, storPoolName, kind, new SharedStorPoolName(node.getName(), storPoolName));
    }

    private void createStorPool(
        Node node,
        StorPoolName storPoolName,
        DeviceProviderKind kind,
        SharedStorPoolName sharedStorPoolName
    )
        throws Exception
    {
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
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);
    }

    private void createResourceOnNode(String nodeName) throws Exception
    {
        createResourceOnNode(nodeName, TEST_RSC_NAME, TEST_SP_NAME);
    }

    private void createResourceOnNode(String nodeName, String rscName, String storPoolName) throws Exception
    {
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolName);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeName,
            rscName,
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

    /**
     * Creates a shared storage pool on both nodes and deploys a STORAGE-only resource
     * {@link #SHARED_RSC_NAME} on both nodes. Both copies are flagged INACTIVE. Must be called
     * from within the setUp scope: a per-test storPoolDfnMap.put conflicts with the transaction
     * manager the setUp scope stamped on the existing storage pool definitions.
     */
    private void deploySharedResource() throws Exception
    {
        StorPoolName sharedSpName = new StorPoolName(SHARED_SP_NAME);
        SharedStorPoolName sharedSpaceName = new SharedStorPoolName(SHARED_SPACE_NAME);
        createStorPool(testNodeA, sharedSpName, DeviceProviderKind.LVM, sharedSpaceName);
        createStorPool(testNodeB, sharedSpName, DeviceProviderKind.LVM, sharedSpaceName);

        ResourceDefinition sharedRscDfn = resourceDefinitionTestFactory.builder(SHARED_RSC_NAME)
            .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(sharedRscDfn.getName(), sharedRscDfn);
        volumeDefinitionTestFactory.builder(SHARED_RSC_NAME, 0)
            .setSize(TEST_VLM_SIZE)
            .build();

        ResourceName sharedRscName = new ResourceName(SHARED_RSC_NAME);
        createResourceOnNode(TEST_NODE_A, SHARED_RSC_NAME, SHARED_SP_NAME);
        createResourceOnNode(TEST_NODE_B, SHARED_RSC_NAME, SHARED_SP_NAME);
        testNodeA.getResource(sharedRscName).getStateFlags().enableFlags(Resource.Flags.INACTIVE);
        testNodeB.getResource(sharedRscName).getStateFlags().enableFlags(Resource.Flags.INACTIVE);
    }

    private void setDiskState(SatelliteState stltState, String diskState) throws Exception
    {
        stltState.setOnVolume(
            testRscName,
            new VolumeNumber(0),
            SatelliteVolumeState::setDiskState,
            diskState
        );
    }

    /**
     * Expected responses of a successful clone of the two-node test resource. All entries carry
     * the resource definition / create context masks of the clone api call unless the entry
     * already has its own masks.
     */
    private long[] cloneSuccessRcs()
    {
        return new long[]
        {
            // cloned resource registered on node A, volume registered on node A,
            // cloned resource registered on node B, volume registered on node B
            ApiConsts.CREATED,
            ApiConsts.MASK_CRT | ApiConsts.MASK_VLM | ApiConsts.CREATED,
            ApiConsts.CREATED,
            ApiConsts.MASK_CRT | ApiConsts.MASK_VLM | ApiConsts.CREATED,
            // the source snapshot (CF_<hash of the target name>): registered, suspended IO, took the snapshot and
            // resumed IO
            // on both satellites
            ApiConsts.CREATED,
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED,
            // deployed the cloned resources on both satellites
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED,
            // disabled the start-cloning flag on both satellites
            ApiConsts.MODIFIED,
            ApiConsts.MODIFIED
        };
    }

    /*
     * validation / error paths
     */

    @Test
    public void cloneUnknownSource() throws Exception
    {
        // characterization: the source resource definition is loaded before the responses are
        // converted, so the not-found error surfaces as a raw ApiRcException instead of an
        // ApiCallRc response entry
        Throwable thrown = catchThrowable(
            () -> new CloneRscDfnCall().setSrcName("UnknownRsc").executeApiCall()
        );
        assertThat(thrown).isInstanceOf(ApiRcException.class);
        assertThat(((ApiRcException) thrown).getApiCallRc().get(0).getReturnCode())
            .isEqualTo(ApiConsts.FAIL_NOT_FOUND_RSC_DFN);
    }

    @Test
    public void cloneNoNameGiven() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_RSC_NAME)
                .setCloneName("")
        );
    }

    @Test
    public void cloneInvalidName() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_RSC_NAME)
                .setCloneName("Invalid Name")
        );
    }

    @Test
    public void cloneTargetExists() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_EXISTS_RSC_DFN)
                .setCloneName(TEST_RSC_NAME)
        );
    }

    @Test
    public void cloneUnknownRscGrp() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .setIntoRscGrpName("UnknownRscGrp")
        );
    }

    @Test
    public void cloneInvalidLayerList() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_LAYER_KIND)
                .setLayerList("NotALayer")
        );
    }

    @Test
    public void clonePassphraseCountMismatch() throws Exception
    {
        // the source has one volume definition, but two passphrases are given
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_REQUEST)
                .setVolumePassphrases("passphrase1", "passphrase2")
        );
    }

    @Test
    public void cloneVolumeSizeCountMismatch() throws Exception
    {
        // the source has one volume definition, but two sizes are given
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_REQUEST)
                .setVolumeSizes(2 * TEST_VLM_SIZE, 2 * TEST_VLM_SIZE)
        );
    }

    @Test
    public void cloneVolumeSizeShrinkRejected() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_VLM_SIZE)
                .setVolumeSizes(TEST_VLM_SIZE / 2)
        );
    }

    @Test
    public void cloneVolumeSizeNotEnoughSpace() throws Exception
    {
        // the test storage pools have 10_000_000 KiB; the grow is rejected before any copy work
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_VLM_SIZE)
                .setVolumeSizes(20_000_000L)
        );
        assertThat(rscDfnMap.get(cloneRscName)).isNull();
    }

    @Test
    public void cloneInvalidOverrideProp() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    @Test
    public void cloneExactSizeRejected() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(
                    ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_EXACT_SIZE,
                    ApiConsts.VAL_TRUE
                )
        );
    }

    @Test
    public void cloneSkipDiskRejected() throws Exception
    {
        enterScope();
        Resource srcRsc = testNodeA.getResource(testRscName);
        srcRsc.getProps().setProp(
            ApiConsts.KEY_DRBD_SKIP_DISK,
            ApiConsts.VAL_TRUE,
            ApiConsts.NAMESPC_DRBD_OPTIONS
        );
        leaveScope();

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_RSC_STATE)
        );
    }

    @Test
    public void cloneNotUpToDateRejected() throws Exception
    {
        setDiskState(stltStateA, "Inconsistent");

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_NOT_ALL_UPTODATE)
        );
    }

    @Test
    public void cloneSourceOnEvacuatingNodeRejected() throws Exception
    {
        /*
         * the source snapshot would not be taken on node B, so a clone target there would never be cloned: the
         * autoplacer finds no storage pool on an evacuating (or offline) node for the clone target
         */
        enterScope();
        testNodeB.getFlags().enableFlags(Node.Flags.EVACUATE);
        leaveScope();

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_NOT_ENOUGH_NODES)
        );
        assertThat(rscDfnMap.get(cloneRscName)).isNull();
    }

    @Test
    public void cloneSourceOnOfflineNodeRejected() throws Exception
    {
        // see cloneSourceOnEvacuatingNodeRejected
        setSatelliteOnline(mockSatelliteB, false);

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_NOT_ENOUGH_NODES)
        );
        assertThat(rscDfnMap.get(cloneRscName)).isNull();
    }

    @Test
    public void cloneSourceRscActivatedWhilePreparingRejected() throws Exception
    {
        /*
         * the clone synchronizes with the background runner on the nodes of the diskful resources it looked up before
         * its transaction, and takes the source snapshot on them without synchronizing again: a resource activated in
         * between would be snapshotted unsynchronized
         */
        enterScope();
        testNodeB.getResource(testRscName).getStateFlags().enableFlags(Resource.Flags.INACTIVE);
        leaveScope();
        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenAnswer(
                ignored -> scopeRunner.fluxInTransactionalScope(
                    "Activate the source resource on node B",
                    lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                    () ->
                    {
                        testNodeB.getResource(testRscName).getStateFlags().disableFlags(Resource.Flags.INACTIVE);
                        ctrlTransactionHelper.commit();
                        return Flux.empty();
                    }
                )
                    .then(Mono.just(Collections.emptyMap()))
            );

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_RSC_STATE)
        );
        assertThat(rscDfnMap.get(cloneRscName)).isNull();
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(cloneSrcSnapName())).isNull();
    }

    @Test
    public void cloneNoSuitableStorPool() throws Exception
    {
        // the target resource group restricts the autoplacer to a storage pool that does not exist
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_NOT_ENOUGH_NODES)
                .setIntoRscGrpName(NO_SP_RSC_GRP)
        );
    }

    @Test
    public void cloneUnsupportedProvider() throws Exception
    {
        // the target resource group restricts the autoplacer to the FILE storage pools, whose
        // provider kind does not support cloning
        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_INVLD_PROVIDER)
                .setIntoRscGrpName(FILE_RSC_GRP)
        );
    }

    @Test
    public void cloneRefusedWhileSharedDualActive() throws Exception
    {
        // during the dual-active window of a live migration both copies of a shared storage pool
        // use the shared data at once: cloning has to wait until only one copy is active again
        enterScope();
        ResourceName sharedRscName = new ResourceName(SHARED_RSC_NAME);
        testNodeA.getResource(sharedRscName).getStateFlags().disableFlags(Resource.Flags.INACTIVE);
        testNodeB.getResource(sharedRscName).getStateFlags().disableFlags(Resource.Flags.INACTIVE);
        leaveScope();

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_IN_USE)
                .setSrcName(SHARED_RSC_NAME)
        );

        assertThat(rscDfnMap.get(cloneRscName)).isNull();
    }

    @Test
    public void cloneDuplicateExtName() throws Exception
    {
        byte[] extName = "CloneExtName".getBytes(StandardCharsets.UTF_8);

        enterScope();
        ResourceDefinition otherRscDfn = resourceDefinitionTestFactory.builder("OtherRsc")
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .setExtName(extName)
            .build();
        rscDfnMap.put(otherRscDfn.getName(), otherRscDfn);
        rscDfnExtNameMap.put(extName, otherRscDfn);
        leaveScope();

        evaluateTest(
            new CloneRscDfnCall(ApiConsts.FAIL_EXISTS_EXT_NAME)
                .setExtName(extName)
        );
    }

    /*
     * success paths
     */

    @Test
    public void cloneSuccess() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(cloneSuccessRcs())
        );

        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn).isNotNull();
        assertThat(clonedRscDfn.getFlags().isSet(ResourceDefinition.Flags.CLONING)).isTrue();
        assertThat(clonedRscDfn.getProps().getProp(InternalApiConsts.KEY_CLONED_FROM))
            .isEqualTo(TEST_RSC_NAME);
        assertThat(clonedRscDfn.getResourceGroup().getName().displayValue).isEqualTo(SRC_RSC_GRP);
        assertThat(clonedRscDfn.getLayerStack())
            .containsExactly(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);

        // the volume definitions were copied
        assertThat(clonedRscDfn.getVolumeDfnCount()).isEqualTo(1);
        assertThat(clonedRscDfn.getVolumeDfn(new VolumeNumber(0)).getVolumeSize())
            .isEqualTo(TEST_VLM_SIZE);

        // a cloned resource was created on each node of the source
        for (Node node : Arrays.asList(testNodeA, testNodeB))
        {
            Resource clonedRsc = node.getResource(cloneRscName);
            assertThat(clonedRsc).isNotNull();
            assertThat(clonedRsc.getProps().getProp(ApiConsts.KEY_STOR_POOL_NAME))
                .isEqualTo(TEST_SP_NAME);
            Volume clonedVlm = clonedRsc.getVolume(new VolumeNumber(0));
            assertThat(clonedVlm).isNotNull();
            assertThat(clonedVlm.getFlags().isSet(Volume.Flags.CLONING)).isTrue();
            // the start flag was removed again by the final flux stage
            assertThat(clonedVlm.getFlags().isSet(Volume.Flags.CLONING_START)).isFalse();
        }

        // the target is cloned from a regular snapshot of the source, taken on both diskful nodes
        String srcSnapName = cloneSrcSnapName().displayValue;
        assertThat(srcSnapName).matches(InternalApiConsts.CLONE_FOR_PREFIX + "[0-9a-f]{8}");
        assertThat(
            clonedRscDfn.getProps().getProp(
                InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
                InternalApiConsts.NAMESPC_INTERNAL_CLONE
            )
        ).isEqualTo(srcSnapName);
        SnapshotDefinition srcSnapDfn = rscDfnMap.get(testRscName).getSnapshotDfn(new SnapshotName(srcSnapName));
        assertThat(srcSnapDfn).isNotNull();
        assertThat(srcSnapDfn.getFlags().isSet(SnapshotDefinition.Flags.FOR_CLONE)).isTrue();
        for (Node node : Arrays.asList(testNodeA, testNodeB))
        {
            assertThat(srcSnapDfn.getSnapshot(node.getName())).isNotNull();
            // IO of the source was resumed by the snapshot creation
            Resource srcRsc = node.getResource(testRscName);
            assertThat(srcRsc.getLayerData().getShouldSuspendIo()).isFalse();
        }
    }

    /*
     * the source snapshot (CF_<hash of the target name>) of a running clone
     */

    /** the name of the source snapshot of a clone to {@link #CLONE_RSC_NAME} if no name is taken */
    private SnapshotName cloneSrcSnapName()
    {
        return CtrlRscDfnApiCallHandler.getCloneSourceSnapshotName(CLONE_RSC_NAME, ignored -> false);
    }

    private @Nullable String getCloneSrcSnapNameProp()
    {
        return rscDfnMap.get(cloneRscName).getProps().getProp(
            InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
            InternalApiConsts.NAMESPC_INTERNAL_CLONE
        );
    }

    private boolean cloneSrcSnapshotDeleted() throws Exception
    {
        @Nullable SnapshotDefinition snapDfn = rscDfnMap.get(testRscName).getSnapshotDfn(cloneSrcSnapName());
        return snapDfn == null || snapDfn.getFlags().isSet(SnapshotDefinition.Flags.DELETE);
    }

    /** reports the clone result of volume 0 of the clone target as the given satellite */
    private void reportCloneUpdate(Peer satellite, boolean success)
    {
        rscDfnInternalCallHandlerProvider.get().handleCloneUpdate(CLONE_RSC_NAME, 0, success)
            .contextWrite(
                Context.of(
                    ApiModule.API_CALL_NAME, "TestApiCallName",
                    Peer.class, satellite,
                    ApiModule.API_CALL_ID, 1L
                )
            )
            .blockLast();
    }

    @Test
    public void cloneSourceSnapshotCannotBeDeletedWhileCloning() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));

        assertThatThrownBy(
            () -> collect(snapDeleteHandlerProvider.get().deleteSnapshot(testRscName, cloneSrcSnapName(), null))
        )
            .isInstanceOf(ApiRcException.class)
            .hasMessageContaining("is the source of the running clone to '" + CLONE_RSC_NAME + "'");
        assertThat(cloneSrcSnapshotDeleted()).isFalse();
    }

    @Test
    public void failedCloneSourceSnapshotCanBeDeleted() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));
        enterScope();
        rscDfnMap.get(testRscName).getSnapshotDfn(cloneSrcSnapName()).getFlags()
            .enableFlags(SnapshotDefinition.Flags.FAILED_DEPLOYMENT);
        leaveScope();

        collect(snapDeleteHandlerProvider.get().deleteSnapshot(testRscName, cloneSrcSnapName(), null));

        assertThat(cloneSrcSnapshotDeleted()).isTrue();
    }

    @Test
    public void unreferencedCloneSourceSnapshotCanBeDeleted() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));
        // the clone no longer referenced it, but deleting it was interrupted (e.g. by a restart of the controller)
        enterScope();
        rscDfnMap.get(cloneRscName).getProps().removeProp(
            InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
            InternalApiConsts.NAMESPC_INTERNAL_CLONE
        );
        leaveScope();

        collect(snapDeleteHandlerProvider.get().deleteSnapshot(testRscName, cloneSrcSnapName(), null));

        assertThat(cloneSrcSnapshotDeleted()).isTrue();
    }

    /**
     * A snapshot that was not taken by a clone, but has the name a clone target of the source reserved for its source
     * snapshot (e.g. it was created before the clone took its snapshot).
     */
    private SnapshotDefinition createSnapshotWithNameReservedByOtherClone() throws Exception
    {
        enterScope();
        ResourceDefinition otherCloneRscDfn = resourceDefinitionTestFactory.builder(CLONE_RSC_NAME).build();
        rscDfnMap.put(otherCloneRscDfn.getName(), otherCloneRscDfn);
        otherCloneRscDfn.getProps().setProp(InternalApiConsts.KEY_CLONED_FROM, TEST_RSC_NAME);
        otherCloneRscDfn.getProps().setProp(
            InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
            cloneSrcSnapName().displayValue,
            InternalApiConsts.NAMESPC_INTERNAL_CLONE
        );
        SnapshotDefinition snapDfn = snapshotDefinitionFactory.create(
            rscDfnMap.get(testRscName),
            cloneSrcSnapName(),
            new SnapshotDefinition.Flags[0]
        );
        leaveScope();
        return snapDfn;
    }

    @Test
    public void snapshotWithReservedNameCanBeDeleted() throws Exception
    {
        createSnapshotWithNameReservedByOtherClone();

        collect(snapDeleteHandlerProvider.get().deleteSnapshot(testRscName, cloneSrcSnapName(), null));

        assertThat(cloneSrcSnapshotDeleted()).isTrue();
    }

    @Test
    public void snapshotWithReservedNameNotDeletedWithCloneTarget() throws Exception
    {
        SnapshotDefinition snapDfn = createSnapshotWithNameReservedByOtherClone();

        collect(rscDfnDeleteHandlerProvider.get().deleteResourceDefinition(CLONE_RSC_NAME));

        assertThat(rscDfnMap.get(cloneRscName)).isNull();
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(cloneSrcSnapName())).isSameAs(snapDfn);
        assertThat(snapDfn.getFlags().isSet(SnapshotDefinition.Flags.DELETE)).isFalse();
    }

    @Test
    public void cloneSourceSnapshotDeletedWhenAllVolumesCloned() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));
        Mockito.when(mockSatelliteA.getNode()).thenReturn(testNodeA);
        Mockito.when(mockSatelliteB.getNode()).thenReturn(testNodeB);

        // node B still clones from the snapshot
        reportCloneUpdate(mockSatelliteA, true);
        assertThat(cloneSrcSnapshotDeleted()).isFalse();

        reportCloneUpdate(mockSatelliteB, true);
        assertThat(cloneSrcSnapshotDeleted()).isTrue();
        assertThat(
            rscDfnMap.get(cloneRscName).getProps().getProp(
                InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
                InternalApiConsts.NAMESPC_INTERNAL_CLONE
            )
        ).isNull();
    }

    @Test
    public void cloneSourceSnapshotDeletedWhenAllVolumesFinishedOrFailed() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));
        Mockito.when(mockSatelliteA.getNode()).thenReturn(testNodeA);
        Mockito.when(mockSatelliteB.getNode()).thenReturn(testNodeB);

        reportCloneUpdate(mockSatelliteA, false);
        // the clone failed, but node B is still reading from the snapshot
        assertThat(rscDfnMap.get(cloneRscName).getFlags().isSet(ResourceDefinition.Flags.FAILED)).isTrue();
        assertThat(cloneSrcSnapshotDeleted()).isFalse();

        reportCloneUpdate(mockSatelliteB, true);
        assertThat(cloneSrcSnapshotDeleted()).isTrue();
    }

    @Test
    public void cloneSourceSnapshotDeletedWithCloneTarget() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));

        collect(rscDfnDeleteHandlerProvider.get().deleteResourceDefinition(CLONE_RSC_NAME));

        assertThat(cloneSrcSnapshotDeleted()).isTrue();
    }

    @Test
    public void cloneSourceSnapshotDeletedWithRetriedCloneTargetDeletion() throws Exception
    {
        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));

        // satellite B fails to delete the target's resource: the deletion stops before the target is deleted
        Mockito.when(mockSatelliteB.apiCall(Mockito.anyString(), any()))
            .thenReturn(
                Flux.error(
                    new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, "delete failed"))
                )
            );
        collect(rscDfnDeleteHandlerProvider.get().deleteResourceDefinition(CLONE_RSC_NAME));
        assertThat(rscDfnMap.get(cloneRscName)).isNotNull();
        assertThat(cloneSrcSnapshotDeleted()).isFalse();

        // the deletion is retried (as on reconnect of the satellite)
        Mockito.when(mockSatelliteB.apiCall(Mockito.anyString(), any())).thenReturn(Flux.empty());
        collect(rscDfnDeleteHandlerProvider.get().deleteResourceDefinition(CLONE_RSC_NAME));
        assertThat(rscDfnMap.get(cloneRscName)).isNull();
        assertThat(cloneSrcSnapshotDeleted()).isTrue();
    }

    @Test
    public void cloneSourceSnapshotNameIsHashOfTargetName()
    {
        String shortName = CtrlRscDfnApiCallHandler.getCloneSourceSnapshotName("short", ignored -> false)
            .displayValue;
        String longName = CtrlRscDfnApiCallHandler.getCloneSourceSnapshotName("a".repeat(48), ignored -> false)
            .displayValue;
        assertThat(shortName).isEqualTo(InternalApiConsts.CLONE_FOR_PREFIX + String.format("%08x", "short".hashCode()));
        assertThat(longName).matches(InternalApiConsts.CLONE_FOR_PREFIX + "[0-9a-f]{8}");
        assertThat(longName).isNotEqualTo(shortName);
    }

    @Test
    public void cloneSourceSnapshotNameSuffixedWhileTaken() throws Exception
    {
        String baseName = cloneSrcSnapName().displayValue;
        Set<SnapshotName> takenNames = Set.of(new SnapshotName(baseName), new SnapshotName(baseName + "_1"));

        assertThat(
            CtrlRscDfnApiCallHandler.getCloneSourceSnapshotName(CLONE_RSC_NAME, takenNames::contains).displayValue
        )
            .isEqualTo(baseName + "_2");
    }

    @Test
    public void cloneSourceSnapshotNameAvoidsExistingSnapshot() throws Exception
    {
        String baseName = cloneSrcSnapName().displayValue;
        enterScope();
        SnapshotDefinition existingSnapDfn = snapshotDefinitionFactory.create(
            rscDfnMap.get(testRscName),
            new SnapshotName(baseName),
            new SnapshotDefinition.Flags[0]
        );
        leaveScope();

        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));

        String srcSnapName = baseName + "_1";
        assertThat(getCloneSrcSnapNameProp()).isEqualTo(srcSnapName);
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(new SnapshotName(srcSnapName))).isNotNull();
        // the snapshot that already had the name is left alone
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(new SnapshotName(baseName))).isSameAs(existingSnapDfn);
        assertThat(existingSnapDfn.getFlags().isSet(SnapshotDefinition.Flags.DELETE)).isFalse();
    }

    @Test
    public void cloneSourceSnapshotNameAvoidsNameReservedByOtherClone() throws Exception
    {
        // another clone of the source that did not take its source snapshot yet
        String baseName = cloneSrcSnapName().displayValue;
        enterScope();
        ResourceDefinition otherCloneRscDfn = resourceDefinitionTestFactory.builder("OtherClone").build();
        rscDfnMap.put(otherCloneRscDfn.getName(), otherCloneRscDfn);
        otherCloneRscDfn.getProps().setProp(InternalApiConsts.KEY_CLONED_FROM, TEST_RSC_NAME);
        otherCloneRscDfn.getProps().setProp(
            InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
            baseName,
            InternalApiConsts.NAMESPC_INTERNAL_CLONE
        );
        leaveScope();

        evaluateTest(new CloneRscDfnCall(cloneSuccessRcs()));

        assertThat(getCloneSrcSnapNameProp()).isEqualTo(baseName + "_1");
    }

    @Test
    public void cloneIntoOtherRscGrp() throws Exception
    {
        String targetGrpName = "TargetRscGrp";
        enterScope();
        createRscGrp(targetGrpName, TEST_SP_NAME);
        leaveScope();

        evaluateTest(
            new CloneRscDfnCall(cloneSuccessRcs())
                .setIntoRscGrpName(targetGrpName)
        );

        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn).isNotNull();
        assertThat(clonedRscDfn.getResourceGroup().getName().displayValue).isEqualTo(targetGrpName);
    }

    @Test
    public void cloneGeneratedNameFromExtName() throws Exception
    {
        byte[] extName = "ExternalNameOfClone".getBytes(StandardCharsets.UTF_8);
        evaluateTest(
            new CloneRscDfnCall(cloneSuccessRcs())
                .setCloneName("")
                .setExtName(extName)
        );

        ResourceDefinition clonedRscDfn = rscDfnExtNameMap.get(extName);
        assertThat(clonedRscDfn).isNotNull();
        assertThat(clonedRscDfn.getName().displayValue).isEqualTo("ExternalNameOfClone");
        assertThat(clonedRscDfn.getExternalName()).isEqualTo(extName);

        // the source snapshot is named after the generated target name, not after the (empty) given name
        String srcSnapName = CtrlRscDfnApiCallHandler.getCloneSourceSnapshotName(
            "ExternalNameOfClone",
            ignored -> false
        ).displayValue;
        assertThat(
            clonedRscDfn.getProps().getProp(
                InternalApiConsts.KEY_CLONE_SOURCE_SNAPSHOT,
                InternalApiConsts.NAMESPC_INTERNAL_CLONE
            )
        ).isEqualTo(srcSnapName);
        assertThat(rscDfnMap.get(testRscName).getSnapshotDfn(new SnapshotName(srcSnapName))).isNotNull();
    }

    @Test
    public void cloneFailsIfSourceSnapshotFails() throws Exception
    {
        // satellite B fails to suspend IO for the source snapshot
        Mockito.when(
            mockSatelliteB.apiCall(Mockito.eq(InternalApiConsts.API_CHANGED_DATA), any())
        )
            .thenReturn(
                Flux.error(
                    new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, "snapshot failed"))
                )
            );

        ApiCallRc rc = new CloneRscDfnCall().executeApiCall();

        assertThat(rc.stream().anyMatch(entry -> entry.isError() && entry.getMessage().equals("snapshot failed")))
            .isTrue();
        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn.getFlags().isSet(ResourceDefinition.Flags.FAILED)).isTrue();
        assertThat(cloneSrcSnapshotDeleted()).isTrue();
        // the target was not deployed: the start-cloning flag was not removed
        for (Node node : Arrays.asList(testNodeA, testNodeB))
        {
            Volume clonedVlm = node.getResource(cloneRscName).getVolume(new VolumeNumber(0));
            assertThat(clonedVlm.getFlags().isSet(Volume.Flags.CLONING_START)).isTrue();
        }
    }

    @Test
    public void cloneUseZfsClone() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(cloneSuccessRcs())
                .setUseZfsClone(Boolean.TRUE)
        );

        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn).isNotNull();
        assertThat(clonedRscDfn.getProps().getProp(InternalApiConsts.KEY_USE_ZFS_CLONE))
            .isEqualTo("true");
    }

    @Test
    public void cloneWithPropOverrides() throws Exception
    {
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        String copiedKey = ApiConsts.NAMESPC_AUXILIARY + "/copied";

        enterScope();
        srcRscDfn.getProps().setProp(copiedKey, "srcValue");
        leaveScope();

        CloneRscDfnCall call = new CloneRscDfnCall(cloneSuccessRcs())
            .overrideProps(auxKey, "value")
            .deleteProps(copiedKey);
        // the property changes are reported first, before the resource registration entries
        call.retCodes.add(
            0,
            ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_CRT | ApiConsts.CREATED // props set
        );
        call.retCodes.add(
            1,
            ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.DELETED // props deleted
        );
        evaluateTest(call);

        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn).isNotNull();
        assertThat(clonedRscDfn.getProps().getProp(auxKey)).isEqualTo("value");
        // the copied source property was deleted on the clone but kept on the source
        assertThat(clonedRscDfn.getProps().getProp(copiedKey)).isNull();
        assertThat(srcRscDfn.getProps().getProp(copiedKey)).isEqualTo("srcValue");
    }

    @Test
    public void cloneSuccessWithVolumeSizes() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(cloneSuccessRcs())
                .setVolumeSizes(2 * TEST_VLM_SIZE)
        );

        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn).isNotNull();
        VolumeDefinition clonedVlmDfn = clonedRscDfn.getVolumeDfn(new VolumeNumber(0));
        // the clone is created at the source size; the grow runs once all volumes finished cloning
        assertThat(clonedVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
        assertThat(
            clonedVlmDfn.getProps().getProp(
                InternalApiConsts.KEY_CLONE_PENDING_RESIZE, InternalApiConsts.NAMESPC_INTERNAL_CLONE
            )
        ).isEqualTo(Long.toString(2 * TEST_VLM_SIZE));
    }

    @Test
    public void cloneVolumeSizeZeroKeepsSourceSize() throws Exception
    {
        evaluateTest(
            new CloneRscDfnCall(cloneSuccessRcs())
                .setVolumeSizes(0L)
        );

        ResourceDefinition clonedRscDfn = rscDfnMap.get(cloneRscName);
        assertThat(clonedRscDfn).isNotNull();
        VolumeDefinition clonedVlmDfn = clonedRscDfn.getVolumeDfn(new VolumeNumber(0));
        assertThat(clonedVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
        assertThat(
            clonedVlmDfn.getProps().getProp(
                InternalApiConsts.KEY_CLONE_PENDING_RESIZE, InternalApiConsts.NAMESPC_INTERNAL_CLONE
            )
        ).isNull();
    }

    private class CloneRscDfnCall extends AbsApiCallTester
    {
        private String srcName;
        private String cloneName;
        private byte[] extName;
        private Boolean useZfsClone;
        private List<String> volumePassphrases;
        private List<Long> volumeSizes;
        private List<String> layerList;
        private String intoRscGrpName;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deleteNamespaces;

        CloneRscDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC_DFN,
                ApiConsts.MASK_CRT,
                expectedRcs
            );

            srcName = TEST_RSC_NAME;
            cloneName = CLONE_RSC_NAME;
            extName = null;
            useZfsClone = null;
            volumePassphrases = null;
            volumeSizes = null;
            layerList = null;
            intoRscGrpName = null;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deleteNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscDfnApiCallHandlerProvider.get().cloneRscDfn(
                srcName,
                cloneName,
                extName,
                useZfsClone,
                volumePassphrases,
                volumeSizes,
                layerList,
                intoRscGrpName,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        CloneRscDfnCall setSrcName(String srcNameRef)
        {
            srcName = srcNameRef;
            return this;
        }

        CloneRscDfnCall setCloneName(String cloneNameRef)
        {
            cloneName = cloneNameRef;
            return this;
        }

        CloneRscDfnCall setExtName(byte[] extNameRef)
        {
            extName = extNameRef;
            return this;
        }

        CloneRscDfnCall setUseZfsClone(Boolean useZfsCloneRef)
        {
            useZfsClone = useZfsCloneRef;
            return this;
        }

        CloneRscDfnCall setVolumePassphrases(String... passphrases)
        {
            volumePassphrases = new ArrayList<>(Arrays.asList(passphrases));
            return this;
        }

        CloneRscDfnCall setVolumeSizes(Long... sizes)
        {
            volumeSizes = new ArrayList<>(Arrays.asList(sizes));
            return this;
        }

        CloneRscDfnCall setLayerList(String... layers)
        {
            layerList = new ArrayList<>(Arrays.asList(layers));
            return this;
        }

        CloneRscDfnCall setIntoRscGrpName(String intoRscGrpNameRef)
        {
            intoRscGrpName = intoRscGrpNameRef;
            return this;
        }

        CloneRscDfnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        CloneRscDfnCall deleteProps(String... keys)
        {
            deletePropKeys.addAll(Arrays.asList(keys));
            return this;
        }
    }
}
