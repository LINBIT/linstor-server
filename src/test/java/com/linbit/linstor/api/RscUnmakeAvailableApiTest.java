package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscUnmakeAvailableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.layer.LayerPayload;
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
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class RscUnmakeAvailableApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_NODE_2_NAME = "TestSatellite2";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_DISKLESS_SP_NAME = "TestDisklessPool";
    private static final String SHARED_RSC_NAME = "SharedRsc";

    private static final String PROP_KEY_TWO_PRIMARIES = "allow-two-primaries";
    private static final String PROP_KEY_PROTOCOL = "protocol";

    @Inject
    private Provider<CtrlRscUnmakeAvailableApiCallHandler> rscUnmakeAvailableApiCallHandlerProvider;
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

    public RscUnmakeAvailableApiTest() throws Exception
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

    @Test
    public void unmakeAvailableUnknownRscDfn() throws Exception
    {
        // a missing resource definition is a successful no-op, so automation clients can always
        // issue unmake-available after a detach
        evaluateTest(
            new UnmakeAvailableCall(ApiConsts.WARN_NOT_FOUND)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void unmakeAvailableUnknownNode() throws Exception
    {
        // an unknown node is most likely a typo and must fail
        evaluateTest(
            new UnmakeAvailableCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void unmakeAvailableNotDeployed() throws Exception
    {
        // node and resource definition exist, but the resource was never deployed on the node
        evaluateTest(
            new UnmakeAvailableCall(ApiConsts.MASK_INFO)
        );
    }

    @Test
    public void unmakeAvailableInUse() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        satelliteState.setOnResource(testRscName, SatelliteResourceState::setInUse, Boolean.TRUE);

        evaluateTest(
            new UnmakeAvailableCall(ApiConsts.FAIL_IN_USE)
        );

        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
    }

    @Test
    public void unmakeAvailableDiskfulKept() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new UnmakeAvailableCall(
                // diskful resource is not deleted, only a warning is given
                ApiConsts.MASK_WARN
            )
        );

        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
    }

    @Test
    public void unmakeAvailableTiebreakerKept() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_2_NAME, Resource.Flags.TIE_BREAKER.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new UnmakeAvailableCall(
                // tiebreaker is left in place
                ApiConsts.MASK_INFO
            )
                .setNodeName(TEST_NODE_2_NAME)
        );

        Resource rsc = node2.getResource(testRscName);
        assertThat(rsc).isNotNull();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.TIE_BREAKER)).isTrue();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DELETE)).isFalse();
    }

    @Test
    public void unmakeAvailableDeletesDiskless() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);
        setSatelliteOnline(mockSatellite2, true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_2_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new UnmakeAvailableCall()
                .setNodeName(TEST_NODE_2_NAME),
            false
        );

        assertThat(node2.getResource(testRscName)).isNull();
        assertThat(testSatelliteNode.getResource(testRscName)).isNotNull();
        assertThat(testRscDfn.getResourceCount()).isEqualTo(1);
    }

    @Test
    public void unmakeAvailableCleansDualPrimaryProps() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        Node node2 = createSecondNode();
        createStorPool(node2, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_2_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        // simulate the state make-available with auto_manage_dual_primary left behind for a migration
        // from the diskful node to the diskless node, with a protocol that was overridden from A to C
        enterScope();
        Resource srcRsc = testSatelliteNode.getResource(testRscName);
        Resource tgtRsc = node2.getResource(testRscName);
        ResourceConnection rscConn = resourceConnectionFactory.create(srcRsc, tgtRsc, null);
        rscConn.getProps().setProp(PROP_KEY_TWO_PRIMARIES, "yes", ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        rscConn.getProps().setProp(PROP_KEY_PROTOCOL, "C", ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        // also an rsc-dfn level prop, the cleanup is unconditional and has to remove it as well
        testRscDfn.getProps().setProp(PROP_KEY_TWO_PRIMARIES, "yes", ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        setLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE, TEST_NODE_NAME);
        setLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE, TEST_NODE_2_NAME);
        setLiveMigrateMarker(
            InternalApiConsts.KEY_LIVE_MIGRATE_PROTOCOL_SET_ON,
            InternalApiConsts.SET_ON_RSC_CONN
        );
        setLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_PREV_PROTOCOL, "A");
        commitAndCleanUp(true);

        // unmake on the diskful migration source: the resource is kept, but all dual-primary
        // settings have to be reverted
        evaluateTest(
            new UnmakeAvailableCall(),
            false
        );

        assertThat(testSatelliteNode.getResource(testRscName)).isNotNull();

        assertThat(rscConn.getProps().getProp(PROP_KEY_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isNull();
        assertThat(rscConn.getProps().getProp(PROP_KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isEqualTo("A");
        assertThat(testRscDfn.getProps().getProp(PROP_KEY_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS))
            .isNull();
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE)).isNull();
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE)).isNull();
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_PROTOCOL_SET_ON)).isNull();
        assertThat(getLiveMigrateMarker(InternalApiConsts.KEY_LIVE_MIGRATE_PREV_PROTOCOL)).isNull();
    }

    @Test
    public void unmakeAvailableExternalLockingSecondLegDeleted() throws Exception
    {
        // reverting a live migration on an externally locked (e.g. lvmlockd) shared storage pool:
        // the leg on the migration source is deactivated first - releasing its LV locks, so the
        // remaining leg can upgrade back to exclusive locks - and then deleted; the shared data
        // stays with the other leg
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        setSatelliteOnline(mockSatellite, true);
        setSatelliteOnline(mockSatellite2, true);

        Node node2 = createSecondNode();
        createDualActiveExternalLockingRsc(node2);

        evaluateTest(
            new UnmakeAvailableCall()
                .setRscName(SHARED_RSC_NAME),
            false
        );

        assertThat(testSatelliteNode.getResource(new ResourceName(SHARED_RSC_NAME))).isNull();
        assertThat(node2.getResource(new ResourceName(SHARED_RSC_NAME))).isNotNull();
    }

    /*
     * helpers
     */

    /**
     * Creates a shared storage pool with external locking on both nodes and a STORAGE-only rscDfn
     * {@link #SHARED_RSC_NAME} with an active resource on both nodes, mirroring the dual-active
     * state during a live migration.
     */
    private void createDualActiveExternalLockingRsc(Node node2) throws Exception
    {
        StorPoolName sharedSpName = new StorPoolName("SharedPool");
        SharedStorPoolName sharedSpaceName = new SharedStorPoolName("SharedSpace");
        createStorPool(testSatelliteNode, sharedSpName, DeviceProviderKind.LVM, sharedSpaceName, true);
        createStorPool(node2, sharedSpName, DeviceProviderKind.LVM, sharedSpaceName, true);

        enterScope();
        ResourceDefinition sharedRscDfn = resourceDefinitionTestFactory.builder(SHARED_RSC_NAME)
            .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(sharedRscDfn.getName(), sharedRscDfn);
        volumeDefinitionTestFactory.builder(SHARED_RSC_NAME, 0)
            .setSize(100 * 1024L)
            .build();
        commitAndCleanUp(true);

        createRscOnNode(TEST_NODE_NAME, SHARED_RSC_NAME, 0L, sharedSpName.displayValue);
        createRscOnNode(TEST_NODE_2_NAME, SHARED_RSC_NAME, 0L, sharedSpName.displayValue);
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

        commitAndCleanUp(true);

        return storPool;
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
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            kind,
            freeSpaceMgrFactory.getInstance(sharedStorPoolName),
            externalLocking
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        commitAndCleanUp(true);

        return storPool;
    }

    private void createRscOnNode(String nodeNameStr, long flags, String storPoolNameStr) throws Exception
    {
        createRscOnNode(nodeNameStr, TEST_RSC_NAME, flags, storPoolNameStr);
    }

    private void createRscOnNode(String nodeNameStr, String rscNameStr, long flags, String storPoolNameStr)
        throws Exception
    {
        enterScope();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeNameStr,
            rscNameStr,
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

        commitAndCleanUp(true);
    }

    /**
     * Sets the given internal live-migrate marker prop, must be called within an active scope.
     */
    private void setLiveMigrateMarker(String key, String value) throws Exception
    {
        testRscDfn.getProps().setProp(key, value, InternalApiConsts.NAMESPC_LIVE_MIGRATE);
    }

    private @Nullable String getLiveMigrateMarker(String key) throws Exception
    {
        return testRscDfn.getProps().getProp(key, InternalApiConsts.NAMESPC_LIVE_MIGRATE);
    }

    private class UnmakeAvailableCall extends AbsApiCallTester
    {
        private String nodeName;
        private String rscName;

        UnmakeAvailableCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            rscName = TEST_RSC_NAME;
        }

        UnmakeAvailableCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        UnmakeAvailableCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscUnmakeAvailableApiCallHandlerProvider.get().unmakeResourceAvailable(nodeName, rscName)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
