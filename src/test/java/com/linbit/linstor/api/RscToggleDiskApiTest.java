package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler.ToggleOp;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
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
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("checkstyle:magicnumber")
public class RscToggleDiskApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_NODE_2_NAME = "TestSatellite2";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_DISKLESS_SP_NAME = "TestDisklessPool";

    @Inject
    private Provider<CtrlRscToggleDiskApiCallHandler> rscToggleDiskApiCallHandlerProvider;
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
    private Node testSatelliteNode2;
    private ResourceDefinition testRscDfn;

    public RscToggleDiskApiTest() throws Exception
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

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenReturn(Mono.just(Collections.emptyMap()));

        stubSatellitePeer(mockSatellite, mockExtToolsMgr, new SatelliteState(), true);
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), true);
        stubAllExtToolsSupported(mockExtToolsMgr);

        testSatelliteNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testSatelliteNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testSatelliteNode);

        testSatelliteNode2 = nodeFactory.create(
            testNode2Name,
            Node.Type.SATELLITE,
            null
        );
        testSatelliteNode2.setPeer(mockSatellite2);
        nodesMap.put(testNode2Name, testSatelliteNode2);

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
     * not-found validation
     */

    @Test
    public void toggleDiskUnknownNode() throws Exception
    {
        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void toggleDiskUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void toggleDiskRscNotDeployedOnNode() throws Exception
    {
        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.FAIL_NOT_FOUND_RSC)
        );
    }

    /*
     * noop validation
     */

    @Test
    public void toggleDiskAlreadyDiskful() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.INFO_NOOP)
        );
    }

    @Test
    public void toggleDiskAlreadyDiskless() throws Exception
    {
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKLESS, ApiConsts.INFO_NOOP)
        );
    }

    /*
     * diskful -> diskless validation
     */

    @Test
    public void toggleDisklessLastDiskfulRejected() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);

        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKLESS, ApiConsts.FAIL_INSUFFICIENT_REPLICA_COUNT)
        );

        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DISK_REMOVE_REQUESTED)).isFalse();
    }

    @Test
    public void toggleDisklessWithBackingStorPoolRejected() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(testSatelliteNode2, testStorPoolName, DeviceProviderKind.LVM);
        createRscOnNode(TEST_NODE_NAME, 0L, TEST_SP_NAME);
        createRscOnNode(TEST_NODE_2_NAME, 0L, TEST_SP_NAME);

        // toggling to diskless while explicitly specifying a storage pool with backing disk is rejected
        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKLESS, ApiConsts.FAIL_INVLD_STOR_POOL_NAME)
                .setNodeName(TEST_NODE_2_NAME)
                .setStorPoolName(TEST_SP_NAME)
        );

        Resource rsc = testSatelliteNode2.getResource(testRscName);
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DISK_REMOVE_REQUESTED)).isFalse();
    }

    /*
     * diskless -> diskful validation
     */

    @Test
    public void toggleDiskfulUnknownStorPool() throws Exception
    {
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL)
                .setStorPoolName("UnknownStorPool")
        );
    }

    @Test
    public void toggleDiskfulMigrateFromUnknownNode() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.FAIL_NOT_FOUND_NODE)
                .setStorPoolName(TEST_SP_NAME)
                .setMigrateFrom("UnknownNode")
        );
    }

    @Test
    public void toggleDiskfulMigrateFromNodeWithoutResource() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        // the migration source node exists but does not have the resource deployed
        evaluateTest(
            new ToggleDiskCall(ToggleOp.INTO_DRBD_DISKFUL, ApiConsts.FAIL_NOT_FOUND_RSC)
                .setStorPoolName(TEST_SP_NAME)
                .setMigrateFrom(TEST_NODE_2_NAME)
        );
    }

    /*
     * state machine start (up to the mocked satellite)
     */

    @Test
    public void toggleDiskfulOfflineSatellite() throws Exception
    {
        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        setSatelliteOnline(mockSatellite, false);

        evaluateTest(
            new ToggleDiskCall(
                ToggleOp.INTO_DRBD_DISKFUL,
                // addition of disk registered
                ApiConsts.MODIFIED,
                // satellite is offline, operation stays pending
                ApiConsts.WARN_NOT_CONNECTED
            )
                .setStorPoolName(TEST_SP_NAME)
        );

        // the operation is only requested; it is continued when the satellite reconnects
        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DISK_ADD_REQUESTED)).isTrue();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS)).isTrue();
    }

    @Test
    public void toggleDiskfulOnlineSatellite() throws Exception
    {
        Mockito.when(mockPeer.isOnline()).thenReturn(true);

        createStorPool(testSatelliteNode, testStorPoolName, DeviceProviderKind.LVM);
        createStorPool(testSatelliteNode, testDisklessStorPoolName, DeviceProviderKind.DISKLESS);
        createRscOnNode(TEST_NODE_NAME, Resource.Flags.DRBD_DISKLESS.flagValue, TEST_DISKLESS_SP_NAME);

        evaluateTest(
            new ToggleDiskCall(
                ToggleOp.INTO_DRBD_DISKFUL,
                // addition of disk registered
                ApiConsts.MODIFIED,
                // "Added disk on ..."
                ApiConsts.MODIFIED,
                // updated resync-after entries
                ApiConsts.MASK_INFO
            )
                .setStorPoolName(TEST_SP_NAME)
        );

        Resource rsc = testSatelliteNode.getResource(testRscName);
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DISK_ADD_REQUESTED)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS)).isFalse();
    }

    /*
     * helpers
     */

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

    private class ToggleDiskCall extends AbsApiCallTester
    {
        private final ToggleOp toggleOp;
        private String nodeName;
        private String rscName;
        private String storPoolName;
        private String migrateFrom;

        ToggleDiskCall(ToggleOp toggleOpRef, long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            toggleOp = toggleOpRef;
            nodeName = TEST_NODE_NAME;
            rscName = TEST_RSC_NAME;
            storPoolName = null;
            migrateFrom = null;
        }

        ToggleDiskCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        ToggleDiskCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        ToggleDiskCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        ToggleDiskCall setMigrateFrom(String migrateFromRef)
        {
            migrateFrom = migrateFromRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscToggleDiskApiCallHandlerProvider.get().resourceToggleDisk(
                nodeName,
                rscName,
                storPoolName,
                migrateFrom,
                null,
                toggleOp,
                null
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
