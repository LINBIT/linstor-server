package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnDeleteApiCallHandler;
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
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
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
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class VlmDfnDeleteApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_NODE_B = "TestNodeB";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final int TEST_VLM_NR = 0;

    @Inject
    private Provider<CtrlVlmDfnDeleteApiCallHandler> vlmDfnDeleteApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

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
    private final StorPoolName testStorPoolName;
    private final VolumeNumber testVlmNr;

    private Node testNodeA;
    private Node testNodeB;
    private ResourceDefinition testRscDfn;
    private SatelliteState satelliteStateA;

    public VlmDfnDeleteApiTest() throws Exception
    {
        testNodeAName = new NodeName(TEST_NODE_A);
        testNodeBName = new NodeName(TEST_NODE_B);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
        testVlmNr = new VolumeNumber(TEST_VLM_NR);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        stubAllExtToolsSupported(mockExtToolsMgr);

        satelliteStateA = new SatelliteState();

        stubSatellitePeer(mockSatelliteA, mockExtToolsMgr, satelliteStateA, true);
        stubSatellitePeer(mockSatelliteB, mockExtToolsMgr, new SatelliteState(), true);

        testNodeA = createSatelliteNode(testNodeAName, mockSatelliteA);
        testNodeB = createSatelliteNode(testNodeBName, mockSatelliteB);

        testRscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(testRscDfn.getName(), testRscDfn);
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, TEST_VLM_NR)
            .setSize(100 * 1024L)
            .build();

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
        enterScope();

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

        leaveScope();
    }

    private VolumeDefinition getVlmDfn() throws Exception
    {
        return testRscDfn.getVolumeDfn(testVlmNr);
    }

    @Test
    public void delNoResources() throws Exception
    {
        evaluateTest(
            new DeleteVlmDfnCall(
                // volume definition marked for deletion
                ApiConsts.DELETED,
                // updated resync-after entries (auto helper)
                ApiConsts.MASK_INFO,
                // volume definition deleted
                ApiConsts.DELETED
            )
        );

        assertThat(getVlmDfn()).isNull();
        assertThat(rscDfnMap.get(testRscName)).isNotNull();
    }

    @Test
    public void delWithResourcesOnlineSatellites() throws Exception
    {
        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        evaluateTest(
            new DeleteVlmDfnCall(
                // volume definition marked for deletion
                ApiConsts.DELETED,
                // updated resync-after entries (auto helper)
                ApiConsts.MASK_INFO,
                // "Deleted volume 0 of ... on ..." per node
                ApiConsts.MODIFIED,
                ApiConsts.MODIFIED,
                // volume definition deleted
                ApiConsts.DELETED
            )
        );

        assertThat(getVlmDfn()).isNull();
        assertThat(testNodeA.getResource(testRscName).getVolume(testVlmNr)).isNull();
        assertThat(testNodeB.getResource(testRscName).getVolume(testVlmNr)).isNull();
    }

    @Test
    public void delWithResourcesOfflineSatellites() throws Exception
    {
        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        setSatelliteOnline(mockSatelliteA, false);
        setSatelliteOnline(mockSatelliteB, false);

        // characterization: unlike resource deletion, volume definition deletion does not wait for
        // the satellites to confirm the undeploy; the offline satellites merely produce warnings and
        // the volume definition is removed from the database right away
        evaluateTest(
            new DeleteVlmDfnCall(
                // volume definition marked for deletion
                ApiConsts.DELETED,
                // updated resync-after entries (auto helper)
                ApiConsts.MASK_INFO,
                // satellite A is offline, still reported as deleted
                ApiConsts.WARN_NOT_CONNECTED,
                ApiConsts.MODIFIED,
                // satellite B is offline, still reported as deleted
                ApiConsts.WARN_NOT_CONNECTED,
                ApiConsts.MODIFIED,
                // volume definition deleted
                ApiConsts.DELETED
            )
        );

        assertThat(getVlmDfn()).isNull();
    }

    @Test
    public void delRscInUse() throws Exception
    {
        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        satelliteStateA.setOnResource(testRscName, SatelliteResourceState::setInUse, Boolean.TRUE);

        evaluateTest(
            new DeleteVlmDfnCall(
                ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.FAIL_IN_USE
            )
        );

        VolumeDefinition vlmDfn = getVlmDfn();
        assertThat(vlmDfn).isNotNull();
        assertThat(vlmDfn.getFlags().isSet(VolumeDefinition.Flags.DELETE)).isFalse();
        Volume vlm = testNodeA.getResource(testRscName).getVolume(testVlmNr);
        assertThat(vlm).isNotNull();
        assertThat(vlm.getFlags().isSet(Volume.Flags.DELETE)).isFalse();
    }

    @Test
    public void delRscOpenWhileSecondary() throws Exception
    {
        createResourceOnNode(TEST_NODE_A);
        createResourceOnNode(TEST_NODE_B);

        // DRBD device opened (e.g. read-only) without being Primary
        satelliteStateA.setOnResource(testRscName, SatelliteResourceState::setInUse, Boolean.FALSE);
        satelliteStateA.setOnResource(testRscName, SatelliteResourceState::setOpen, Boolean.TRUE);

        evaluateTest(
            new DeleteVlmDfnCall(
                ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.FAIL_IN_USE
            )
        );

        VolumeDefinition vlmDfn = getVlmDfn();
        assertThat(vlmDfn).isNotNull();
        assertThat(vlmDfn.getFlags().isSet(VolumeDefinition.Flags.DELETE)).isFalse();
    }

    @Test
    public void delUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new DeleteVlmDfnCall(ApiConsts.WARN_NOT_FOUND)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void delUnknownVlmNr() throws Exception
    {
        evaluateTest(
            new DeleteVlmDfnCall(ApiConsts.WARN_NOT_FOUND)
                .setVlmNr(4)
        );

        assertThat(getVlmDfn()).isNotNull();
    }

    private class DeleteVlmDfnCall extends AbsApiCallTester
    {
        private String rscName;
        private int vlmNr;

        DeleteVlmDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM_DFN,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            rscName = TEST_RSC_NAME;
            vlmNr = TEST_VLM_NR;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            vlmDfnDeleteApiCallHandlerProvider.get().deleteVolumeDefinition(rscName, vlmNr)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        DeleteVlmDfnCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        DeleteVlmDfnCall setVlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
            return this;
        }
    }
}
