package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscActivateApiCallHandler;
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
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class RscActivateApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_NODE_B = "TestNodeB";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_SHARED_SP_NAME = "TestSharedStorPool";

    @Inject
    private Provider<CtrlRscActivateApiCallHandler> rscActivateApiCallHandlerProvider;
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
    private final StorPoolName testSharedStorPoolName;

    private Node testNodeA;
    private Node testNodeB;
    private SatelliteState satelliteStateA;

    public RscActivateApiTest() throws Exception
    {
        testNodeAName = new NodeName(TEST_NODE_A);
        testNodeBName = new NodeName(TEST_NODE_B);
        testRscName = new ResourceName(TEST_RSC_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
        testSharedStorPoolName = new StorPoolName(TEST_SHARED_SP_NAME);
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

        // the shared storage pool has to be created while still in the setup scope; a later
        // storPoolDfnMap.put would cascade a fresh transaction manager onto the map values, and the
        // default diskless storage pool definition of the test harness holds a stale transaction
        // manager after setup (it is registered twice via equal instances, so it is never cleared)
        createStorPoolOnNode(testNodeA, testSharedStorPoolName, new SharedStorPoolName(TEST_SHARED_SP_NAME));
        createStorPoolOnNode(testNodeB, testSharedStorPoolName, new SharedStorPoolName(TEST_SHARED_SP_NAME));

        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 0)
            .setSize(100 * 1024L)
            .build();

        leaveScope();
    }

    private Node createSatelliteNode(NodeName nodeName, Peer peer) throws Exception
    {
        Node node = nodeFactory.create(nodeName, Node.Type.SATELLITE, null);
        node.setPeer(peer);
        nodesMap.put(nodeName, node);

        createStorPoolOnNode(node, testStorPoolName, new SharedStorPoolName(nodeName, testStorPoolName));

        return node;
    }

    private StorPool createStorPoolOnNode(
        Node node,
        StorPoolName storPoolName,
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
            DeviceProviderKind.LVM,
            fsm,
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        return storPool;
    }

    private void createResourceOnNode(String nodeName, String storPoolName) throws Exception
    {
        enterScope();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolName);
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

    private Resource getRscOnNodeA() throws Exception
    {
        return testNodeA.getResource(testRscName);
    }

    private void setRscFlags(Resource rsc, Resource.Flags... flags) throws Exception
    {
        enterScope();
        rsc.getStateFlags().enableFlags(flags);
        leaveScope();
    }

    private void unsetRscFlags(Resource rsc, Resource.Flags... flags) throws Exception
    {
        enterScope();
        rsc.getStateFlags().disableFlags(flags);
        leaveScope();
    }

    @Test
    public void deactivateSuccess() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        evaluateTest(
            new DeactivateRscCall(
                // "Resource deactivated on ..."
                ApiConsts.MODIFIED,
                // "Finished deactivation of resource on ..."
                ApiConsts.MODIFIED
            )
        );

        Resource rsc = getRscOnNodeA();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVATING)).isFalse();
    }

    @Test
    public void deactivateAlreadyInactive() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        evaluateTest(
            new DeactivateRscCall(
                ApiConsts.MODIFIED,
                ApiConsts.MODIFIED
            )
        );

        evaluateTest(
            new DeactivateRscCall(ApiConsts.INFO_NOOP)
        );

        assertThat(getRscOnNodeA().getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void activateAlreadyActive() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        evaluateTest(
            new ActivateRscCall(ApiConsts.INFO_NOOP)
        );

        assertThat(getRscOnNodeA().getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
    }

    @Test
    public void deactivateInUse() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        satelliteStateA.setOnResource(testRscName, SatelliteResourceState::setInUse, Boolean.TRUE);

        evaluateTest(
            new DeactivateRscCall(ApiConsts.FAIL_IN_USE)
        );

        assertThat(getRscOnNodeA().getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
    }

    @Test
    public void activateAfterDeactivate() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        evaluateTest(
            new DeactivateRscCall(
                ApiConsts.MODIFIED,
                ApiConsts.MODIFIED
            )
        );

        evaluateTest(
            new ActivateRscCall(
                // "Reactivating resource on ..."
                ApiConsts.MODIFIED,
                // "Resource activated on ..."
                ApiConsts.MODIFIED
            )
        );

        Resource rsc = getRscOnNodeA();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.INACTIVE)).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.REACTIVATE)).isFalse();
    }

    @Test
    public void activateInactivePermanently() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        setRscFlags(getRscOnNodeA(), Resource.Flags.INACTIVE, Resource.Flags.INACTIVE_PERMANENTLY);

        evaluateTest(
            new ActivateRscCall(ApiConsts.FAIL_INVLD_LAYER_STACK)
        );

        assertThat(getRscOnNodeA().getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void activateBlockedBySharedStorPool() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SHARED_SP_NAME);
        createResourceOnNode(TEST_NODE_B, TEST_SHARED_SP_NAME);

        // ensure the resource on node A is inactive while the one on node B is active
        setRscFlags(getRscOnNodeA(), Resource.Flags.INACTIVE);
        unsetRscFlags(testNodeB.getResource(testRscName), Resource.Flags.INACTIVE);

        evaluateTest(
            new ActivateRscCall(ApiConsts.FAIL_ONLY_ONE_ACT_RSC_PER_SHARED_STOR_POOL_ALLOWED)
        );

        assertThat(getRscOnNodeA().getStateFlags().isSet(Resource.Flags.INACTIVE)).isTrue();
    }

    @Test
    public void activateUnknownRsc() throws Exception
    {
        evaluateTest(
            new ActivateRscCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
        );
    }

    @Test
    public void activateUnknownNode() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        evaluateTest(
            new ActivateRscCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void deactivateRscNotOnNode() throws Exception
    {
        createResourceOnNode(TEST_NODE_A, TEST_SP_NAME);

        evaluateTest(
            new DeactivateRscCall(ApiConsts.FAIL_NOT_FOUND_RSC)
                .setNodeName(TEST_NODE_B)
        );
    }

    private abstract class AbsActivateRscCall extends AbsApiCallTester
    {
        protected String nodeName;
        protected String rscName;

        AbsActivateRscCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            nodeName = TEST_NODE_A;
            rscName = TEST_RSC_NAME;
        }

        AbsActivateRscCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        AbsActivateRscCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        protected ApiCallRc collect(Flux<ApiCallRc> flux)
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            flux
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }

    private class ActivateRscCall extends AbsActivateRscCall
    {
        ActivateRscCall(long... expectedRcs)
        {
            super(expectedRcs);
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return collect(rscActivateApiCallHandlerProvider.get().activateRsc(nodeName, rscName));
        }
    }

    private class DeactivateRscCall extends AbsActivateRscCall
    {
        DeactivateRscCall(long... expectedRcs)
        {
            super(expectedRcs);
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return collect(rscActivateApiCallHandlerProvider.get().deactivateRsc(nodeName, rscName));
        }
    }
}
