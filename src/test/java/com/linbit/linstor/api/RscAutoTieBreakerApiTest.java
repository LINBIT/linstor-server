package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiConsts.ConnectionStatus;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.apidata.RscApiData;
import com.linbit.linstor.proto.common.LayerTypeOuterClass.LayerType;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.RscOuterClass;
import com.linbit.linstor.proto.common.RscOuterClass.Rsc;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageRsc;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests the auto-tiebreaker behaviour when a resource is placed <b>manually</b> (not via the autoplacer) while the
 * resource group carries a {@code --replicas-on-same} rule that is already violated by the deployed diskful resources.
 */
@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:descendenttokencheck"})
public class RscAutoTieBreakerApiTest extends ApiTestBase
{
    private static final long KB = 1;
    private static final long MB = 1_000 * KB;
    private static final long GB = 1_000 * MB;

    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String DFLT_STOR_POOL_NAME = InternalApiConsts.DEFAULT_STOR_POOL_NAME;
    private static final String ROLE_KEY = "Aux/role";

    private static final int MINOR_NR_MIN = 1000;
    private static final AtomicInteger MINOR_GEN = new AtomicInteger(MINOR_NR_MIN);

    private static final StorPoolName DFLT_DISKLESS_STOR_POOL_NAME;

    static
    {
        try
        {
            DFLT_DISKLESS_STOR_POOL_NAME = new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME);
        }
        catch (Exception exc)
        {
            throw new RuntimeException(exc);
        }
    }

    @Inject private CtrlRscCrtApiCallHandler rscCrtApiCallHandler;
    @Inject private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    // Mocked so the create flow does not block on the resource-ready event: with more than one resource in the
    // definition, waitResourcesReady() waits up to 15s per resource for a resourceStateEvent that no (mocked)
    // satellite ever fires. Returning an empty stream lets the wait complete immediately.
    @Bind @Mock
    protected EventWaiter eventWaiter;

    private ResourceGroup dfltRscGrp;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        MINOR_GEN.set(MINOR_NR_MIN);

        dfltRscGrp = createDefaultResourceGroup();

        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any())).thenReturn(Mono.just(Collections.emptyMap()));
        Mockito.when(eventWaiter.waitForStream(any(), any())).thenReturn(Flux.empty());

        commitAndCleanUp(true);
    }

    @org.junit.After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void manualCreateDoesNotAbortWhenTieBreakerCheckIsUndecidable() throws Exception
    {
        // 2 diskful on nodeA + nodeB (role=data), and we manually add a 3rd diskful on nodeC (role=different).
        // The resulting 3 diskful nodes carry conflicting --replicas-on-same values, so building the tiebreaker
        // eligibility SelectionManager throws SelectionException. Since 3 diskful is odd, no tiebreaker is needed at
        // all - the manual create must succeed instead of being aborted by that check.
        createStlt("nodeA").withProp(ROLE_KEY, "data").build();
        createStlt("nodeB").withProp(ROLE_KEY, "data").build();
        createStlt("nodeC").withProp(ROLE_KEY, "different").build();

        createRscDfn();
        setReplicasOnSame(ROLE_KEY);

        preSeedResource("nodeA");
        preSeedResource("nodeB");

        ApiCallRc rc = manualCreateDiskful("nodeC");

        assertFalse(
            "manual create must not fail with FAIL_UNDECIDABLE_AUTOPLACMENT",
            containsRc(rc, ApiConsts.FAIL_UNDECIDABLE_AUTOPLACMENT)
        );
        assertFalse("manual create must not report any error", containsMask(rc, ApiConsts.MASK_ERROR));

        // nodeC now holds a diskful resource ...
        Resource rscOnNodeC = getResource("nodeC");
        assertNotNull("resource should have been created on nodeC", rscOnNodeC);
        assertFalse(
            "resource on nodeC must be diskful",
            rscOnNodeC.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS)
        );

        // ... and no tiebreaker was created (odd diskful count -> none needed)
        assertNull("no tiebreaker must exist for an odd diskful count", findTieBreaker());
        assertEquals("expected exactly 3 diskful resources", 3, countDiskful());
    }

    @Test
    public void tieBreakerIsPlacedByRelaxingReplicasOnSame() throws Exception
    {
        // 1 diskful pre-seeded on nodeA (role=data); we manually add a 2nd diskful on nodeB (role=different).
        // That makes 2 (even) diskful with conflicting --replicas-on-same values, so a tiebreaker is needed but the
        // autoplacer cannot honour the rule. It must relax the rule and place the tiebreaker on the only free node
        // (nodeC), emitting WARN_TIE_BREAKER_RULE_RELAXED.
        createStlt("nodeA").withProp(ROLE_KEY, "data").build();
        createStlt("nodeB").withProp(ROLE_KEY, "different").build();
        createStlt("nodeC").withProp(ROLE_KEY, "data").build();

        createRscDfn();
        setReplicasOnSame(ROLE_KEY);

        preSeedResource("nodeA");

        ApiCallRc rc = manualCreateDiskful("nodeB");

        assertFalse(
            "manual create must not fail with FAIL_UNDECIDABLE_AUTOPLACMENT",
            containsRc(rc, ApiConsts.FAIL_UNDECIDABLE_AUTOPLACMENT)
        );
        assertTrue(
            "a WARN_TIE_BREAKER_RULE_RELAXED response must be present, got:" + dump(rc),
            containsRc(rc, ApiConsts.WARN_TIE_BREAKER_RULE_RELAXED)
        );
        assertTrue(
            "an INFO_TIE_BREAKER_CREATED response must be present, got:" + dump(rc),
            containsRc(rc, ApiConsts.INFO_TIE_BREAKER_CREATED)
        );

        Resource tieBreaker = findTieBreaker();
        assertNotNull("a tiebreaker resource must have been created", tieBreaker);
        assertEquals(
            "the tiebreaker must have been placed on the only free node (nodeC)",
            "nodeC",
            tieBreaker.getNode().getName().displayValue
        );
        assertTrue(
            "the tiebreaker must be diskless",
            tieBreaker.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS)
        );
        assertEquals("expected exactly 2 diskful resources", 2, countDiskful());
    }

    // -------------------------------------------------------------------------------------------------------------
    // setup helpers
    // -------------------------------------------------------------------------------------------------------------

    private StltBuilder createStlt(String nodeNameStr)
    {
        return new StltBuilder(nodeNameStr);
    }

    /**
     * Fluent builder for a satellite node: sets arbitrary node properties and its storage pool(s) before creating
     * the node. If no storage pool is requested, a single default LVM pool ({@value #DFLT_STOR_POOL_NAME}) is added.
     * Every node also gets a diskless storage pool so a diskless tiebreaker can be placed on it.
     */
    private final class StltBuilder
    {
        private final String nodeNameStr;
        private final Map<String, String> nodeProps = new TreeMap<>();

        private StltBuilder(String nodeNameStrRef)
        {
            nodeNameStr = nodeNameStrRef;
        }

        private StltBuilder withProp(String key, String value)
        {
            nodeProps.put(key, value);
            return this;
        }

        private Node build() throws Exception
        {
            enterScope();

            Node stlt = nodeFactory.create(new NodeName(nodeNameStr), Node.Type.SATELLITE, null);
            nodesMap.put(stlt.getName(), stlt);

            stlt.setPeer(mockSatellitePeer());

            for (Entry<String, String> propEntry : nodeProps.entrySet())
            {
                stlt.getProps().setProp(propEntry.getKey(), propEntry.getValue());
            }

            // every node needs a diskless storage pool so the diskless tiebreaker can be placed on it
            createStorPool(stlt, DFLT_DISKLESS_STOR_POOL_NAME.displayValue, DeviceProviderKind.DISKLESS);

            commitAndCleanUp(true);
            addStorPool(stlt, DFLT_STOR_POOL_NAME);

            return stlt;
        }
    }

    private Peer mockSatellitePeer()
    {
        Peer peer = Mockito.mock(Peer.class);
        ExtToolsManager extToolsMgr = Mockito.mock(ExtToolsManager.class);
        // deployment does not need to actually happen for these tests
        Mockito.when(peer.apiCall(anyString(), any())).thenReturn(Flux.empty());
        Mockito.when(peer.isOnline()).thenReturn(true);
        Mockito.when(peer.getConnectionStatus()).thenReturn(ConnectionStatus.ONLINE);
        Mockito.when(peer.getExtToolsManager()).thenReturn(extToolsMgr);
        Mockito.when(peer.getSatelliteStateLock()).thenReturn(new ReentrantReadWriteLock());
        Mockito.when(peer.getSatelliteState()).thenReturn(null);

        for (DeviceLayerKind kind : DeviceLayerKind.values())
        {
            Mockito.when(extToolsMgr.isLayerSupported(kind)).thenReturn(true);
        }
        Mockito.when(extToolsMgr.getSupportedLayers())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceLayerKind.values())));
        for (DeviceProviderKind kind : DeviceProviderKind.values())
        {
            Mockito.when(extToolsMgr.isProviderSupported(kind)).thenReturn(true);
        }
        Mockito.when(extToolsMgr.getSupportedProviders())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceProviderKind.values())));
        // tiebreaker placement requires DRBD kernel >= 9.0.19
        Mockito.when(extToolsMgr.getExtToolInfo(ExtTools.DRBD9_KERNEL))
            .thenReturn(new ExtToolsInfo(ExtTools.DRBD9_KERNEL, true, 9, 0, 19, Collections.emptyList()));
        Mockito.when(extToolsMgr.getVersion(ExtTools.DRBD9_KERNEL)).thenReturn(new Version(9, 0, 19));
        return peer;
    }

    private void addStorPool(Node stlt, String storPoolNameStr) throws Exception
    {
        enterScope();
        createStorPool(stlt, storPoolNameStr, DeviceProviderKind.LVM)
            .getFreeSpaceTracker()
            .setCapacityInfo( 10 * GB, 10 * GB);
        commitAndCleanUp(true);
    }

    private StorPool createStorPool(
        Node stlt,
        String storPoolNameStr,
        DeviceProviderKind providerKind
    )
        throws Exception
    {
        StorPoolName storPoolName = new StorPoolName(storPoolNameStr);
        StorPoolDefinition storPoolDfn = storPoolDefinitionRepository.get(storPoolName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(storPoolName);
            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
        }

        return storPoolFactory.create(
            stlt,
            storPoolDfn,
            providerKind,
            freeSpaceMgrFactory.getInstance(new SharedStorPoolName(stlt.getName(), storPoolDfn.getName())),
            false
        );
    }

    private void createRscDfn() throws Exception
    {
        createRscDfn(TEST_RSC_NAME);
    }

    private void createRscDfn(String rscNameStr) throws Exception
    {
        enterScope();

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = "notTellingYou";
        drbdRscDfn.transportType = TransportType.IP;

        ResourceDefinition rscDfn = resourceDefinitionFactory.create(
            new ResourceName(rscNameStr),
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            dfltRscGrp
        );
        rscDfnMap.put(rscDfn.getName(), rscDfn);

        // enable the auto-tiebreaker feature (no default -> must be set explicitly)
        rscDfn.getProps().setProp(
            ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER,
            ApiConsts.VAL_TRUE,
            ApiConsts.NAMESPC_DRBD_OPTIONS
        );

        volumeDefinitionFactory.create(
            rscDfn,
            new VolumeNumber(0),
            MINOR_GEN.incrementAndGet(),
            100 * MB,
            null
        );

        ctrlConf.setProp(
            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
            java.util.UUID.randomUUID().toString(),
            ApiConsts.NAMESPC_CLUSTER
        );

        commitAndCleanUp(true);
    }

    private void setReplicasOnSame(String... keys) throws Exception
    {
        enterScope();
        dfltRscGrp.getAutoPlaceConfig().applyChanges(
            new AutoSelectFilterBuilder()
                .setReplicasOnSameList(Arrays.asList(keys))
                .build()
        );
        commitAndCleanUp(true);
    }

    private void preSeedResource(String nodeNameStr) throws Exception
    {
        preSeedResource(nodeNameStr, TEST_RSC_NAME, DFLT_STOR_POOL_NAME);
    }

    private void preSeedResource(String nodeNameStr, String rscNameStr, String storPoolNameStr) throws Exception
    {
        enterScope();

        Map<String, String> rscPropsMap = new TreeMap<>();
        rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);

        // Diskful/diskless is derived from the target storage pool: pinning to the default diskless pool creates a
        // diskless resource, any other pool a diskful one.
        boolean diskless = DFLT_DISKLESS_STOR_POOL_NAME.displayValue.equals(storPoolNameStr);
        long flags = diskless ? Resource.Flags.DRBD_DISKLESS.flagValue : 0L;

        // createResourceDb only registers the resource in the DB - it does NOT run the auto-helpers, which is exactly
        // what we want for pre-seeding "already deployed" resources.
        ctrlRscCrtApiHelper.createResourceDb(
            nodeNameStr,
            rscNameStr,
            flags,
            rscPropsMap,
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            diskless ? null : Resource.DiskfulBy.USER,
            false
        );

        commitAndCleanUp(true);
    }

    // -------------------------------------------------------------------------------------------------------------
    // execution + assertion helpers
    // -------------------------------------------------------------------------------------------------------------

    /**
     * Manually creates a diskful resource on the given node through the regular create-resource API handler. This is
     * the path that runs the auto-helpers (including the auto-tiebreaker).
     */
    private ApiCallRc manualCreateDiskful(String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        rscCrtApiCallHandler.createResource(
            Collections.singletonList(
                new RscWithPayloadApiData(
                    RscOuterClass.Rsc.newBuilder()
                        .setNodeName(nodeNameStr)
                        .setName(TEST_RSC_NAME)
                        .putAllProps(Collections.singletonMap(ApiConsts.KEY_STOR_POOL_NAME, DFLT_STOR_POOL_NAME))
                        .setLayerObject(
                            RscLayerData.newBuilder()
                                .setId(0)
                                .setRscNameSuffix("")
                                .setLayerType(LayerType.STORAGE)
                                .setStorage(StorageRsc.newBuilder().build())
                                .setSuspend(false)
                                .build()
                        )
                        .build()
                )
            ),
            Resource.DiskfulBy.USER,
            false,
            Collections.emptyList(),
            false
        )
            .contextWrite(contextWrite())
            .toStream()
            .forEach(apiCallRc::addEntries);
        return apiCallRc;
    }

    private static String dump(ApiCallRc rc)
    {
        StringBuilder sb = new StringBuilder();
        for (RcEntry entry : rc)
        {
            sb.append("\n  [0x")
                .append(Long.toHexString(entry.getReturnCode()))
                .append("] ")
                .append(entry.getMessage());
        }
        return sb.toString();
    }

    // return codes carry the "raw" code (severity + number) plus object/operation context masks (e.g. MASK_RSC,
    // MASK_CRT) that the API flow OR-s in. To recognize a specific code we compare only the severity bits
    // (MASK_ERROR covers the two top severity bits) and the numeric code (low 16 bits), ignoring the context masks.
    private static final long RC_IDENTITY_MASK = ApiConsts.MASK_ERROR | 0xFFFFL;

    private static boolean containsRc(ApiCallRc rc, long expectedRc)
    {
        long want = expectedRc & RC_IDENTITY_MASK;
        boolean found = false;
        for (RcEntry entry : rc)
        {
            if ((entry.getReturnCode() & RC_IDENTITY_MASK) == want)
            {
                found = true;
                break;
            }
        }
        return found;
    }

    private static boolean containsMask(ApiCallRc rc, long mask)
    {
        boolean found = false;
        for (RcEntry entry : rc)
        {
            if ((entry.getReturnCode() & mask) == mask)
            {
                found = true;
                break;
            }
        }
        return found;
    }

    private @Nullable Resource getResource(String nodeNameStr) throws Exception
    {
        return rscDfnMap.get(new ResourceName(TEST_RSC_NAME))
            .getResource(new NodeName(nodeNameStr));
    }

    private @Nullable Resource findTieBreaker() throws Exception
    {
        ResourceDefinition rscDfn = rscDfnMap.get(new ResourceName(TEST_RSC_NAME));
        Resource tieBreaker = null;
        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (rsc.getStateFlags().isSet(Resource.Flags.TIE_BREAKER))
            {
                tieBreaker = rsc;
                break;
            }
        }
        return tieBreaker;
    }

    private int countDiskful() throws Exception
    {
        ResourceDefinition rscDfn = rscDfnMap.get(new ResourceName(TEST_RSC_NAME));
        int count = 0;
        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (!rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS))
            {
                count++;
            }
        }
        return count;
    }

    // -------------------------------------------------------------------------------------------------------------
    // proto glue (mirrors RscApiTest)
    // -------------------------------------------------------------------------------------------------------------

    private static class RscWithPayloadApiData implements ResourceWithPayloadApi
    {
        private final RscApiData rscApi;
        private final @Nullable Integer drbdNodeId;
        private final List<String> layerStackList;

        RscWithPayloadApiData(Rsc rscRef)
        {
            rscApi = new RscApiData(rscRef, 0, 0);
            drbdNodeId = null;
            layerStackList = Collections.emptyList();
        }

        @Override
        public ResourceApi getRscApi()
        {
            return rscApi;
        }

        @Override
        public List<String> getLayerStack()
        {
            return layerStackList;
        }

        @Override
        public @Nullable Integer getDrbdNodeId()
        {
            return drbdNodeId;
        }

        @Override
        public @Nullable Integer getPortCount()
        {
            return null;
        }

        @Override
        public @Nullable List<Integer> getPorts()
        {
            return null;
        }

        @Override
        public @Nullable Boolean isDrbdClient()
        {
            return null;
        }
    }
}
