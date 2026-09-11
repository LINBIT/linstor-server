package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlQueryMaxVlmSizeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

/**
 * Exercises the query-size-info and query-max-volume-size APIs (CtrlQuerySizeInfoHelper and
 * CtrlQueryMaxVlmSizeHelper) through the resource group API handlers.
 *
 * The setup defines three mocked online satellites with LVM ("pool1", thick) storage pools and two of
 * them additionally with LVM_THIN ("thinpool") storage pools:
 *
 * <pre>
 *          pool1 (LVM)          thinpool (LVM_THIN)  sharedpool (LVM)      sharedonlypool (LVM)
 * NodeA    free 10000/cap 20000  free 4000/cap 5000  free 10000/cap 20000  free 10000/cap 20000
 * NodeB    free  8000/cap 20000  free 2000/cap 5000  free 10000/cap 20000  free 10000/cap 20000
 * NodeC    free  6000/cap 12000  -                   free  6000/cap 12000  -
 * </pre>
 *
 * The "sharedpool" and "sharedonlypool" pools of NodeA and NodeB are backed by the same shared
 * storage (same shared stor pool name), NodeC's "sharedpool" has its own storage.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class QuerySizeInfoApiTest extends ApiTestBase
{
    private static final String TEST_RSC_GRP_NAME = "TestRscGrp";
    private static final String NODE_A = "QsiNodeA";
    private static final String NODE_B = "QsiNodeB";
    private static final String NODE_C = "QsiNodeC";
    private static final String THICK_POOL = "pool1";
    private static final String THIN_POOL = "thinpool";
    private static final String SHARED_POOL = "sharedpool";
    private static final String SHARED_ONLY_POOL = "sharedonlypool";

    // LVM_THIN pools are 20x oversubscribed by default
    private static final long THIN_OVERSUBSCRIPTION_RATIO = 20L;

    @Inject
    private Provider<CtrlRscGrpApiCallHandler> rscGrpApiCallHandlerProvider;
    @Inject
    private Provider<CtrlQueryMaxVlmSizeApiCallHandler> qmvsApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final ResourceGroupName testRscGrpName;
    private ResourceGroup testRscGrp;

    public QuerySizeInfoApiTest() throws Exception
    {
        testRscGrpName = new ResourceGroupName(TEST_RSC_GRP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenReturn(Mono.just(Collections.emptyMap()));

        stubAllExtToolsSupported(mockExtToolsMgr);

        testRscGrp = resourceGroupTestFactory.create(TEST_RSC_GRP_NAME);
        rscGrpMap.put(testRscGrpName, testRscGrp);

        // all storage pool definitions have to be created within the setUp scope
        StorPoolDefinition thickPoolDfn = storPoolDefinitionFactory.create(new StorPoolName(THICK_POOL));
        storPoolDfnMap.put(thickPoolDfn.getName(), thickPoolDfn);
        StorPoolDefinition thinPoolDfn = storPoolDefinitionFactory.create(new StorPoolName(THIN_POOL));
        storPoolDfnMap.put(thinPoolDfn.getName(), thinPoolDfn);

        Node nodeA = createSatelliteNode(NODE_A);
        Node nodeB = createSatelliteNode(NODE_B);
        Node nodeC = createSatelliteNode(NODE_C);

        createStorPool(nodeA, thickPoolDfn, DeviceProviderKind.LVM, 10_000, 20_000);
        createStorPool(nodeB, thickPoolDfn, DeviceProviderKind.LVM, 8_000, 20_000);
        createStorPool(nodeC, thickPoolDfn, DeviceProviderKind.LVM, 6_000, 12_000);

        createStorPool(nodeA, thinPoolDfn, DeviceProviderKind.LVM_THIN, 4_000, 5_000);
        createStorPool(nodeB, thinPoolDfn, DeviceProviderKind.LVM_THIN, 2_000, 5_000);

        // "sharedpool" is backed by the same shared storage on nodes A and B (same shared stor
        // pool name), node C has its own storage
        StorPoolDefinition sharedPoolDfn = storPoolDefinitionFactory.create(new StorPoolName(SHARED_POOL));
        storPoolDfnMap.put(sharedPoolDfn.getName(), sharedPoolDfn);
        SharedStorPoolName sharedName = new SharedStorPoolName("SharedAB");
        createStorPool(nodeA, sharedPoolDfn, DeviceProviderKind.LVM, sharedName, 10_000, 20_000);
        createStorPool(nodeB, sharedPoolDfn, DeviceProviderKind.LVM, sharedName, 10_000, 20_000);
        createStorPool(nodeC, sharedPoolDfn, DeviceProviderKind.LVM, 6_000, 12_000);

        // "sharedonlypool" only exists as shared storage on nodes A and B
        StorPoolDefinition sharedOnlyPoolDfn =
            storPoolDefinitionFactory.create(new StorPoolName(SHARED_ONLY_POOL));
        storPoolDfnMap.put(sharedOnlyPoolDfn.getName(), sharedOnlyPoolDfn);
        SharedStorPoolName sharedOnlyName = new SharedStorPoolName("SharedAbOnly");
        createStorPool(nodeA, sharedOnlyPoolDfn, DeviceProviderKind.LVM, sharedOnlyName, 10_000, 20_000);
        createStorPool(nodeB, sharedOnlyPoolDfn, DeviceProviderKind.LVM, sharedOnlyName, 10_000, 20_000);

        leaveScope();
    }

    private Node createSatelliteNode(String nodeNameStr) throws Exception
    {
        Node node = nodeFactory.create(new NodeName(nodeNameStr), Node.Type.SATELLITE, null);

        Peer mockedPeer = Mockito.mock(Peer.class);
        stubSatellitePeer(mockedPeer, mockExtToolsMgr, new SatelliteState(), true);

        node.setPeer(mockedPeer);
        nodesMap.put(node.getName(), node);
        return node;
    }

    private void createStorPool(
        Node node,
        StorPoolDefinition storPoolDfn,
        DeviceProviderKind kind,
        long freeSpace,
        long capacity
    )
        throws Exception
    {
        createStorPool(
            node,
            storPoolDfn,
            kind,
            new SharedStorPoolName(node.getName(), storPoolDfn.getName()),
            freeSpace,
            capacity
        );
    }

    private void createStorPool(
        Node node,
        StorPoolDefinition storPoolDfn,
        DeviceProviderKind kind,
        SharedStorPoolName sharedStorPoolName,
        long freeSpace,
        long capacity
    )
        throws Exception
    {
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            kind,
            freeSpaceMgrFactory.getInstance(sharedStorPoolName),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(freeSpace, capacity);
    }

    private QuerySizeInfoResponsePojo querySizeInfo(QuerySizeInfoRequestPojo req)
    {
        List<ApiCallRcWith<QuerySizeInfoResponsePojo>> responses = querySizeInfoRaw(req);
        assertThat(responses).hasSize(1);
        return responses.get(0).getValue();
    }

    private List<ApiCallRcWith<QuerySizeInfoResponsePojo>> querySizeInfoRaw(QuerySizeInfoRequestPojo req)
    {
        return rscGrpApiCallHandlerProvider.get()
            .querySizeInfo(req)
            .contextWrite(contextWrite())
            .toStream()
            .collect(Collectors.toList());
    }

    private List<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSizeByRscGrp(String rscGrpName)
    {
        return rscGrpApiCallHandlerProvider.get()
            .queryMaxVlmSize(rscGrpName)
            .contextWrite(contextWrite())
            .toStream()
            .collect(Collectors.toList());
    }

    private List<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSizeByFilter(
        AutoSelectFilterPojo filter
    )
    {
        return qmvsApiCallHandlerProvider.get()
            .queryMaxVlmSize(filter)
            .contextWrite(contextWrite())
            .toStream()
            .collect(Collectors.toList());
    }

    private static QuerySizeInfoRequestPojo qsiRequest(AutoSelectFilterPojo filter)
    {
        return qsiRequest(filter, 0);
    }

    private static QuerySizeInfoRequestPojo qsiRequest(AutoSelectFilterPojo filter, int ignoreCacheOlderThanSec)
    {
        return new QuerySizeInfoRequestPojo(TEST_RSC_GRP_NAME, filter, ignoreCacheOlderThanSec);
    }

    private static QuerySizeInfoRequestPojo qsiRequestCanonical(AutoSelectFilterPojo filter, int ignoreCacheOlderThanSec)
    {
        return new QuerySizeInfoRequestPojo(
            TEST_RSC_GRP_NAME.toUpperCase(),
            filter,
            ignoreCacheOlderThanSec
        );
    }

    private static AutoSelectFilterPojo filter(Integer placeCount, String... storPoolNames)
    {
        AutoSelectFilterBuilder builder = new AutoSelectFilterBuilder()
            .setPlaceCount(placeCount);
        if (storPoolNames.length > 0)
        {
            builder.setStorPoolNameList(new ArrayList<>(Arrays.asList(storPoolNames)));
        }
        return builder.build();
    }

    private static List<String> spNodeNames(QuerySizeInfoResponsePojo pojo)
    {
        return pojo.nextSpawnSpList().stream()
            .map(StorPoolApi::getNodeName)
            .collect(Collectors.toList());
    }

    /*
     * query size info tests (CtrlQuerySizeInfoHelper, flux based, must not run within the testScope)
     */

    @Test
    public void qsiThickPools() throws Exception
    {
        QuerySizeInfoResponsePojo pojo = querySizeInfo(qsiRequest(filter(2, THICK_POOL)));

        // the autoplacer selects the two pools with the most free space (NodeA and NodeB); the max
        // volume size is limited by the smaller of the two
        assertThat(pojo.getMaxVlmSize()).isEqualTo(8_000);
        // total available: 8000 on A+B, then the remaining 2000 on A combined with 6000 on C -> 10000
        assertThat(pojo.getAvailableSize()).isEqualTo(10_000);
        // capacity: two 20000 pools can hold 20000 twice
        assertThat(pojo.getCapacity()).isEqualTo(20_000);

        assertThat(pojo.nextSpawnSpList())
            .allSatisfy(sp -> assertThat(sp.getStorPoolName()).isEqualTo(THICK_POOL));
        assertThat(spNodeNames(pojo)).containsExactlyInAnyOrder(NODE_A, NODE_B);
    }

    @Test
    public void qsiThinPools() throws Exception
    {
        QuerySizeInfoResponsePojo pojo = querySizeInfo(qsiRequest(filter(2, THIN_POOL)));

        // thin pools are oversubscribed: min(free, capacity) * ratio of the smaller pool (NodeB)
        assertThat(pojo.getMaxVlmSize()).isEqualTo(2_000 * THIN_OVERSUBSCRIPTION_RATIO);
        // available and capacity are calculated without the oversubscription ratio
        assertThat(pojo.getAvailableSize()).isEqualTo(2_000);
        assertThat(pojo.getCapacity()).isEqualTo(5_000);

        assertThat(pojo.nextSpawnSpList())
            .allSatisfy(sp -> assertThat(sp.getStorPoolName()).isEqualTo(THIN_POOL));
        assertThat(spNodeNames(pojo)).containsExactlyInAnyOrder(NODE_A, NODE_B);
    }

    @Test
    public void qsiNodeNameFilter() throws Exception
    {
        AutoSelectFilterPojo selectFilter = AutoSelectFilterPojo.merge(
            new AutoSelectFilterBuilder()
                .setPlaceCount(2)
                .setStorPoolNameList(new ArrayList<>(Arrays.asList(THICK_POOL)))
                .setNodeNameList(new ArrayList<>(Arrays.asList(NODE_A, NODE_C)))
                .build()
        );

        QuerySizeInfoResponsePojo pojo = querySizeInfo(qsiRequest(selectFilter));

        assertThat(pojo.getMaxVlmSize()).isEqualTo(6_000);
        assertThat(pojo.getAvailableSize()).isEqualTo(6_000);
        assertThat(pojo.getCapacity()).isEqualTo(12_000);
        assertThat(spNodeNames(pojo)).containsExactlyInAnyOrder(NODE_A, NODE_C);
    }

    @Test
    public void qsiNotEnoughNodes() throws Exception
    {
        // only three nodes have the requested storage pool, a replica count of 4 cannot be fulfilled
        QuerySizeInfoResponsePojo pojo = querySizeInfo(qsiRequest(filter(4, THICK_POOL)));

        assertThat(pojo.getMaxVlmSize()).isEqualTo(0);
        assertThat(pojo.getAvailableSize()).isEqualTo(0);
        assertThat(pojo.getCapacity()).isEqualTo(0);
        assertThat(pojo.nextSpawnSpList()).isEmpty();
    }

    @Test
    public void qsiSharedStorPoolsCountOnce() throws Exception
    {
        QuerySizeInfoResponsePojo pojo = querySizeInfo(qsiRequest(filter(2, SHARED_POOL)));

        // the pools of A and B are backed by the same shared storage, so the autoplacer must not
        // choose them together although they report the most free space; the only valid placement
        // is one of A/B plus C
        assertThat(pojo.getMaxVlmSize()).isEqualTo(6_000);
        assertThat(spNodeNames(pojo)).hasSize(2).contains(NODE_C).containsAnyOf(NODE_A, NODE_B);
    }

    @Test
    public void qsiOnlySharedStorPoolsCannotFulfillPlaceCount() throws Exception
    {
        // both pools are backed by the same shared storage, a replica count of 2 cannot be fulfilled
        QuerySizeInfoResponsePojo pojo = querySizeInfo(qsiRequest(filter(2, SHARED_ONLY_POOL)));

        assertThat(pojo.getMaxVlmSize()).isEqualTo(0);
        assertThat(pojo.getAvailableSize()).isEqualTo(0);
        // the capacity calculation is not shared-storage aware: it counts the pools of A and B
        // although they are backed by the same storage
        assertThat(pojo.getCapacity()).isEqualTo(20_000);
        assertThat(pojo.nextSpawnSpList()).isEmpty();
    }

    @Test
    public void qsiUnknownRscGrp() throws Exception
    {
        assertThatThrownBy(
            () -> querySizeInfoRaw(
                new QuerySizeInfoRequestPojo("UnknownRscGrp", filter(2, THICK_POOL), 0)
            )
        )
            .isInstanceOf(ApiRcException.class)
            .satisfies(
                exc -> assertThat(((ApiRcException) exc).getApiCallRc().get(0).getReturnCode())
                    .isEqualTo(ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
            );
    }

    @Test
    public void qsiCachedResponse() throws Exception
    {
        QuerySizeInfoResponsePojo first = querySizeInfo(qsiRequest(filter(2, THICK_POOL), 60));

        // an equal request within the cache age limit is answered from the cache,
        // regardless of the casing of the requested resource group name
        QuerySizeInfoResponsePojo second = querySizeInfo(qsiRequest(filter(2, THICK_POOL), 60));
        assertThat(second).isSameAs(first);
        QuerySizeInfoResponsePojo canonical = querySizeInfo(qsiRequestCanonical(filter(2, THICK_POOL), 60));
        assertThat(canonical).isSameAs(first);

        // a request with a different filter bypasses the cache ...
        QuerySizeInfoResponsePojo otherFilter = querySizeInfo(qsiRequest(filter(1, THICK_POOL), 60));
        assertThat(otherFilter).isNotSameAs(first);
        assertThat(otherFilter.getMaxVlmSize()).isEqualTo(10_000);

        // ... and so does a request that does not allow cached answers
        QuerySizeInfoResponsePojo uncached = querySizeInfo(qsiRequest(filter(2, THICK_POOL), 0));
        assertThat(uncached).isNotSameAs(first);
        assertThat(uncached.getMaxVlmSize()).isEqualTo(first.getMaxVlmSize());
    }

    @Test
    public void qsiQueryAllSizeInfo() throws Exception
    {
        QueryAllSizeInfoResponsePojo response = queryAllSizeInfo(
            new QueryAllSizeInfoRequestPojo(filter(2, THICK_POOL), 60)
        );

        // query-all iterates every resource group, including the default one
        assertThat(response.getResult()).containsOnlyKeys(TEST_RSC_GRP_NAME, "DfltRscGrp");
        QuerySizeInfoResponsePojo qsiPojo = response.getResult().get(TEST_RSC_GRP_NAME).getQsiRespPojo();
        assertThat(qsiPojo.getMaxVlmSize()).isEqualTo(8_000);
        assertThat(qsiPojo.getAvailableSize()).isEqualTo(10_000);
        assertThat(qsiPojo.getCapacity()).isEqualTo(20_000);

        // an equal request within the cache age limit is answered from the cache
        QueryAllSizeInfoResponsePojo second = queryAllSizeInfo(
            new QueryAllSizeInfoRequestPojo(filter(2, THICK_POOL), 60)
        );
        assertThat(second).isSameAs(response);
    }

    private QueryAllSizeInfoResponsePojo queryAllSizeInfo(QueryAllSizeInfoRequestPojo req)
    {
        List<QueryAllSizeInfoResponsePojo> responses = rscGrpApiCallHandlerProvider.get()
            .queryAllSizeInfo(req)
            .contextWrite(contextWrite())
            .toStream()
            .collect(Collectors.toList());
        assertThat(responses).hasSize(1);
        return responses.get(0);
    }

    /*
     * query max volume size tests (CtrlQueryMaxVlmSizeHelper, flux based, must not run within the
     * testScope)
     */

    @Test
    public void qmvsByRscGrpDefaults() throws Exception
    {
        // the resource group has no storage pool filter, so each storage pool definition becomes its
        // own candidate (with the default replica count of 2)
        List<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> responses = queryMaxVlmSizeByRscGrp(
            TEST_RSC_GRP_NAME
        );
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).hasApiCallRc()).isFalse();

        List<MaxVlmSizeCandidatePojo> candidates = responses.get(0).getValue();
        // candidates are sorted by storage pool definition name; "sharedonlypool" yields no
        // candidate since its two pools are backed by the same shared storage
        assertThat(candidates).hasSize(3);

        MaxVlmSizeCandidatePojo thickCandidate = candidates.get(0);
        assertThat(thickCandidate.getStorPoolDfnApi().getName()).isEqualTo(THICK_POOL);
        assertThat(thickCandidate.areAllThin()).isFalse();
        assertThat(thickCandidate.getNodeNames()).containsExactlyInAnyOrder(NODE_A, NODE_B);
        // the max volume size reports the lowest raw free space of the selection (no oversubscription)
        assertThat(thickCandidate.getMaxVlmSize()).isEqualTo(8_000);

        MaxVlmSizeCandidatePojo sharedCandidate = candidates.get(1);
        assertThat(sharedCandidate.getStorPoolDfnApi().getName()).isEqualTo(SHARED_POOL);
        assertThat(sharedCandidate.areAllThin()).isFalse();
        assertThat(sharedCandidate.getNodeNames()).hasSize(2).contains(NODE_C).containsAnyOf(NODE_A, NODE_B);
        assertThat(sharedCandidate.getMaxVlmSize()).isEqualTo(6_000);

        MaxVlmSizeCandidatePojo thinCandidate = candidates.get(2);
        assertThat(thinCandidate.getStorPoolDfnApi().getName()).isEqualTo(THIN_POOL);
        assertThat(thinCandidate.areAllThin()).isTrue();
        assertThat(thinCandidate.getNodeNames()).containsExactlyInAnyOrder(NODE_A, NODE_B);
        assertThat(thinCandidate.getMaxVlmSize()).isEqualTo(2_000);
    }

    @Test
    public void qmvsByRscGrpUnknown() throws Exception
    {
        assertThatThrownBy(() -> queryMaxVlmSizeByRscGrp("UnknownRscGrp"))
            .isInstanceOf(ApiRcException.class)
            .satisfies(
                exc -> assertThat(((ApiRcException) exc).getApiCallRc().get(0).getReturnCode())
                    .isEqualTo(ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
            );
    }

    @Test
    public void qmvsByFilter() throws Exception
    {
        List<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> responses = queryMaxVlmSizeByFilter(
            filter(3, THICK_POOL)
        );
        assertThat(responses).hasSize(1);

        List<MaxVlmSizeCandidatePojo> candidates = responses.get(0).getValue();
        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).getStorPoolDfnApi().getName()).isEqualTo(THICK_POOL);
        assertThat(candidates.get(0).getNodeNames()).containsExactlyInAnyOrder(NODE_A, NODE_B, NODE_C);
        assertThat(candidates.get(0).getMaxVlmSize()).isEqualTo(6_000);
    }

    @Test
    public void qmvsNoCandidates() throws Exception
    {
        // a replica count of 4 cannot be fulfilled by any storage pool definition
        List<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> responses = queryMaxVlmSizeByFilter(
            filter(4)
        );
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).getValue()).isEmpty();

        ApiCallRc apiCallRc = responses.get(0).getApiCallRc();
        assertThat(apiCallRc).hasSize(1);
        assertThat(apiCallRc.get(0).getReturnCode())
            .isEqualTo(ApiConsts.MASK_ERROR | ApiConsts.FAIL_NOT_ENOUGH_NODES);
    }

    @Test
    public void qmvsMissingReplicaCount() throws Exception
    {
        assertThatThrownBy(() -> queryMaxVlmSizeByFilter(filter(null, THICK_POOL)))
            .isInstanceOf(ApiRcException.class)
            .satisfies(
                exc -> assertThat(((ApiRcException) exc).getApiCallRc().get(0).getReturnCode())
                    .isEqualTo(ApiConsts.FAIL_INVLD_PLACE_COUNT)
            );
    }
}
