package com.linbit.linstor.api;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.pojo.VlmGrpPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class RscGrpApiTest extends ApiTestBase
{
    private static final String TEST_RSC_GRP_NAME = "TestRscGrp";
    private static final String TEST_DESCRIPTION = "initial description";

    /*
     * The resource group handler builds its modify context with ApiOperation.makeRegisterOperation() and its
     * delete context with ApiOperation.makeCreateOperation(). Both use MASK_CRT, so every resource group
     * response - even MODIFIED and DELETED ones - carries the MASK_CRT operation mask.
     */
    private static final long MASK_RSC_GRP_CRT = ApiConsts.MASK_RSC_GRP | ApiConsts.MASK_CRT;
    private static final long RC_RSC_GRP_CREATED = MASK_RSC_GRP_CRT | ApiConsts.CREATED;
    private static final long RC_RSC_GRP_MODIFIED = MASK_RSC_GRP_CRT | ApiConsts.MODIFIED;
    private static final long RC_RSC_GRP_DELETED = MASK_RSC_GRP_CRT | ApiConsts.DELETED;
    // volume group success entries of the create API are added without an operation mask
    private static final long RC_VLM_GRP_CREATED = ApiConsts.MASK_VLM_GRP | ApiConsts.CREATED;
    // resource group props are stored with the RSC_DFN whitelist, so prop entries carry the RSC_DFN mask
    private static final long RC_PROP_SET = ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_CRT | ApiConsts.CREATED;
    private static final long RC_PROP_DELETED = ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.DELETED;

    @Inject private Provider<CtrlRscGrpApiCallHandler> rscGrpApiCallHandlerProvider;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    private ResourceGroupName testRscGrpName;
    private ResourceGroup testRscGrp;

    public RscGrpApiTest() throws Exception
    {
        testRscGrpName = new ResourceGroupName(TEST_RSC_GRP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        testRscGrp = resourceGroupTestFactory.builder(TEST_RSC_GRP_NAME)
            .setDescription(TEST_DESCRIPTION)
            .build();
        rscGrpMap.put(testRscGrpName, testRscGrp);

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(Mockito.any()))
            .thenReturn(Mono.just(Collections.emptyMap()));

        leaveScope();
    }

    /*
     * create tests. The create API is not flux based, so the tests need to (re-)enter the testScope
     * before calling the handler directly.
     */

    @Test
    public void crtSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(RC_RSC_GRP_CREATED)
        );

        ResourceGroup created = rscGrpMap.get(new ResourceGroupName("NewRscGrp"));
        assertThat(created).isNotNull();
    }

    @Test
    public void crtWithDescription() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(RC_RSC_GRP_CREATED)
                .setDescription("some test description")
        );

        ResourceGroup created = rscGrpMap.get(new ResourceGroupName("NewRscGrp"));
        assertThat(created).isNotNull();
        assertThat(created.getDescription()).isEqualTo("some test description");
    }

    @Test
    public void crtWithProps() throws Exception
    {
        enterScope();
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new CreateRscGrpCall(
                RC_PROP_SET,
                RC_RSC_GRP_CREATED
            )
                .setProp(auxKey, "value")
        );

        ResourceGroup created = rscGrpMap.get(new ResourceGroupName("NewRscGrp"));
        assertThat(created.getProps().getProp(auxKey)).isEqualTo("value");
    }

    @Test
    public void crtWithVlmGrp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(
                RC_VLM_GRP_CREATED,
                RC_RSC_GRP_CREATED
            )
                .addVlmGrp(0)
        );

        ResourceGroup created = rscGrpMap.get(new ResourceGroupName("NewRscGrp"));
        assertThat(created.getVolumeGroups()).hasSize(1);
        assertThat(created.getVolumeGroups().get(0).getVolumeNumber().value).isEqualTo(0);
    }

    @Test
    public void crtWithSelectFilter() throws Exception
    {
        enterScope();
        // the referenced storage pool does not exist on any node, so the create API warns about it
        evaluateTest(
            new CreateRscGrpCall(
                ApiConsts.WARN_NOT_FOUND,
                RC_RSC_GRP_CREATED
            )
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(2)
                        .setStorPoolNameList(new ArrayList<>(Arrays.asList("unknownPool")))
                        .build()
                )
        );

        ResourceGroup created = rscGrpMap.get(new ResourceGroupName("NewRscGrp"));
        assertThat(created.getAutoPlaceConfig().getReplicaCount()).isEqualTo(2);
        assertThat(created.getAutoPlaceConfig().getStorPoolNameList()).containsExactly("unknownPool");
    }

    @Test
    public void crtSecondExists() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_EXISTS_RSC_GRP)
                .setRscGrpName(TEST_RSC_GRP_NAME)
        );
    }

    @Test
    public void crtInvalidName() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_RSC_GRP_NAME)
                .setRscGrpName("Invalid Name") // blank is not allowed
        );
    }

    @Test
    public void crtInvalidPlaceCount() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(0)
                        .build()
                )
        );
    }

    @Test
    public void crtPlaceCountAboveDrbdLimit() throws Exception
    {
        enterScope();
        // 60 replicas can never be deployed: DRBD only has node ids 0-31, and an unset
        // layer stack defaults to a DRBD based one
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(60)
                        .build()
                )
        );
        assertThat(rscGrpMap.get(new ResourceGroupName("NewRscGrp"))).isNull();
    }

    @Test
    public void crtPlaceCountJustAboveDrbdLimit() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(CtrlRscGrpApiCallHandler.MAX_DRBD_REPLICAS + 1)
                        .build()
                )
        );
        assertThat(rscGrpMap.get(new ResourceGroupName("NewRscGrp"))).isNull();
    }

    @Test
    public void crtPlaceCountAtDrbdLimit() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(RC_RSC_GRP_CREATED)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(CtrlRscGrpApiCallHandler.MAX_DRBD_REPLICAS)
                        .build()
                )
        );
        assertThat(rscGrpMap.get(new ResourceGroupName("NewRscGrp"))).isNotNull();
    }

    @Test
    public void crtPlaceCountAboveDrbdLimitExplicitDrbdStack() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(60)
                        .setLayerStackList(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE))
                        .build()
                )
        );
        assertThat(rscGrpMap.get(new ResourceGroupName("NewRscGrp"))).isNull();
    }

    @Test
    public void crtPlaceCountAboveDrbdLimitStorageOnly() throws Exception
    {
        enterScope();
        // without DRBD in the layer stack the DRBD node id limit does not apply
        evaluateTest(
            new CreateRscGrpCall(RC_RSC_GRP_CREATED)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(60)
                        .setLayerStackList(Collections.singletonList(DeviceLayerKind.STORAGE))
                        .build()
                )
        );
        assertThat(rscGrpMap.get(new ResourceGroupName("NewRscGrp")).getAutoPlaceConfig().getReplicaCount())
            .isEqualTo(60);
    }

    @Test
    public void crtInvalidProp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PROP)
                .setProp("ThisIsNotAWhitelistedKey", "value")
        );
    }

    @Test
    public void crtExactSizePropNotAllowed() throws Exception
    {
        enterScope();
        // DrbdOptions/ExactSize is only allowed on resource definition level
        evaluateTest(
            new CreateRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PROP)
                .setProp(
                    ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_EXACT_SIZE,
                    "true"
                )
        );
    }

    /*
     * modify tests (flux based, must not run within the testScope)
     */

    @Test
    public void modDescription() throws Exception
    {
        evaluateTest(
            new ModifyRscGrpCall(RC_RSC_GRP_MODIFIED)
                .description("new description")
        );
        assertThat(testRscGrp.getDescription()).isEqualTo("new description");
    }

    @Test
    public void modProps() throws Exception
    {
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new ModifyRscGrpCall(
                RC_PROP_SET,
                RC_RSC_GRP_MODIFIED
            )
                .overrideProps(auxKey, "value")
        );
        assertThat(testRscGrp.getProps().getProp(auxKey)).isEqualTo("value");

        evaluateTest(
            new ModifyRscGrpCall(
                RC_PROP_DELETED,
                RC_RSC_GRP_MODIFIED
            )
                .deleteProp(auxKey)
        );
        assertThat(testRscGrp.getProps().getProp(auxKey)).isNull();
    }

    @Test
    public void modUnknownRscGrp() throws Exception
    {
        // the modify API does not take a UUID, so there is no FAIL_UUID_RSC_GRP path to test here
        evaluateTest(
            new ModifyRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .rscGrpName("UnknownRscGrp")
        );
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    @Test
    public void modInvalidPlaceCount() throws Exception
    {
        evaluateTest(
            new ModifyRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(0)
                        .build()
                )
        );
    }

    @Test
    public void modPlaceCountAboveDrbdLimit() throws Exception
    {
        int placeCountBefore = testRscGrp.getAutoPlaceConfig().getReplicaCount();
        evaluateTest(
            new ModifyRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(60)
                        .build()
                )
        );
        // the rejected modify must not have changed the stored place count
        assertThat(testRscGrp.getAutoPlaceConfig().getReplicaCount()).isEqualTo(placeCountBefore);
    }

    @Test
    public void modPlaceCountAboveDrbdLimitStorageOnly() throws Exception
    {
        // switching to a storage-only stack and a place count above the DRBD limit in the
        // same call is legal: the new stack governs
        evaluateTest(
            new ModifyRscGrpCall(RC_RSC_GRP_MODIFIED)
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(60)
                        .setLayerStackList(Collections.singletonList(DeviceLayerKind.STORAGE))
                        .build()
                )
        );
        assertThat(testRscGrp.getAutoPlaceConfig().getReplicaCount()).isEqualTo(60);
    }

    @Test
    public void modAddingDrbdChecksExistingPlaceCount() throws Exception
    {
        // legal while the stack is storage-only ...
        evaluateTest(
            new ModifyRscGrpCall(RC_RSC_GRP_MODIFIED)
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(60)
                        .setLayerStackList(Collections.singletonList(DeviceLayerKind.STORAGE))
                        .build()
                )
        );
        // ... but adding DRBD back to the stack makes the stored place count impossible
        evaluateTest(
            new ModifyRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_PLACE_COUNT)
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setLayerStackList(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE))
                        .build()
                )
        );
        assertThat(testRscGrp.getAutoPlaceConfig().getLayerStackList())
            .containsExactly(DeviceLayerKind.STORAGE);
    }

    @Test
    public void modSelectFilter() throws Exception
    {
        // the referenced storage pool does not exist on any node, hence the additional warning
        evaluateTest(
            new ModifyRscGrpCall(
                MASK_RSC_GRP_CRT | ApiConsts.WARN_NOT_FOUND,
                RC_RSC_GRP_MODIFIED
            )
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(2)
                        .setStorPoolNameList(new ArrayList<>(Arrays.asList("unknownPool")))
                        .build()
                )
        );
        assertThat(testRscGrp.getAutoPlaceConfig().getReplicaCount()).isEqualTo(2);
        assertThat(testRscGrp.getAutoPlaceConfig().getStorPoolNameList()).containsExactly("unknownPool");
    }

    @Test
    public void modInvalidStorPoolName() throws Exception
    {
        evaluateTest(
            new ModifyRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_STOR_POOL_NAME)
                .autoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setStorPoolNameList(new ArrayList<>(Arrays.asList("invalid pool name")))
                        .build()
                )
        );
    }

    /*
     * delete tests. The delete API is not flux based, so the tests need to (re-)enter the testScope
     * before calling the handler directly.
     */

    @Test
    public void delSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteRscGrpCall(RC_RSC_GRP_DELETED)
        );
        assertThat(rscGrpMap.get(testRscGrpName)).isNull();
    }

    @Test
    public void delUnknownRscGrp() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.WARN_NOT_FOUND)
                .rscGrpName("UnknownRscGrp")
        );
    }

    @Test
    public void delStillReferencedByRscDfn() throws Exception
    {
        enterScope();

        LayerPayload payload = new LayerPayload();
        payload.getDrbdRscDfn().sharedSecret = "notTellingYou";
        payload.getDrbdRscDfn().transportType = TransportType.IP;
        ResourceName rscName = new ResourceName("TestRsc");
        ResourceDefinition rscDfn = resourceDefinitionFactory.create(
            rscName,
            null,
            null,
            new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)),
            payload,
            testRscGrp
        );
        rscDfnMap.put(rscName, rscDfn);

        evaluateTest(
            new DeleteRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_EXISTS_RSC_DFN)
        );
        assertThat(rscGrpMap.get(testRscGrpName)).isNotNull();
    }

    /*
     * spawn tests (flux based, must not run within the testScope)
     */

    @Test
    public void spawnUnknownRscGrp() throws Exception
    {
        evaluateTest(
            new SpawnRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .rscGrpName("UnknownRscGrp")
        );
    }

    @Test
    public void spawnVlmSizeCountMismatch() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateRscGrpCall(
                RC_VLM_GRP_CREATED,
                RC_RSC_GRP_CREATED
            )
                .setRscGrpName("GrpWithVlmGrp")
                .addVlmGrp(0)
        );
        leaveScope();

        // the group has one volume group but no volume sizes are given and partial mode is not requested
        evaluateTest(
            new SpawnRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_VLM_SIZES)
                .rscGrpName("GrpWithVlmGrp")
        );
    }

    @Test
    public void spawnPassphraseCountMismatch() throws Exception
    {
        evaluateTest(
            new SpawnRscGrpCall(MASK_RSC_GRP_CRT | ApiConsts.FAIL_INVLD_REQUEST)
                .addVlmSize(100 * 1024L)
                .passphrases("passphrase1", "passphrase2")
        );
    }

    @Test
    public void spawnEmptyGrpWithoutVlmSizesAndAutoPlaceConfig() throws Exception
    {
        // even without volume groups and without an explicit autoplace config the spawn runs the
        // autoplacer with the group defaults - and fails since this test defines no satellites
        evaluateTest(
            new SpawnRscGrpCall(
                MASK_RSC_GRP_CRT | ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
        );
    }

    @Test
    public void spawnSuccess() throws Exception
    {
        Node stlt = createSatelliteWithStorPool("SpawnStlt", "spool", 10_000_000L);

        enterScope();
        evaluateTest(
            new CreateRscGrpCall(
                RC_VLM_GRP_CREATED,
                RC_RSC_GRP_CREATED
            )
                .setRscGrpName("SpawnRscGrp")
                .addVlmGrp(0)
                .setAutoSelectFilter(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(1)
                        .setStorPoolNameList(new ArrayList<>(Arrays.asList("spool")))
                        .setLayerStackList(
                            new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE))
                        )
                        .build()
                )
        );
        leaveScope();

        // the deployment on the satellite deliberately fails (the mocked peer answers all api calls with
        // an error), hence the trailing FAIL_UNKNOWN_ERROR after the successful registration responses
        evaluateTest(
            new SpawnRscGrpCall(
                ApiConsts.MASK_VLM_DFN | ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MASK_RSC | ApiConsts.MASK_CRT | ApiConsts.CREATED, // resource registered
                MASK_RSC_GRP_CRT | ApiConsts.CREATED, // successfully spawned
                MASK_RSC_GRP_CRT | ApiConsts.FAIL_UNKNOWN_ERROR // deployment deliberately fails (mock peer)
            )
                .rscGrpName("SpawnRscGrp")
                .addVlmSize(100 * 1024L)
        );

        ResourceDefinition spawned = rscDfnMap.get(new ResourceName("SpawnedRsc"));
        assertThat(spawned).isNotNull();
        assertThat(spawned.getVolumeDfnCount()).isEqualTo(1);
        assertThat(spawned.getResourceCount()).isEqualTo(1);
        assertThat(stlt.getResource(new ResourceName("SpawnedRsc"))).isNotNull();
    }

    private Node createSatelliteWithStorPool(String nodeNameStr, String storPoolNameStr, long poolSizeKib)
        throws Exception
    {
        enterScope();

        Node stlt = nodeFactory.create(
            new NodeName(nodeNameStr),
            Node.Type.SATELLITE,
            null
        );

        Peer mockedPeer = Mockito.mock(Peer.class);
        ExtToolsManager mockedExtToolsMgr = Mockito.mock(ExtToolsManager.class);

        stubSatellitePeer(mockedPeer, mockedExtToolsMgr, new SatelliteState(), true);
        stubAllExtToolsSupported(mockedExtToolsMgr);
        // fail the deployment of new resources so that the API call handler does not wait for the
        // resource to become ready
        Mockito.when(mockedPeer.apiCall(Mockito.anyString(), Mockito.any()))
            .thenReturn(Flux.error(new RuntimeException("Deployment deliberately failed")));

        stlt.setPeer(mockedPeer);
        nodesMap.put(stlt.getName(), stlt);

        StorPoolName dfltDisklessName = new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME);
        StorPoolDefinition dfltDisklessStorPoolDfn = storPoolDefinitionRepository.get(dfltDisklessName);
        if (dfltDisklessStorPoolDfn == null)
        {
            dfltDisklessStorPoolDfn = storPoolDefinitionFactory.create(dfltDisklessName);
        }
        storPoolFactory.create(
            stlt,
            dfltDisklessStorPoolDfn,
            DeviceProviderKind.DISKLESS,
            freeSpaceMgrFactory.getInstance(new SharedStorPoolName(stlt.getName(), dfltDisklessName)),
            false
        );

        StorPoolName storPoolName = new StorPoolName(storPoolNameStr);
        StorPoolDefinition storPoolDfn = storPoolDefinitionRepository.get(storPoolName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(storPoolName);
            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
        }
        StorPool storPool = storPoolFactory.create(
            stlt,
            storPoolDfn,
            DeviceProviderKind.LVM,
            freeSpaceMgrFactory.getInstance(new SharedStorPoolName(stlt.getName(), storPoolName)),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(poolSizeKib, poolSizeKib);

        leaveScope();

        return stlt;
    }

    private class CreateRscGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;
        private String description;
        private final Map<String, String> props;
        private final List<VolumeGroupApi> vlmGrpApis;
        private AutoSelectFilterApi autoSelectFilter;

        CreateRscGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = "NewRscGrp";
            description = "";
            props = new TreeMap<>();
            vlmGrpApis = new ArrayList<>();
            autoSelectFilter = null;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return rscGrpApiCallHandlerProvider.get().create(
                new RscGrpPojo(
                    null,
                    rscGrpName,
                    description,
                    props,
                    vlmGrpApis,
                    autoSelectFilter,
                    null
                )
            );
        }

        public CreateRscGrpCall setRscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        public CreateRscGrpCall setDescription(String descriptionRef)
        {
            description = descriptionRef;
            return this;
        }

        public CreateRscGrpCall setProp(String key, String value)
        {
            props.put(key, value);
            return this;
        }

        public CreateRscGrpCall addVlmGrp(Integer vlmNr)
        {
            vlmGrpApis.add(
                new VlmGrpPojo(
                    null,
                    vlmNr,
                    new TreeMap<>(),
                    0L
                )
            );
            return this;
        }

        public CreateRscGrpCall setAutoSelectFilter(AutoSelectFilterApi autoSelectFilterRef)
        {
            autoSelectFilter = autoSelectFilterRef;
            return this;
        }
    }

    private class ModifyRscGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;
        private String description;
        private final Map<String, String> overrideProps;
        private final HashSet<String> deletePropKeys;
        private final HashSet<String> deletePropNamespaces;
        private AutoSelectFilterApi autoSelectFilter;

        ModifyRscGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = TEST_RSC_GRP_NAME;
            description = null; // default: do not update the description
            overrideProps = new TreeMap<>();
            deletePropKeys = new HashSet<>();
            deletePropNamespaces = new HashSet<>();
            autoSelectFilter = null;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscGrpApiCallHandlerProvider.get().modify(
                rscGrpName,
                description,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces,
                autoSelectFilter,
                null,
                false,
                new ArrayList<>()
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public ModifyRscGrpCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        public ModifyRscGrpCall description(String descriptionRef)
        {
            description = descriptionRef;
            return this;
        }

        public ModifyRscGrpCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        public ModifyRscGrpCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        public ModifyRscGrpCall autoSelectFilter(AutoSelectFilterApi autoSelectFilterRef)
        {
            autoSelectFilter = autoSelectFilterRef;
            return this;
        }
    }

    private class DeleteRscGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;

        DeleteRscGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = TEST_RSC_GRP_NAME;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return rscGrpApiCallHandlerProvider.get().delete(rscGrpName);
        }

        public DeleteRscGrpCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }
    }

    private class SpawnRscGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;
        private String rscDfnName;
        private final List<Long> vlmSizes;
        private boolean partial;
        private boolean definitionsOnly;
        private List<String> volumePassphrases;

        SpawnRscGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = TEST_RSC_GRP_NAME;
            rscDfnName = "SpawnedRsc";
            vlmSizes = new ArrayList<>();
            partial = false;
            definitionsOnly = false;
            volumePassphrases = null;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscGrpApiCallHandlerProvider.get().spawn(
                rscGrpName,
                rscDfnName,
                null,
                vlmSizes,
                null,
                partial,
                definitionsOnly,
                null,
                volumePassphrases,
                new TreeMap<>()
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public SpawnRscGrpCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        public SpawnRscGrpCall addVlmSize(long vlmSizeRef)
        {
            vlmSizes.add(vlmSizeRef);
            return this;
        }

        public SpawnRscGrpCall passphrases(String... passphrasesRef)
        {
            volumePassphrases = new ArrayList<>(Arrays.asList(passphrasesRef));
            return this;
        }
    }
}
