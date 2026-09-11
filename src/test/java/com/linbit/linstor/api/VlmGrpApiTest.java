package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.VlmGrpPojo;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmGrpApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.VolumeGroupControllerFactory;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("checkstyle:magicnumber")
public class VlmGrpApiTest extends ApiTestBase
{
    private static final String TEST_RSC_GRP_NAME = "TestRscGrp";
    private static final String EMPTY_RSC_GRP_NAME = "EmptyRscGrp";
    private static final String AUX_KEY = ApiConsts.NAMESPC_AUXILIARY + "/test";
    private static final int EXISTING_VLM_NR = 0;

    /*
     * The volume group handler builds all of its response contexts with ApiOperation.makeCreateOperation()
     * (also for modify and delete), so every response - even MODIFIED and DELETED ones - carries the
     * MASK_CRT operation mask.
     */
    private static final long MASK_VLM_GRP_CRT = ApiConsts.MASK_VLM_GRP | ApiConsts.MASK_CRT;
    private static final long RC_VLM_GRP_CREATED = MASK_VLM_GRP_CRT | ApiConsts.CREATED;
    private static final long RC_VLM_GRP_MODIFIED = MASK_VLM_GRP_CRT | ApiConsts.MODIFIED;
    private static final long RC_VLM_GRP_DELETED = MASK_VLM_GRP_CRT | ApiConsts.DELETED;
    // volume group props are stored with the VLM_DFN whitelist, so prop entries carry the VLM_DFN mask
    private static final long RC_PROP_SET = ApiConsts.MASK_VLM_DFN | ApiConsts.MASK_CRT | ApiConsts.CREATED;
    private static final long RC_PROP_DELETED = ApiConsts.MASK_VLM_DFN | ApiConsts.MASK_DEL | ApiConsts.DELETED;
    // the per-volume-group context (used while creating a single volume group) carries MASK_VLM_DFN
    private static final long MASK_VLM_DFN_CRT = ApiConsts.MASK_VLM_DFN | ApiConsts.MASK_CRT;

    @Inject
    private Provider<CtrlVlmGrpApiCallHandler> vlmGrpApiCallHandlerProvider;
    @Inject
    private VolumeGroupControllerFactory volumeGroupFactory;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    private final ResourceGroupName testRscGrpName;
    private final ResourceGroupName emptyRscGrpName;

    private ResourceGroup testRscGrp;
    private ResourceGroup emptyRscGrp;

    public VlmGrpApiTest() throws Exception
    {
        testRscGrpName = new ResourceGroupName(TEST_RSC_GRP_NAME);
        emptyRscGrpName = new ResourceGroupName(EMPTY_RSC_GRP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        testRscGrp = createRscGrp(testRscGrpName);
        emptyRscGrp = createRscGrp(emptyRscGrpName);

        // the test resource group starts with an existing volume group, the "empty" one without
        volumeGroupFactory.create(testRscGrp, new VolumeNumber(EXISTING_VLM_NR), 0L);

        leaveScope();
    }

    private ResourceGroup createRscGrp(ResourceGroupName rscGrpName) throws Exception
    {
        ResourceGroup rscGrp = resourceGroupTestFactory.create(rscGrpName.displayValue);
        rscGrpMap.put(rscGrpName, rscGrp);
        return rscGrp;
    }

    private VolumeGroup getVlmGrp(ResourceGroup rscGrp, int vlmNr) throws Exception
    {
        return rscGrp.getVolumeGroup(new VolumeNumber(vlmNr));
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
            new CreateVlmGrpCall(RC_VLM_GRP_CREATED)
                .addVlmGrp(0)
        );

        VolumeGroup vlmGrp = getVlmGrp(emptyRscGrp, 0);
        assertThat(vlmGrp).isNotNull();
        assertThat(vlmGrp.getVolumeNumber().value).isEqualTo(0);
    }

    @Test
    public void crtGeneratedVlmNrs() throws Exception
    {
        enterScope();
        // volume numbers are auto-generated when not given
        evaluateTest(
            new CreateVlmGrpCall(
                RC_VLM_GRP_CREATED,
                RC_VLM_GRP_CREATED
            )
                .addVlmGrp(null)
                .addVlmGrp(null)
        );

        assertThat(getVlmGrp(emptyRscGrp, 0)).isNotNull();
        assertThat(getVlmGrp(emptyRscGrp, 1)).isNotNull();
        assertThat(emptyRscGrp.getVolumeGroups()).hasSize(2);
    }

    @Test
    public void crtWithProps() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(
                RC_PROP_SET,
                RC_VLM_GRP_CREATED
            )
                .addVlmGrp(0, AUX_KEY, "value")
        );

        VolumeGroup vlmGrp = getVlmGrp(emptyRscGrp, 0);
        assertThat(vlmGrp).isNotNull();
        assertThat(vlmGrp.getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void crtWithGrossSizeFlag() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(RC_VLM_GRP_CREATED)
                .addVlmGrp(0, VolumeGroup.Flags.GROSS_SIZE.flagValue)
        );

        VolumeGroup vlmGrp = getVlmGrp(emptyRscGrp, 0);
        assertThat(vlmGrp).isNotNull();
        assertThat(vlmGrp.getFlags().isSet(VolumeGroup.Flags.GROSS_SIZE)).isTrue();
    }

    @Test
    public void crtSecondExists() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(MASK_VLM_DFN_CRT | ApiConsts.FAIL_EXISTS_VLM_GRP)
                .rscGrpName(TEST_RSC_GRP_NAME)
                .addVlmGrp(EXISTING_VLM_NR)
        );
    }

    @Test
    public void crtUnknownRscGrp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .rscGrpName("UnknownRscGrp")
                .addVlmGrp(0)
        );
    }

    @Test
    public void crtEmptyVlmGrpList() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.MASK_WARN)
        );
    }

    @Test
    public void crtRscDfnAlreadyExists() throws Exception
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
            emptyRscGrp
        );
        rscDfnMap.put(rscName, rscDfn);

        // volume groups cannot be added once the resource group has resource definitions
        evaluateTest(
            new CreateVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_EXISTS_RSC_DFN)
                .addVlmGrp(0)
        );
        assertThat(emptyRscGrp.getVolumeGroups()).isEmpty();
    }

    @Test
    public void crtInvalidVlmNr() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_INVLD_VLM_NR)
                .addVlmGrp(-1)
        );
    }

    @Test
    public void crtInvalidProp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateVlmGrpCall(MASK_VLM_DFN_CRT | ApiConsts.FAIL_INVLD_PROP)
                .addVlmGrp(0, "ThisIsNotAWhitelistedKey", "value")
        );
    }

    /*
     * modify tests (flux based, must not run within the testScope)
     */

    @Test
    public void modSetProp() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(
                RC_PROP_SET,
                RC_VLM_GRP_MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR).getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modDeleteProp() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(
                RC_PROP_SET,
                RC_VLM_GRP_MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyVlmGrpCall(
                RC_PROP_DELETED,
                RC_VLM_GRP_MODIFIED
            )
                .deleteProp(AUX_KEY)
        );

        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR).getProps().getProp(AUX_KEY)).isNull();
    }

    @Test
    public void modDeleteNamespace() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(
                // both props are reported in a single "Successfully set property key(s)" entry
                RC_PROP_SET,
                RC_VLM_GRP_MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
                .overrideProps(ApiConsts.NAMESPC_AUXILIARY + "/other", "otherValue")
        );

        evaluateTest(
            new ModifyVlmGrpCall(RC_VLM_GRP_MODIFIED)
                .deleteNamespace(ApiConsts.NAMESPC_AUXILIARY)
        );

        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR).getProps().isEmpty()).isTrue();
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );

        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR).getProps().isEmpty()).isTrue();
    }

    @Test
    public void modGrossSizeFlagToggle() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(RC_VLM_GRP_MODIFIED)
                .flags(ApiConsts.FLAG_GROSS_SIZE)
        );
        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR).getFlags().isSet(VolumeGroup.Flags.GROSS_SIZE))
            .isTrue();

        evaluateTest(
            new ModifyVlmGrpCall(RC_VLM_GRP_MODIFIED)
                .flags("-" + ApiConsts.FLAG_GROSS_SIZE)
        );
        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR).getFlags().isSet(VolumeGroup.Flags.GROSS_SIZE))
            .isFalse();
    }

    @Test
    public void modUnknownVlmNr() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_VLM_GRP)
                .vlmNr(99)
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modUnknownRscGrp() throws Exception
    {
        evaluateTest(
            new ModifyVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .rscGrpName("UnknownRscGrp")
                .overrideProps(AUX_KEY, "value")
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
            new DeleteVlmGrpCall(RC_VLM_GRP_DELETED)
        );

        assertThat(getVlmGrp(testRscGrp, EXISTING_VLM_NR)).isNull();
        assertThat(testRscGrp.getVolumeGroups()).isEmpty();
    }

    @Test
    public void delUnknownVlmNr() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_VLM_GRP)
                .vlmNr(99)
        );
    }

    @Test
    public void delUnknownRscGrp() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteVlmGrpCall(MASK_VLM_GRP_CRT | ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .rscGrpName("UnknownRscGrp")
        );
    }

    /*
     * list tests (plain method calls, need the testScope for the peer access context)
     */

    @Test
    public void listAll() throws Exception
    {
        enterScope();
        volumeGroupFactory.create(testRscGrp, new VolumeNumber(1), 0L);

        List<VolumeGroupApi> vlmGrps = vlmGrpApiCallHandlerProvider.get()
            .listVolumeGroups(TEST_RSC_GRP_NAME, null);

        assertThat(vlmGrps).hasSize(2);
        assertThat(vlmGrps.get(0).getVolumeNr()).isEqualTo(0);
        assertThat(vlmGrps.get(1).getVolumeNr()).isEqualTo(1);
    }

    @Test
    public void listSingle() throws Exception
    {
        enterScope();
        List<VolumeGroupApi> vlmGrps = vlmGrpApiCallHandlerProvider.get()
            .listVolumeGroups(TEST_RSC_GRP_NAME, EXISTING_VLM_NR);

        assertThat(vlmGrps).hasSize(1);
        assertThat(vlmGrps.get(0).getVolumeNr()).isEqualTo(EXISTING_VLM_NR);
    }

    @Test
    public void listUnknownVlmNr() throws Exception
    {
        enterScope();
        assertThatThrownBy(
            () -> vlmGrpApiCallHandlerProvider.get().listVolumeGroups(TEST_RSC_GRP_NAME, 99)
        )
            .isInstanceOf(ApiRcException.class)
            .satisfies(
                exc -> assertThat(((ApiRcException) exc).getApiCallRc().get(0).getReturnCode())
                    .isEqualTo(ApiConsts.FAIL_NOT_FOUND_VLM_GRP)
            );
    }

    @Test
    public void listUnknownRscGrp() throws Exception
    {
        enterScope();
        assertThatThrownBy(
            () -> vlmGrpApiCallHandlerProvider.get().listVolumeGroups("UnknownRscGrp", null)
        )
            .isInstanceOf(ApiRcException.class)
            .satisfies(
                exc -> assertThat(((ApiRcException) exc).getApiCallRc().get(0).getReturnCode())
                    .isEqualTo(ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
            );
    }

    private class CreateVlmGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;
        private final List<VolumeGroupApi> vlmGrpApis;

        CreateVlmGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = EMPTY_RSC_GRP_NAME;
            vlmGrpApis = new ArrayList<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return vlmGrpApiCallHandlerProvider.get().createVlmGrps(rscGrpName, vlmGrpApis);
        }

        CreateVlmGrpCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        CreateVlmGrpCall addVlmGrp(Integer vlmNr)
        {
            return addVlmGrp(vlmNr, new TreeMap<>(), 0L);
        }

        CreateVlmGrpCall addVlmGrp(Integer vlmNr, long flags)
        {
            return addVlmGrp(vlmNr, new TreeMap<>(), flags);
        }

        CreateVlmGrpCall addVlmGrp(Integer vlmNr, String propKey, String propValue)
        {
            Map<String, String> props = new TreeMap<>();
            props.put(propKey, propValue);
            return addVlmGrp(vlmNr, props, 0L);
        }

        CreateVlmGrpCall addVlmGrp(Integer vlmNr, Map<String, String> props, long flags)
        {
            vlmGrpApis.add(
                new VlmGrpPojo(
                    null,
                    vlmNr,
                    props,
                    flags
                )
            );
            return this;
        }
    }

    private class ModifyVlmGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;
        private int vlmNr;
        private final Map<String, String> overrideProps;
        private final HashSet<String> deletePropKeys;
        private final HashSet<String> deleteNamespaces;
        private final List<String> flagsList;

        ModifyVlmGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = TEST_RSC_GRP_NAME;
            vlmNr = EXISTING_VLM_NR;
            overrideProps = new TreeMap<>();
            deletePropKeys = new HashSet<>();
            deleteNamespaces = new HashSet<>();
            flagsList = new ArrayList<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            vlmGrpApiCallHandlerProvider.get().modify(
                rscGrpName,
                vlmNr,
                overrideProps,
                deletePropKeys,
                deleteNamespaces,
                flagsList
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        ModifyVlmGrpCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        ModifyVlmGrpCall vlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
            return this;
        }

        ModifyVlmGrpCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyVlmGrpCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyVlmGrpCall deleteNamespace(String namespace)
        {
            deleteNamespaces.add(namespace);
            return this;
        }

        ModifyVlmGrpCall flags(String... flagsRef)
        {
            flagsList.addAll(Arrays.asList(flagsRef));
            return this;
        }
    }

    private class DeleteVlmGrpCall extends AbsApiCallTester
    {
        private String rscGrpName;
        private int vlmNr;

        DeleteVlmGrpCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscGrpName = TEST_RSC_GRP_NAME;
            vlmNr = EXISTING_VLM_NR;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return vlmGrpApiCallHandlerProvider.get().delete(rscGrpName, vlmNr);
        }

        DeleteVlmGrpCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        DeleteVlmGrpCall vlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
            return this;
        }
    }
}
