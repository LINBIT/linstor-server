package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmDfnWithCreationPayloadPojo;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class RscDfnApiTest extends ApiTestBase
{
    private static final String TEST_RSC_DFN_NAME = "TestRscDfn";
    private static final long TEST_VLM_SIZE = 100 * 1024L;

    private static final long MASK_CRT_RSC_DFN = ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_CRT;
    private static final long RC_RSC_DFN_CREATED = MASK_CRT_RSC_DFN | ApiConsts.CREATED;
    // volume definition success entries of the create API are added without an operation mask
    private static final long RC_VLM_DFN_CREATED = ApiConsts.MASK_VLM_DFN | ApiConsts.CREATED;

    @Inject private Provider<CtrlRscDfnApiCallHandler> rscDfnApiCallHandlerProvider;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    private ResourceName testRscName;
    private ResourceDefinition testRscDfn;

    public RscDfnApiTest() throws Exception
    {
        testRscName = new ResourceName(TEST_RSC_DFN_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        LayerPayload payload = new LayerPayload();
        payload.getDrbdRscDfn().sharedSecret = "notTellingYou";
        payload.getDrbdRscDfn().transportType = TransportType.IP;
        testRscDfn = resourceDefinitionFactory.create(
            testRscName,
            null,
            null,
            new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)),
            payload,
            createDefaultResourceGroup()
        );
        rscDfnMap.put(testRscName, testRscDfn);

        leaveScope();
    }

    /*
     * createResourceDefinition tests. The create API is not flux based, so the test needs to
     * (re-)enter the testScope before calling the handler directly.
     */

    @Test
    public void crtSuccess() throws Exception
    {
        enterScope();
        CrtRscDfnCall call = new CrtRscDfnCall(RC_RSC_DFN_CREATED);
        evaluateTest(call);

        assertThat(call.createdRscDfn).isNotNull();
        assertThat(rscDfnMap.get(new ResourceName("NewRsc"))).isSameAs(call.createdRscDfn);
    }

    @Test
    public void crtWithVlmDfn() throws Exception
    {
        enterScope();
        CrtRscDfnCall call = new CrtRscDfnCall(
            RC_VLM_DFN_CREATED,
            RC_RSC_DFN_CREATED
        );
        call.addVlmDfn(0, TEST_VLM_SIZE);
        evaluateTest(call);

        assertThat(call.createdRscDfn).isNotNull();
        assertThat(call.createdRscDfn.getVolumeDfnCount()).isEqualTo(1);
    }

    @Test
    public void crtWithProps() throws Exception
    {
        enterScope();
        CrtRscDfnCall call = new CrtRscDfnCall(
            MASK_CRT_RSC_DFN | ApiConsts.CREATED, // props set
            RC_RSC_DFN_CREATED
        );
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        call.setProp(auxKey, "value");
        evaluateTest(call);

        assertThat(call.createdRscDfn.getProps().getProp(auxKey)).isEqualTo("value");
    }

    @Test
    public void crtSecondExists() throws Exception
    {
        enterScope();
        evaluateTest(
            new CrtRscDfnCall(MASK_CRT_RSC_DFN | ApiConsts.FAIL_EXISTS_RSC_DFN)
                .setRscName(TEST_RSC_DFN_NAME)
        );
    }

    @Test
    public void crtInvalidRscName() throws Exception
    {
        enterScope();
        evaluateTest(
            new CrtRscDfnCall(MASK_CRT_RSC_DFN | ApiConsts.FAIL_INVLD_RSC_NAME)
                .setRscName("Invalid Name") // blank is not allowed
        );
    }

    @Test
    public void crtInvalidLayerKind() throws Exception
    {
        enterScope();
        evaluateTest(
            new CrtRscDfnCall(MASK_CRT_RSC_DFN | ApiConsts.FAIL_INVLD_LAYER_KIND)
                .setLayerStack("NotALayer")
        );
    }

    @Test
    public void crtDrbdLayerStackAddsStorage() throws Exception
    {
        enterScope();
        CrtRscDfnCall call = new CrtRscDfnCall(
            ApiConsts.WARN_STORAGE_KIND_ADDED,
            RC_RSC_DFN_CREATED
        );
        call.setLayerStack("DRBD");
        evaluateTest(call);

        assertThat(call.createdRscDfn.getLayerStack())
            .containsExactly(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);
    }

    @Test
    public void crtGeneratedNameFromExtName() throws Exception
    {
        enterScope();
        byte[] extName = "ExternalNameTest".getBytes(StandardCharsets.UTF_8);
        CrtRscDfnCall call = new CrtRscDfnCall(RC_RSC_DFN_CREATED);
        call.setRscName("");
        call.setExtName(extName);
        evaluateTest(call);

        assertThat(call.createdRscDfn).isNotNull();
        assertThat(call.createdRscDfn.getExternalName()).isEqualTo(extName);
    }

    @Test
    public void crtMissingExtName() throws Exception
    {
        enterScope();
        evaluateTest(
            new CrtRscDfnCall(MASK_CRT_RSC_DFN | ApiConsts.FAIL_MISSING_EXT_NAME)
                .setRscName("")
        );
    }

    @Test
    public void crtDuplicateExtName() throws Exception
    {
        enterScope();
        byte[] extName = "DuplicateExtName".getBytes(StandardCharsets.UTF_8);
        evaluateTest(
            new CrtRscDfnCall(RC_RSC_DFN_CREATED)
                .setRscName("ExtRsc1")
                .setExtName(extName)
        );
        evaluateTest(
            new CrtRscDfnCall(MASK_CRT_RSC_DFN | ApiConsts.FAIL_EXISTS_EXT_NAME)
                .setRscName("ExtRsc2")
                .setExtName(extName)
        );
    }

    @Test
    public void crtUnknownRscGrp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CrtRscDfnCall(MASK_CRT_RSC_DFN | ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .setRscGrpName("UnknownRscGrp")
        );
    }

    /*
     * modify tests (flux based, must not run within the testScope)
     */

    @Test
    public void modProps() throws Exception
    {
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new ModifyRscDfnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(auxKey, "value")
        );
        assertThat(testRscDfn.getProps().getProp(auxKey)).isEqualTo("value");

        evaluateTest(
            new ModifyRscDfnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(auxKey)
        );
        assertThat(testRscDfn.getProps().getProp(auxKey)).isNull();
    }

    @Test
    public void modWrongUuid() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.FAIL_UUID_RSC_DFN)
                .rscDfnUuid(randomUUID())
        );
    }

    @Test
    public void modUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .rscName("UnknownRsc")
        );
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    @Test
    public void modInvalidLayerStack() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.FAIL_INVLD_LAYER_STACK)
                .layerStack("LUKS", "STORAGE", "DRBD")
        );
        assertThat(testRscDfn.getLayerStack()).containsExactly(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);
    }

    @Test
    public void modLayerStackAddsStorage() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.WARN_STORAGE_KIND_ADDED, ApiConsts.MODIFIED)
                .layerStack("DRBD", "LUKS")
        );
        assertThat(testRscDfn.getLayerStack())
            .containsExactly(DeviceLayerKind.DRBD, DeviceLayerKind.LUKS, DeviceLayerKind.STORAGE);
    }

    @Test
    public void modPort() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.MODIFIED)
                .port(7100)
        );

        DrbdRscDfnData<Resource> drbdRscDfnData = testRscDfn.getLayerData(DeviceLayerKind.DRBD, "");
        assertThat(drbdRscDfnData).isNotNull();
        assertThat(drbdRscDfnData.getTcpPort()).isNotNull();
        assertThat(drbdRscDfnData.getTcpPort().value).isEqualTo(7100);
    }

    @Test
    public void modInvalidPort() throws Exception
    {
        // an out of range port currently surfaces as ValueOutOfRangeException, which the
        // response converter reports as FAIL_UNKNOWN_ERROR
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.FAIL_UNKNOWN_ERROR)
                .port(123456)
        );
    }

    @Test
    public void modLayerStack() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.MODIFIED)
                .layerStack("STORAGE")
        );
        assertThat(testRscDfn.getLayerStack()).containsExactly(DeviceLayerKind.STORAGE);
    }

    @Test
    public void modUnknownRscGrp() throws Exception
    {
        evaluateTest(
            new ModifyRscDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_GRP)
                .rscGrpName("UnknownRscGrp")
        );
    }

    @Test
    public void rscDfnDescriptions()
    {
        assertThat(CtrlRscDfnApiCallHandler.getRscDfnDescription(TEST_RSC_DFN_NAME))
            .isEqualTo("Resource definition: " + TEST_RSC_DFN_NAME);
        assertThat(CtrlRscDfnApiCallHandler.getRscDfnDescription(testRscName))
            .isEqualTo("Resource definition: " + TEST_RSC_DFN_NAME);
        assertThat(CtrlRscDfnApiCallHandler.getRscDfnDescription(testRscDfn))
            .isEqualTo("Resource definition: " + TEST_RSC_DFN_NAME);

        assertThat(CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline(TEST_RSC_DFN_NAME))
            .isEqualTo("resource definition '" + TEST_RSC_DFN_NAME + "'");
        assertThat(CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline(testRscName))
            .isEqualTo("resource definition '" + TEST_RSC_DFN_NAME + "'");
        assertThat(CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline(testRscDfn))
            .isEqualTo("resource definition '" + TEST_RSC_DFN_NAME + "'");
    }

    private class CrtRscDfnCall extends AbsApiCallTester
    {
        private String rscName;
        private byte[] extName;
        private final Map<String, String> props;
        private final List<VolumeDefinitionWithCreationPayload> vlmDfnPayloads;
        private final List<String> layerStack;
        private String rscGrpName;

        private ResourceDefinition createdRscDfn;

        CrtRscDfnCall(long... expectedRcs)
        {
            super(
                0, // the expected RCs are fully specified, do not OR any masks
                0,
                expectedRcs
            );

            rscName = "NewRsc";
            extName = null;
            props = new TreeMap<>();
            vlmDfnPayloads = new ArrayList<>();
            layerStack = new ArrayList<>();
            rscGrpName = null;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            createdRscDfn = rscDfnApiCallHandlerProvider.get().createResourceDefinition(
                rscName,
                extName,
                props,
                vlmDfnPayloads,
                layerStack,
                null,
                rscGrpName,
                false,
                apiCallRc,
                true
            );
            return apiCallRc;
        }

        public CrtRscDfnCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        public CrtRscDfnCall setExtName(byte[] extNameRef)
        {
            extName = extNameRef;
            return this;
        }

        public CrtRscDfnCall setProp(String key, String value)
        {
            props.put(key, value);
            return this;
        }

        public CrtRscDfnCall addVlmDfn(int vlmNr, long size)
        {
            vlmDfnPayloads.add(
                new VlmDfnWithCreationPayloadPojo(
                    new VlmDfnPojo(
                        null,
                        vlmNr,
                        size,
                        0L,
                        new TreeMap<>(),
                        new ArrayList<>()
                    ),
                    null
                )
            );
            return this;
        }

        public CrtRscDfnCall setLayerStack(String... layers)
        {
            layerStack.clear();
            layerStack.addAll(Arrays.asList(layers));
            return this;
        }

        public CrtRscDfnCall setRscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }
    }

    private class ModifyRscDfnCall extends AbsApiCallTester
    {
        private java.util.UUID rscDfnUuid;
        private String rscName;
        private Integer port;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deletePropNamespaces;
        private final List<String> layerStack;
        private Short peerSlots;
        private String rscGrpName;

        ModifyRscDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_RSC_DFN,
                ApiConsts.MASK_MOD,
                expectedRcs
            );

            rscDfnUuid = null; // default: do not check against uuid
            rscName = testRscName.displayValue;
            port = null;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deletePropNamespaces = new TreeSet<>();
            layerStack = new ArrayList<>();
            peerSlots = null;
            rscGrpName = null;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscDfnApiCallHandlerProvider.get().modify(
                rscDfnUuid,
                rscName,
                port,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces,
                layerStack,
                peerSlots,
                rscGrpName
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public ModifyRscDfnCall rscDfnUuid(java.util.UUID uuid)
        {
            rscDfnUuid = uuid;
            return this;
        }

        public ModifyRscDfnCall rscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        public ModifyRscDfnCall port(Integer portRef)
        {
            port = portRef;
            return this;
        }

        public ModifyRscDfnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        public ModifyRscDfnCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        public ModifyRscDfnCall layerStack(String... layers)
        {
            layerStack.clear();
            layerStack.addAll(Arrays.asList(layers));
            return this;
        }

        public ModifyRscDfnCall rscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }
    }
}
