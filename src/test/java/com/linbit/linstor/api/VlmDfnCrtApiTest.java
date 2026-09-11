package com.linbit.linstor.api;

import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmDfnWithCreationPayloadPojo;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.event.EventSerializerDescriptor;
import com.linbit.linstor.event.WatchStore;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class VlmDfnCrtApiTest extends ApiTestBase
{
    private static final String TEST_RSC_NAME = "TestVlmDfnRsc";
    private static final long TEST_VLM_SIZE = 100 * 1024L; // size in KiB

    @Inject
    private Provider<CtrlApiCallHandler> ctrlApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    // dependencies of the CtrlApiCallHandler facade that no test module provides
    @Bind
    @Mock
    protected WatchStore watchStore;

    @Bind
    protected Map<String, EventSerializer> eventSerializers = Collections.emptyMap();

    @Bind
    protected Map<String, EventSerializerDescriptor> eventSerializerDescriptors = Collections.emptyMap();

    private ResourceDefinition testRscDfn;

    private final AtomicInteger minorNrGenerator = new AtomicInteger(1000);

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        // each auto-allocated DRBD minor number has to be unique
        Mockito.when(minorNrPoolMock.autoAllocate())
            .thenAnswer(ignoredContext -> minorNrGenerator.getAndIncrement());

        testRscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(testRscDfn.getName(), testRscDfn);

        leaveScope();
    }

    @Test
    public void crtSuccess() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.CREATED)
                .addVlmDfn(null, TEST_VLM_SIZE)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(1);
        VolumeDefinition vlmDfn = testRscDfn.getVolumeDfn(new VolumeNumber(0));
        assertThat(vlmDfn).isNotNull();
        assertThat(vlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
    }

    @Test
    public void crtMultipleAutoVlmNrs() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(
                ApiConsts.CREATED,
                ApiConsts.CREATED
            )
                .addVlmDfn(null, TEST_VLM_SIZE)
                .addVlmDfn(null, TEST_VLM_SIZE * 2)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(2);
        assertThat(testRscDfn.getVolumeDfn(new VolumeNumber(0)).getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
        assertThat(testRscDfn.getVolumeDfn(new VolumeNumber(1)).getVolumeSize()).isEqualTo(TEST_VLM_SIZE * 2);
    }

    @Test
    public void crtExplicitVlmNr() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.CREATED)
                .addVlmDfn(4, TEST_VLM_SIZE)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(1);
        assertThat(testRscDfn.getVolumeDfn(new VolumeNumber(4))).isNotNull();
    }

    @Test
    public void crtDuplicateVlmNr() throws Exception
    {
        createVlmDfn(0);

        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.FAIL_EXISTS_VLM_DFN)
                .addVlmDfn(0, TEST_VLM_SIZE)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(1);
    }

    @Test
    public void crtInvalidVlmNr() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.FAIL_INVLD_VLM_NR)
                .addVlmDfn(-1, TEST_VLM_SIZE)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(0);
    }

    @Test
    public void crtSizeZero() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.FAIL_INVLD_VLM_SIZE)
                .addVlmDfn(null, 0L)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(0);
    }

    @Test
    public void crtSizeNegative() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.FAIL_INVLD_VLM_SIZE)
                .addVlmDfn(null, -TEST_VLM_SIZE)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(0);
    }

    @Test
    public void crtSizeAboveDrbdMax() throws Exception
    {
        // the DRBD maximum size is only enforced once DRBD layer data exists (i.e. with deployed
        // resources), so creating an oversized volume definition on an undeployed resource
        // definition is accepted
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.CREATED)
                .addVlmDfn(null, MetaData.DRBD_MAX_kiB + 1)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(1);
        assertThat(testRscDfn.getVolumeDfn(new VolumeNumber(0)).getVolumeSize())
            .isEqualTo(MetaData.DRBD_MAX_kiB + 1);
    }

    @Test
    public void crtInvalidMinorNr() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.FAIL_INVLD_MINOR_NR)
                .addVlmDfn(null, TEST_VLM_SIZE, 0L, -5)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(0);
    }

    @Test
    public void crtUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
                .addVlmDfn(null, TEST_VLM_SIZE)
        );
    }

    @Test
    public void crtEmptyVlmDfnList() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.MASK_WARN)
        );

        assertThat(testRscDfn.getVolumeDfnCount()).isEqualTo(0);
    }

    @Test
    public void crtGrossSizeFlag() throws Exception
    {
        evaluateTest(
            new CreateVlmDfnCall(ApiConsts.CREATED)
                .addVlmDfn(null, TEST_VLM_SIZE, VolumeDefinition.Flags.GROSS_SIZE.flagValue, null)
        );

        VolumeDefinition vlmDfn = testRscDfn.getVolumeDfn(new VolumeNumber(0));
        assertThat(vlmDfn).isNotNull();
        assertThat(vlmDfn.getFlags().isSet(VolumeDefinition.Flags.GROSS_SIZE)).isTrue();
    }

    private void createVlmDfn(int vlmNr) throws Exception
    {
        enterScope();
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, vlmNr)
            .setSize(TEST_VLM_SIZE)
            .build();
        leaveScope();
    }

    private class CreateVlmDfnCall extends AbsApiCallTester
    {
        private String rscName;
        private final List<VolumeDefinitionWithCreationPayload> vlmDfnPayloads;

        CreateVlmDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM_DFN,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            rscName = TEST_RSC_NAME;
            vlmDfnPayloads = new ArrayList<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            ctrlApiCallHandlerProvider.get().createVlmDfns(
                rscName,
                vlmDfnPayloads
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public CreateVlmDfnCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        public CreateVlmDfnCall addVlmDfn(Integer vlmNr, long size)
        {
            return addVlmDfn(vlmNr, size, 0L, null);
        }

        public CreateVlmDfnCall addVlmDfn(Integer vlmNr, long size, long flags, Integer minorNr)
        {
            vlmDfnPayloads.add(
                new VlmDfnWithCreationPayloadPojo(
                    new VlmDfnPojo(
                        null,
                        vlmNr,
                        size,
                        flags,
                        new TreeMap<>(),
                        new ArrayList<>()
                    ),
                    minorNr
                )
            );
            return this;
        }
    }
}
