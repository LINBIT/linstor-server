package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnModifyApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
public class VlmDfnModifyApiTest extends ApiTestBase
{
    private static final String TEST_RSC_NAME = "TestVlmDfnRsc";
    private static final String SHARED_RSC_NAME = "SharedRsc";
    private static final int TEST_VLM_NR = 0;
    private static final long TEST_VLM_SIZE = 100 * 1024L; // size in KiB

    @Inject private Provider<CtrlVlmDfnModifyApiCallHandler> vlmDfnModifyApiCallHandlerProvider;
    @Inject private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected Peer mockSatellite2;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private ResourceDefinition testRscDfn;
    private VolumeDefinition testVlmDfn;

    private VolumeDefinition sharedVlmDfn;
    private Resource sharedRscNode2;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        testRscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(testRscDfn.getName(), testRscDfn);

        testVlmDfn = volumeDefinitionTestFactory.builder(TEST_RSC_NAME, TEST_VLM_NR)
            .setSize(TEST_VLM_SIZE)
            .build();

        leaveScope();
    }

    @Test
    public void modProps() throws Exception
    {
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new ModifyVlmDfnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(auxKey, "value")
        );
        assertThat(testVlmDfn.getProps().getProp(auxKey)).isEqualTo("value");

        evaluateTest(
            new ModifyVlmDfnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(auxKey)
        );
        assertThat(testVlmDfn.getProps().getProp(auxKey)).isNull();
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    @Test
    public void modWrongUuid() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_UUID_VLM_DFN)
                .vlmDfnUuid(randomUUID())
        );
    }

    @Test
    public void modUnknownVlmNr() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_NOT_FOUND_VLM_DFN)
                .vlmNr(4)
        );
    }

    @Test
    public void modInvalidVlmNr() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_INVLD_VLM_NR)
                .vlmNr(-1)
        );
    }

    @Test
    public void modUnknownRscDfn() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .rscName("UnknownRsc")
        );
    }

    @Test
    public void modGrowSize() throws Exception
    {
        // growing a volume definition without deployed volumes only updates the size
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.MODIFIED)
                .size(TEST_VLM_SIZE * 2)
        );

        assertThat(testVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE * 2);
        assertThat(testVlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE)).isFalse();
        assertThat(testVlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE_SHRINK)).isFalse();
    }

    @Test
    public void modShrinkSizeWithoutDeployedVolumes() throws Exception
    {
        // without deployed volumes there is no layer / provider that could veto shrinking,
        // so the volume definition simply shrinks. The RESIZE_SHRINK flag (which includes the
        // RESIZE bit) triggers the resize workflow, which immediately finishes and clears the
        // flags again since no satellites are involved
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.MODIFIED)
                .size(TEST_VLM_SIZE / 2)
        );

        assertThat(testVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE / 2);
        assertThat(testVlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE)).isFalse();
        assertThat(testVlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE_SHRINK)).isFalse();
    }

    @Test
    public void modSameSize() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(
                ApiConsts.WARN_VLMDFN_RESIZE_SAME_SIZE,
                ApiConsts.MODIFIED
            )
                .size(TEST_VLM_SIZE)
        );

        assertThat(testVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
    }

    @Test
    public void modGrowSizeWithExactSizeSet() throws Exception
    {
        enterScope();
        testRscDfn.getProps().setProp(
            ApiConsts.KEY_DRBD_EXACT_SIZE,
            "True",
            ApiConsts.NAMESPC_DRBD_OPTIONS
        );
        leaveScope();

        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .size(TEST_VLM_SIZE * 2)
        );
        assertThat(testVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);
    }

    @Test
    public void modGrowSizeSharedStorPoolDualActive() throws Exception
    {
        // during the dual-active window of a live migration both legs of a shared-storage-pool
        // resource are active: resizing the shared data underneath the two nodes must be refused
        createDualActiveSharedRsc();

        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.FAIL_IN_USE)
                .rscName(SHARED_RSC_NAME)
                .size(TEST_VLM_SIZE * 2)
        );
        assertThat(sharedVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE);

        // once the dual-active window is closed (second leg deactivated) resizing works again
        enterScope();
        sharedRscNode2.getStateFlags().enableFlags(Resource.Flags.INACTIVE);
        commitAndCleanUp(true);

        evaluateTest(
            new ModifyVlmDfnCall()
                .rscName(SHARED_RSC_NAME)
                .size(TEST_VLM_SIZE * 2),
            false
        );
        assertThat(sharedVlmDfn.getVolumeSize()).isEqualTo(TEST_VLM_SIZE * 2);
    }

    @Test
    public void modGrossSizeFlag() throws Exception
    {
        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.MODIFIED)
                .flags(VolumeDefinition.Flags.GROSS_SIZE.name())
        );
        assertThat(testVlmDfn.getFlags().isSet(VolumeDefinition.Flags.GROSS_SIZE)).isTrue();

        evaluateTest(
            new ModifyVlmDfnCall(ApiConsts.MODIFIED)
                .flags("-" + VolumeDefinition.Flags.GROSS_SIZE.name())
        );
        assertThat(testVlmDfn.getFlags().isSet(VolumeDefinition.Flags.GROSS_SIZE)).isFalse();
    }

    /**
     * Creates two nodes sharing a storage pool, a STORAGE-only rscDfn {@link #SHARED_RSC_NAME} with
     * one volume definition ({@link #sharedVlmDfn}) and an active resource on both nodes, mirroring
     * the dual-active state during a live migration. The second node's resource is stored in
     * {@link #sharedRscNode2}.
     */
    private void createDualActiveSharedRsc() throws Exception
    {
        stubSatellitePeer(mockSatellite, mockExtToolsMgr, new SatelliteState(), true);
        stubSatellitePeer(mockSatellite2, mockExtToolsMgr, new SatelliteState(), true);
        stubAllExtToolsSupported(mockExtToolsMgr);

        enterScope();
        Node node1 = nodeFactory.create(new NodeName("SharedNode1"), Node.Type.SATELLITE, null);
        node1.setPeer(mockSatellite);
        nodesMap.put(node1.getName(), node1);
        Node node2 = nodeFactory.create(new NodeName("SharedNode2"), Node.Type.SATELLITE, null);
        node2.setPeer(mockSatellite2);
        nodesMap.put(node2.getName(), node2);
        commitAndCleanUp(true);

        StorPoolName spName = new StorPoolName("SharedPool");
        SharedStorPoolName sharedSpaceName = new SharedStorPoolName("SharedSpace");
        createSharedStorPool(node1, spName, sharedSpaceName);
        createSharedStorPool(node2, spName, sharedSpaceName);

        enterScope();
        ResourceDefinition sharedRscDfn = resourceDefinitionTestFactory.builder(SHARED_RSC_NAME)
            .setLayerStack(new ArrayList<>(Collections.singletonList(DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(sharedRscDfn.getName(), sharedRscDfn);
        sharedVlmDfn = volumeDefinitionTestFactory.builder(SHARED_RSC_NAME, TEST_VLM_NR)
            .setSize(TEST_VLM_SIZE)
            .build();
        commitAndCleanUp(true);

        createSharedRscOnNode(node1.getName().displayValue, spName.displayValue);
        createSharedRscOnNode(node2.getName().displayValue, spName.displayValue);
        sharedRscNode2 = node2.getResource(new ResourceName(SHARED_RSC_NAME));
    }

    private void createSharedStorPool(Node node, StorPoolName spName, SharedStorPoolName sharedSpaceName)
        throws Exception
    {
        enterScope();
        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(spName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(spName);
            storPoolDfnMap.put(spName, storPoolDfn);
        }
        StorPool storPool = storPoolFactory.create(
            node,
            storPoolDfn,
            DeviceProviderKind.LVM,
            freeSpaceMgrFactory.getInstance(sharedSpaceName),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);
        commitAndCleanUp(true);
    }

    private void createSharedRscOnNode(String nodeNameStr, String spNameStr) throws Exception
    {
        enterScope();
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, spNameStr);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeNameStr,
            SHARED_RSC_NAME,
            0L,
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

    private class ModifyVlmDfnCall extends AbsApiCallTester
    {
        private java.util.UUID vlmDfnUuid;
        private String rscName;
        private int vlmNr;
        private Long size;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final List<String> vlmDfnFlags;

        ModifyVlmDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM_DFN,
                ApiConsts.MASK_MOD,
                expectedRcs
            );

            vlmDfnUuid = null; // default: do not check against uuid
            rscName = TEST_RSC_NAME;
            vlmNr = TEST_VLM_NR;
            size = null; // default: do not change size
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            vlmDfnFlags = new ArrayList<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            vlmDfnModifyApiCallHandlerProvider.get().modifyVlmDfn(
                vlmDfnUuid,
                rscName,
                vlmNr,
                size,
                overrideProps,
                deletePropKeys,
                vlmDfnFlags
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public ModifyVlmDfnCall vlmDfnUuid(java.util.UUID uuid)
        {
            vlmDfnUuid = uuid;
            return this;
        }

        public ModifyVlmDfnCall rscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        public ModifyVlmDfnCall vlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
            return this;
        }

        public ModifyVlmDfnCall size(long sizeRef)
        {
            size = sizeRef;
            return this;
        }

        public ModifyVlmDfnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        public ModifyVlmDfnCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        public ModifyVlmDfnCall flags(String... flags)
        {
            vlmDfnFlags.clear();
            vlmDfnFlags.addAll(Arrays.asList(flags));
            return this;
        }
    }
}
