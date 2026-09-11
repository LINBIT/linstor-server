package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmApiCallHandler;
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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class VlmModifyApiTest extends ApiTestBase
{
    private static final String TEST_NODE_A = "TestNodeA";
    private static final String TEST_RSC_NAME = "TestRsc";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final int TEST_VLM_NR = 0;
    private static final String AUX_KEY = ApiConsts.NAMESPC_AUXILIARY + "/test";
    private static final String AUX_KEY2 = ApiConsts.NAMESPC_AUXILIARY + "/other";

    @Inject
    private Provider<CtrlVlmApiCallHandler> vlmApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer mockSatelliteA;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeAName;
    private final ResourceName testRscName;
    private final StorPoolName testStorPoolName;
    private final VolumeNumber testVlmNr;

    private Node testNodeA;

    public VlmModifyApiTest() throws Exception
    {
        testNodeAName = new NodeName(TEST_NODE_A);
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

        stubSatellitePeer(mockSatelliteA, mockExtToolsMgr, new SatelliteState(), true);

        testNodeA = createSatelliteNode(testNodeAName, mockSatelliteA);

        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, TEST_VLM_NR)
            .setSize(100 * 1024L)
            .build();

        createResourceOnNode(TEST_NODE_A);

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
    }

    private Volume getVlm() throws Exception
    {
        return testNodeA.getResource(testRscName).getVolume(testVlmNr);
    }

    @Test
    public void modSetProp() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        assertThat(getVlm().getProps().getProp(AUX_KEY)).isEqualTo("value");
    }

    @Test
    public void modDeleteProp() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
        );

        evaluateTest(
            new ModifyVlmCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(AUX_KEY)
        );

        assertThat(getVlm().getProps().getProp(AUX_KEY)).isNull();
    }

    @Test
    public void modDeleteNamespace() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
                .overrideProps(AUX_KEY, "value")
                .overrideProps(AUX_KEY2, "otherValue")
        );

        evaluateTest(
            new ModifyVlmCall(ApiConsts.MODIFIED)
                .deleteNamespace(ApiConsts.NAMESPC_AUXILIARY)
        );

        assertThat(getVlm().getProps().getProp(AUX_KEY)).isNull();
        assertThat(getVlm().getProps().getProp(AUX_KEY2)).isNull();
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        // the auxiliary prop is set before the invalid prop is rejected, but the transaction is
        // rolled back, so neither prop may be visible afterwards
        evaluateTest(
            new ModifyVlmCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps(AUX_KEY, "value")
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );

        assertThat(getVlm().getProps().getProp(AUX_KEY)).isNull();
    }

    @Test
    public void modWrongUuid() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(ApiConsts.FAIL_UUID_VLM)
                .vlmUuid(randomUUID())
                .overrideProps(AUX_KEY, "value")
        );

        assertThat(getVlm().getProps().getProp(AUX_KEY)).isNull();
    }

    @Test
    public void modUnknownVlmNr() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(ApiConsts.FAIL_NOT_FOUND_VLM)
                .setVlmNr(4)
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modUnknownNode() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
                .overrideProps(AUX_KEY, "value")
        );
    }

    @Test
    public void modUnknownRsc() throws Exception
    {
        evaluateTest(
            new ModifyVlmCall(ApiConsts.FAIL_NOT_FOUND_RSC_DFN)
                .setRscName("UnknownRsc")
                .overrideProps(AUX_KEY, "value")
        );
    }

    private class ModifyVlmCall extends AbsApiCallTester
    {
        private UUID vlmUuid;
        private String nodeName;
        private String rscName;
        private int vlmNr;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deleteNamespaces;

        ModifyVlmCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_VLM,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            vlmUuid = null; // default: do not check against uuid
            nodeName = TEST_NODE_A;
            rscName = TEST_RSC_NAME;
            vlmNr = TEST_VLM_NR;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deleteNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            vlmApiCallHandlerProvider.get().modify(
                vlmUuid,
                nodeName,
                rscName,
                vlmNr,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        ModifyVlmCall vlmUuid(UUID uuid)
        {
            vlmUuid = uuid;
            return this;
        }

        ModifyVlmCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        ModifyVlmCall setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        ModifyVlmCall setVlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
            return this;
        }

        ModifyVlmCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        ModifyVlmCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        ModifyVlmCall deleteNamespace(String namespace)
        {
            deleteNamespaces.add(namespace);
            return this;
        }
    }
}
