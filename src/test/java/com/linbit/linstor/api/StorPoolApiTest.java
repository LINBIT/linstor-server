package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.event.EventSerializerDescriptor;
import com.linbit.linstor.event.WatchStore;
import com.linbit.linstor.event.serializer.EventSerializer;
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

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("checkstyle:magicnumber")
public class StorPoolApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_SP_NAME = "TestStorPool";
    private static final String TEST_RSC_NAME = "TestRsc";

    @Inject
    private Provider<CtrlApiCallHandler> ctrlApiCallHandlerProvider;
    @Inject
    private Provider<CtrlStorPoolCrtApiCallHandler> storPoolCrtApiCallHandlerProvider;
    @Inject
    private Provider<CtrlStorPoolApiCallHandler> storPoolApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

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

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final NodeName testNodeName;
    private final StorPoolName testStorPoolName;

    private Node testNode;

    public StorPoolApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
        testStorPoolName = new StorPoolName(TEST_SP_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        stubSatellitePeer(mockSatellite, mockExtToolsMgr, new SatelliteState(), false);
        stubAllExtToolsSupported(mockExtToolsMgr);

        testNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testNode);

        leaveScope();
    }

    /*
     * storage pool definition create tests. The create API is not flux based, so the test needs to
     * (re-)enter the testScope before calling the handler directly.
     */

    @Test
    public void dfnCrtSuccess() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateStorPoolDfnCall(ApiConsts.CREATED)
        );

        assertThat(storPoolDfnMap.get(testStorPoolName)).isNotNull();
    }

    @Test
    public void dfnCrtWithProps() throws Exception
    {
        enterScope();
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        CreateStorPoolDfnCall call = new CreateStorPoolDfnCall(
            ApiConsts.CREATED, // props set
            ApiConsts.CREATED
        );
        call.setProp(auxKey, "value");
        evaluateTest(call);

        assertThat(storPoolDfnMap.get(testStorPoolName).getProps().getProp(auxKey)).isEqualTo("value");
    }

    @Test
    public void dfnCrtSecondExists() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateStorPoolDfnCall(ApiConsts.CREATED)
        );
        evaluateTest(
            new CreateStorPoolDfnCall(ApiConsts.FAIL_EXISTS_STOR_POOL_DFN)
        );
    }

    @Test
    public void dfnCrtInvalidName() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateStorPoolDfnCall(ApiConsts.FAIL_INVLD_STOR_POOL_NAME)
                .setStorPoolName("Invalid Name") // blank is not allowed
        );

        assertThat(storPoolDfnMap.get(testStorPoolName)).isNull();
    }

    @Test
    public void dfnCrtInvalidProp() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateStorPoolDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .setProp("ThisIsNotAWhitelistedKey", "value")
        );
    }

    /*
     * storage pool definition modify tests (flux based, must not run within the testScope)
     */

    @Test
    public void dfnModProps() throws Exception
    {
        createStorPoolDfn();

        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new ModifyStorPoolDfnCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(auxKey, "value")
        );
        assertThat(storPoolDfnMap.get(testStorPoolName).getProps().getProp(auxKey)).isEqualTo("value");

        evaluateTest(
            new ModifyStorPoolDfnCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(auxKey)
        );
        assertThat(storPoolDfnMap.get(testStorPoolName).getProps().getProp(auxKey)).isNull();
    }

    @Test
    public void dfnModWrongUuid() throws Exception
    {
        createStorPoolDfn();

        evaluateTest(
            new ModifyStorPoolDfnCall(ApiConsts.FAIL_UUID_STOR_POOL_DFN)
                .storPoolDfnUuid(randomUUID())
        );
    }

    @Test
    public void dfnModUnknown() throws Exception
    {
        evaluateTest(
            new ModifyStorPoolDfnCall(ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN)
                .setStorPoolName("UnknownStorPool")
        );
    }

    @Test
    public void dfnModInvalidProp() throws Exception
    {
        createStorPoolDfn();

        evaluateTest(
            new ModifyStorPoolDfnCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    /*
     * storage pool definition delete tests (not flux based, needs the testScope)
     */

    @Test
    public void dfnDelSuccess() throws Exception
    {
        createStorPoolDfn();

        enterScope();
        evaluateTest(
            new DeleteStorPoolDfnCall(ApiConsts.DELETED)
        );

        assertThat(storPoolDfnMap.get(testStorPoolName)).isNull();
    }

    @Test
    public void dfnDelUnknown() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteStorPoolDfnCall(ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN)
                .setStorPoolName("UnknownStorPool")
        );
    }

    @Test
    public void dfnDelInUse() throws Exception
    {
        // creating the storage pool also implicitly creates the storage pool definition
        evaluateTest(
            new CreateStorPoolCall(
                ApiConsts.CREATED,
                ApiConsts.MODIFIED // "storage pool updated on ..."
            )
        );

        enterScope();
        evaluateTest(
            new DeleteStorPoolDfnCall(ApiConsts.FAIL_IN_USE)
        );

        assertThat(storPoolDfnMap.get(testStorPoolName)).isNotNull();
    }

    /*
     * storage pool create tests (flux based)
     */

    @Test
    public void spCrtSuccess() throws Exception
    {
        evaluateTest(
            new CreateStorPoolCall(
                ApiConsts.CREATED,
                ApiConsts.MODIFIED // "storage pool updated on ..."
            )
        );

        StorPool storPool = testNode.getStorPool(testStorPoolName);
        assertThat(storPool).isNotNull();
        assertThat(storPool.getDeviceProviderKind()).isEqualTo(DeviceProviderKind.DISKLESS);
        // the storage pool definition was implicitly created
        assertThat(storPoolDfnMap.get(testStorPoolName)).isNotNull();
    }

    @Test
    public void spCrtSecondExists() throws Exception
    {
        evaluateTest(
            new CreateStorPoolCall(
                ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
        );
        evaluateTest(
            new CreateStorPoolCall(ApiConsts.FAIL_EXISTS_STOR_POOL)
        );
    }

    @Test
    public void spCrtUnknownNode() throws Exception
    {
        evaluateTest(
            new CreateStorPoolCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void spCrtInvalidStorPoolName() throws Exception
    {
        evaluateTest(
            new CreateStorPoolCall(ApiConsts.FAIL_INVLD_STOR_POOL_NAME)
                .setStorPoolName("Invalid Name") // blank is not allowed
        );
    }

    @Test
    public void spCrtVlmLayerKindRejected() throws Exception
    {
        // FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER is rejected with a plain error ("nice try")
        evaluateTest(
            new CreateStorPoolCall(ApiConsts.MASK_ERROR)
                .setProviderKind(DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER)
        );
    }

    @Test
    public void spCrtProviderNotSupportedBySatellite() throws Exception
    {
        Mockito.when(mockExtToolsMgr.isProviderSupported(any())).thenReturn(false);

        evaluateTest(
            new CreateStorPoolCall(ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_PROVIDER)
        );

        assertThat(testNode.getStorPool(testStorPoolName)).isNull();
    }

    /*
     * storage pool modify tests (flux based)
     */

    @Test
    public void spModProps() throws Exception
    {
        createStorPool();

        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new ModifyStorPoolCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED,
                ApiConsts.MODIFIED // satellite update
            )
                .overrideProps(auxKey, "value")
        );
        assertThat(testNode.getStorPool(testStorPoolName).getProps().getProp(auxKey)).isEqualTo("value");

        evaluateTest(
            new ModifyStorPoolCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED,
                ApiConsts.MODIFIED // satellite update
            )
                .deleteProp(auxKey)
        );
        assertThat(testNode.getStorPool(testStorPoolName).getProps().getProp(auxKey)).isNull();
    }

    @Test
    public void spModWrongUuid() throws Exception
    {
        createStorPool();

        evaluateTest(
            new ModifyStorPoolCall(ApiConsts.FAIL_UUID_STOR_POOL)
                .storPoolUuid(randomUUID())
        );
    }

    @Test
    public void spModUnknownStorPool() throws Exception
    {
        evaluateTest(
            new ModifyStorPoolCall(ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN)
                .setStorPoolName("UnknownStorPool")
        );
    }

    @Test
    public void spModInvalidProp() throws Exception
    {
        createStorPool();

        evaluateTest(
            new ModifyStorPoolCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    /*
     * storage pool delete tests (flux based)
     */

    @Test
    public void spDelSuccess() throws Exception
    {
        createStorPool();

        evaluateTest(
            new DeleteStorPoolCall(ApiConsts.DELETED)
        );

        assertThat(testNode.getStorPool(testStorPoolName)).isNull();
        // the implicitly created storage pool definition is deleted alongside its last storage pool
        assertThat(storPoolDfnMap.get(testStorPoolName)).isNull();
    }

    @Test
    public void spDelUnknown() throws Exception
    {
        evaluateTest(
            new DeleteStorPoolCall(ApiConsts.WARN_NOT_FOUND)
                .setStorPoolName("UnknownStorPool")
        );
    }

    @Test
    public void spDelDfltDisklessRejected() throws Exception
    {
        evaluateTest(
            new DeleteStorPoolCall(ApiConsts.FAIL_INVLD_STOR_POOL_NAME)
                .setStorPoolName(LinStor.DISKLESS_STOR_POOL_NAME)
        );
    }

    @Test
    public void spDelWithVolumesInUse() throws Exception
    {
        createStorPoolWithVolume();

        evaluateTest(
            new DeleteStorPoolCall(ApiConsts.FAIL_IN_USE)
        );

        assertThat(testNode.getStorPool(testStorPoolName)).isNotNull();
    }

    private void createStorPoolDfn() throws Exception
    {
        enterScope();
        ApiCallRc rc = ctrlApiCallHandlerProvider.get().createStoragePoolDefinition(
            TEST_SP_NAME,
            Collections.emptyMap()
        );
        assertThat(rc.hasErrors()).isFalse();
        leaveScope();
    }

    private void createStorPool() throws Exception
    {
        evaluateTest(
            new CreateStorPoolCall(
                ApiConsts.CREATED,
                ApiConsts.MODIFIED
            )
        );
    }

    private void createStorPoolWithVolume() throws Exception
    {
        enterScope();

        StorPoolDefinition storPoolDfn = storPoolDefinitionFactory.create(testStorPoolName);
        storPoolDfnMap.put(testStorPoolName, storPoolDfn);
        StorPool storPool = storPoolFactory.create(
            testNode,
            storPoolDfn,
            DeviceProviderKind.LVM,
            getFreeSpaceMgr(storPoolDfn, testNode),
            false
        );
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);

        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder(TEST_RSC_NAME)
            .setLayerStack(new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE)))
            .build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);
        volumeDefinitionTestFactory.builder(TEST_RSC_NAME, 0)
            .setSize(100 * 1024L)
            .build();

        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, TEST_SP_NAME);
        ctrlRscCrtApiHelper.createResourceDb(
            TEST_NODE_NAME,
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

    private class CreateStorPoolDfnCall extends AbsApiCallTester
    {
        private String storPoolName;
        private final Map<String, String> props;

        CreateStorPoolDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_STOR_POOL_DFN,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            storPoolName = TEST_SP_NAME;
            props = new TreeMap<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().createStoragePoolDefinition(
                storPoolName,
                props
            );
        }

        public CreateStorPoolDfnCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public CreateStorPoolDfnCall setProp(String key, String value)
        {
            props.put(key, value);
            return this;
        }
    }

    private class ModifyStorPoolDfnCall extends AbsApiCallTester
    {
        private java.util.UUID storPoolDfnUuid;
        private String storPoolName;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deletePropNamespaces;

        ModifyStorPoolDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_STOR_POOL_DFN,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            storPoolDfnUuid = null; // default: do not check against uuid
            storPoolName = TEST_SP_NAME;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deletePropNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            ctrlApiCallHandlerProvider.get().modifyStorPoolDfn(
                storPoolDfnUuid,
                storPoolName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public ModifyStorPoolDfnCall storPoolDfnUuid(java.util.UUID uuid)
        {
            storPoolDfnUuid = uuid;
            return this;
        }

        public ModifyStorPoolDfnCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public ModifyStorPoolDfnCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        public ModifyStorPoolDfnCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }
    }

    private class DeleteStorPoolDfnCall extends AbsApiCallTester
    {
        private String storPoolName;

        DeleteStorPoolDfnCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_STOR_POOL_DFN,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            storPoolName = TEST_SP_NAME;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().deleteStoragePoolDefinition(storPoolName);
        }

        public DeleteStorPoolDfnCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }
    }

    private class CreateStorPoolCall extends AbsApiCallTester
    {
        private String nodeName;
        private String storPoolName;
        private DeviceProviderKind providerKind;
        private final Map<String, String> props;

        CreateStorPoolCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_STOR_POOL,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            storPoolName = TEST_SP_NAME;
            providerKind = DeviceProviderKind.DISKLESS;
            props = new TreeMap<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            storPoolCrtApiCallHandlerProvider.get().createStorPool(
                nodeName,
                storPoolName,
                providerKind,
                null,
                false,
                props,
                Flux.empty()
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public CreateStorPoolCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public CreateStorPoolCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public CreateStorPoolCall setProviderKind(DeviceProviderKind providerKindRef)
        {
            providerKind = providerKindRef;
            return this;
        }
    }

    private class ModifyStorPoolCall extends AbsApiCallTester
    {
        private java.util.UUID storPoolUuid;
        private String nodeName;
        private String storPoolName;
        private final Map<String, String> overrideProps;
        private final Set<String> deletePropKeys;
        private final Set<String> deletePropNamespaces;

        ModifyStorPoolCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_STOR_POOL,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            storPoolUuid = null; // default: do not check against uuid
            nodeName = TEST_NODE_NAME;
            storPoolName = TEST_SP_NAME;
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deletePropNamespaces = new TreeSet<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            storPoolApiCallHandlerProvider.get().modify(
                storPoolUuid,
                nodeName,
                storPoolName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public ModifyStorPoolCall storPoolUuid(java.util.UUID uuid)
        {
            storPoolUuid = uuid;
            return this;
        }

        public ModifyStorPoolCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public ModifyStorPoolCall overrideProps(String key, String value)
        {
            overrideProps.put(key, value);
            return this;
        }

        public ModifyStorPoolCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }
    }

    private class DeleteStorPoolCall extends AbsApiCallTester
    {
        private String nodeName;
        private String storPoolName;

        DeleteStorPoolCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_STOR_POOL,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            storPoolName = TEST_SP_NAME;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            storPoolApiCallHandlerProvider.get().deleteStorPool(
                nodeName,
                storPoolName
            )
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public DeleteStorPoolCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public DeleteStorPoolCall setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }
    }
}
