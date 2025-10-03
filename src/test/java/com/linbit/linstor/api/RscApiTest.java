package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.apidata.RscApiData;
import com.linbit.linstor.proto.apidata.VlmApiData;
import com.linbit.linstor.proto.common.LayerTypeOuterClass.LayerType;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.RscOuterClass;
import com.linbit.linstor.proto.common.RscOuterClass.Rsc;
import com.linbit.linstor.proto.common.StorageRscOuterClass.StorageRsc;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class RscApiTest extends ApiTestBase
{
    @Inject private CtrlRscCrtApiCallHandler rscCrtApiCallHandler;

    private NodeName testControllerName;
    private Node.Type testControllerType;
    private Node.Flags[] testControllerFlags;
    private Node testControllerNode;

    private NodeName testSatelliteName;
    private Node.Type testSatelliteType;
    private Node.Flags[] testSatelliteFlags;
    private Node testSatelliteNode;

    private ResourceName testRscName;
    private ResourceDefinition.Flags[] testRscDfnFlags;
    private String testRscDfnSecret;
    private TransportType tesTRscDfnTransportType;
    private ResourceDefinition testRscDfn;

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @SuppressWarnings("checkstyle:magicnumber")
    public RscApiTest() throws Exception
    {
        testControllerName = new NodeName("TestController");
        testControllerType = Node.Type.CONTROLLER;
        testControllerFlags = null;

        testSatelliteName = new NodeName("TestSatellite");
        testSatelliteType = Node.Type.SATELLITE;
        testSatelliteFlags = null;

        testRscName = new ResourceName("TestRsc");
        testRscDfnFlags = null;
        testRscDfnSecret = "notTellingYou";
        tesTRscDfnTransportType = TransportType.IP;
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        testControllerNode = nodeFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testControllerName,
            testControllerType,
            testControllerFlags
        );
        nodesMap.put(testControllerName, testControllerNode);

        Mockito.when(mockSatellite.apiCall(anyString(), any())).thenReturn(Flux.empty());
        testSatelliteNode = nodeFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testSatelliteName,
            testSatelliteType,
            testSatelliteFlags // flags
        );
        testSatelliteNode.setPeer(GenericDbBase.SYS_CTX, mockSatellite);
        nodesMap.put(testSatelliteName, testSatelliteNode);

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any())).thenReturn(Mono.just(Collections.emptyMap()));

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = testRscDfnSecret;
        drbdRscDfn.transportType = tesTRscDfnTransportType;
        testRscDfn = resourceDefinitionFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testRscName,
            null,
            testRscDfnFlags,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup(BOB_ACC_CTX)
        );
        rscDfnMap.put(testRscName, testRscDfn);

        ctrlConf.setProp(
            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
            java.util.UUID.randomUUID().toString(),
            ApiConsts.NAMESPC_CLUSTER
        );

        commitAndCleanUp(true);
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void createRscSuccess() throws Exception
    {
        Mockito.when(mockPeer.getAccessContext()).thenReturn(BOB_ACC_CTX);
        Mockito.when(mockPeer.isOnline()).thenReturn(true);
        Mockito.when(mockSatellite.getExtToolsManager()).thenReturn(mockExtToolsMgr);
        Mockito.when(mockSatellite.isOnline()).thenReturn(true);
        Mockito.when(mockExtToolsMgr.getSupportedLayers())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceLayerKind.values())));
        Mockito.when(mockExtToolsMgr.getSupportedProviders())
            .thenReturn(new TreeSet<>(Arrays.asList(DeviceProviderKind.values())));
        evaluateTest(
            new CrtRscCall(
                // Registered
                ApiConsts.CREATED,
                // Deployed
                ApiConsts.MODIFIED,
                // No volumes => WARN_NOT_FOUND response
                ApiConsts.WARN_NOT_FOUND,
                ApiConsts.MASK_INFO // updated resync-after entries
            )
        );
    }

    private class CrtRscCall extends AbsApiCallTester
    {
        private String nodeName;
        private String rscName;
        private Map<String, String> rscPropsMap;
        private List<VolumeApi> vlmApiDataList;
        private List<String> flags;

        CrtRscCall(long... expectedRc)
        {
            super(
                // peer
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_CRT,
                expectedRc
            );

            nodeName = testSatelliteName.displayValue;
            rscName = testRscName.displayValue;
            rscPropsMap = new TreeMap<>();
            vlmApiDataList = new ArrayList<>();
            flags = new ArrayList<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscCrtApiCallHandler.createResource(
                Collections.singletonList(
                    new RscWithPayloadApiData(
                        RscOuterClass.Rsc.newBuilder()
                            .setNodeName(nodeName)
                            .setName(rscName)
                            .addAllRscFlags(flags)
                            .putAllProps(rscPropsMap)
                            .addAllVlms(VlmApiData.toVlmProtoList(vlmApiDataList))
                            .setLayerObject(
                                RscLayerData.newBuilder()
                                    .setId(0)
                                    .setRscNameSuffix("")
                                    .setLayerType(LayerType.STORAGE)
                                    .setStorage(
                                        StorageRsc.newBuilder()
                                            .build()
                                    )
                                    .setSuspend(false)
                                    .build()
                            )
                            .build()
                    )
                ),
                Resource.DiskfulBy.USER,
                false,
                Collections.emptyList()
            )
            .contextWrite(contextWrite()).toStream().forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

    }

    private class RscWithPayloadApiData implements ResourceWithPayloadApi
    {
        private RscApiData rscApi;
        private Integer drbdNodeId;
        private List<String> layerStackList;
        private @Nullable Integer portCount;
        private @Nullable List<Integer> ports;

        RscWithPayloadApiData(Rsc rscRef)
        {
            this(rscRef, null, Collections.emptyList());
        }

        RscWithPayloadApiData(Rsc rscRef, Integer drbdNodeIdRef, List<String> layerStackListRef)
        {
            rscApi = new RscApiData(rscRef, 0, 0);
            drbdNodeId = drbdNodeIdRef;
            layerStackList = layerStackListRef;
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
        public Integer getDrbdNodeId()
        {
            return drbdNodeId;
        }

        @Override
        public @Nullable Integer getPortCount()
        {
            return portCount;
        }

        @Override
        public @Nullable List<Integer> getPorts()
        {
            return ports;
        }
    }
}
