package com.linbit.linstor.api;

import javax.inject.Inject;
import javax.inject.Provider;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler;

import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RunWith(JUnitParamsRunner.class)
public class RscApiTest extends ApiTestBase
{
    @Inject private Provider<CtrlRscApiCallHandler> rscApiCallHandlerProvider;

    private NodeName testControllerName;
    private NodeType testControllerType;
    private NodeFlag[] testControllerFlags;
    private NodeData testControllerNode;

    private NodeName testSatelliteName;
    private NodeType testSatelliteType;
    private NodeFlag[] testSatelliteFlags;
    private NodeData testSatelliteNode;

    private ResourceName testRscName;
    private Integer testRscDfnPort;
    private RscDfnFlags[] testRscDfnFlags;
    private String testRscDfnSecret;
    private TransportType tesTRscDfnTransportType;
    private ResourceDefinitionData testRscDfn;

    @SuppressWarnings("checkstyle:magicnumber")
    public RscApiTest() throws Exception
    {
        super();
        testControllerName = new NodeName("TestController");
        testControllerType = NodeType.CONTROLLER;
        testControllerFlags = null;

        testSatelliteName = new NodeName("TestSatellite");
        testSatelliteType = NodeType.SATELLITE;
        testSatelliteFlags = null;

        testRscName = new ResourceName("TestRsc");
        testRscDfnPort = 4242;
        testRscDfnFlags = null;
        testRscDfnSecret = "notTellingYou";
        tesTRscDfnTransportType = TransportType.IP;
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        testControllerNode = nodeDataFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testControllerName,
            testControllerType,
            testControllerFlags
        );
        nodesMap.put(testControllerName, testControllerNode);
        testSatelliteNode = nodeDataFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testSatelliteName,
            testSatelliteType,
            testSatelliteFlags // flags
        );
        nodesMap.put(testSatelliteName, testSatelliteNode);

        testRscDfn = resourceDefinitionDataFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testRscName,
            testRscDfnPort,
            testRscDfnFlags,
            testRscDfnSecret,
            tesTRscDfnTransportType
        );
        rscDfnMap.put(testRscName, testRscDfn);
    }

    @Test
    public void createRscSuccess() throws Exception
    {
        evaluateTest(
            new CrtRscCall(ApiConsts.CREATED)
        );
    }

    private class CrtRscCall extends AbsApiCallTester
    {
        private String nodeName;
        private String rscName;
        private Map<String, String> rscPropsMap;
        private List<VlmApi> vlmApiDataList;
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
            return rscApiCallHandlerProvider.get().createResource(
                nodeName,
                rscName,
                flags,
                rscPropsMap,
                vlmApiDataList
            );
        }

    }
}
