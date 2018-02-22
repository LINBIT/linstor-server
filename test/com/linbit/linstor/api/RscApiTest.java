package com.linbit.linstor.api;

import com.google.inject.Inject;
import com.linbit.TransactionMgr;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.CtrlRscApiCallHandler;
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
    @Inject private CtrlRscApiCallHandler rscApiCallHandler;

    private NodeName testControllerName;
    private NodeType testControllerType;
    private NodeFlag[] testControllerFlags;
    private NodeData testControllerNode;

    private NodeName testSatelliteName;
    private NodeType testSatelliteType;
    private NodeFlag[] testSatelliteFlags;
    private NodeData testSatelliteNode;

    private ResourceName testRscName;
    private TcpPortNumber testRscDfnPort;
    private RscDfnFlags[] testRscDfnFlags;
    private String testRscDfnSecret;
    private TransportType tesTRscDfnTransportType;
    private ResourceDefinitionData testRscDfn;

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
        testRscDfnPort = new TcpPortNumber(4242);
        testRscDfnFlags = null;
        testRscDfnSecret = "notTellingYou";
        tesTRscDfnTransportType = TransportType.IP;
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        TransactionMgr transMgr = new TransactionMgr(dbConnPool);
        testControllerNode = nodeDataFactory.getInstance(
            BOB_ACC_CTX,
            testControllerName,
            testControllerType,
            testControllerFlags,
            transMgr,
            true,
            true
        );
        nodesMap.put(testControllerName, testControllerNode);
        testSatelliteNode = nodeDataFactory.getInstance(
            BOB_ACC_CTX,
            testSatelliteName,
            testSatelliteType,
            testSatelliteFlags, // flags
            transMgr,
            true,
            true
        );
        nodesMap.put(testSatelliteName, testSatelliteNode);

        testRscDfn = resourceDefinitionDataFactory.getInstance(
            BOB_ACC_CTX,
            testRscName,
            testRscDfnPort,
            testRscDfnFlags,
            testRscDfnSecret,
            tesTRscDfnTransportType,
            transMgr,
            true,
            true
        );
        rscDfnMap.put(testRscName, testRscDfn);

        transMgr.commit();

        dbConnPool.returnConnection(transMgr);
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
                BOB_ACC_CTX,
                null, // peer
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
            return rscApiCallHandler.createResource(
                accCtx,
                peer,
                nodeName,
                rscName,
                flags,
                rscPropsMap,
                vlmApiDataList
            );
        }

    }
}
