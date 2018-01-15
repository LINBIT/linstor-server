package com.linbit.linstor.api;

import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.Controller;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Controller.class)
@PowerMockIgnore({"javax.*", "com.sun.*"})
public class NodeApiTest extends ApiTestBase
{
    public NodeApiTest()
    {
        super();
    }

    @Test
    public void createSuccessTest() throws Exception
    {
        String nodeName = "TestNode";
        ApiCallRc rc = apiCallHandler.createNode(
            PUBLIC_CTX,
            null, // peer
            nodeName,
            ApiConsts.VAL_NODE_TYPE_STLT,
            Arrays.asList(
                createNetInterfaceApi("tcp0", "127.0.0.1")
            ),
            Arrays.asList(
                createStltConnApi("tcp0")
            ),
            null // props
        );

        Assert.assertTrue(rc.getEntries().size() == 1);

        RcEntry rcEntry = rc.getEntries().get(0);
        Assert.assertEquals(rcEntry.getReturnCode(), ApiConsts.RC_NODE_CREATED);

        Assert.assertTrue(nodesMap.size() == 1);
        Assert.assertNotNull(nodesMap.get(new NodeName(nodeName)));
    }

    @Test
    public void createFailMissingNetComTest() throws Exception
    {
        ApiCallRc rc = apiCallHandler.createNode(
            PUBLIC_CTX,
            null, // peer
            "TestNode",
            ApiConsts.VAL_NODE_TYPE_STLT,
            Arrays.<NetInterfaceApi>asList(), // no netIf
            Arrays.<SatelliteConnectionApi>asList(), // no satellite connections
            null // props
        );

        Assert.assertTrue(rc.getEntries().size() == 1);

        RcEntry rcEntry = rc.getEntries().get(0);
        expectRc(rcEntry, ApiConsts.RC_NODE_CRT_FAIL_MISSING_NETCOM);
    }

    @Test
    public void createFailMissingSatelliteConnectionTest() throws Exception
    {
        ApiCallRc rc = apiCallHandler.createNode(
            PUBLIC_CTX,
            null, // peer
            "TestNode",
            ApiConsts.VAL_NODE_TYPE_STLT,
            Arrays.asList(
                createNetInterfaceApi("tcp0", "127.0.0.1")
            ),
            Arrays.<SatelliteConnectionApi>asList(), // no satellite connections
            null // props
        );

        Assert.assertTrue(rc.getEntries().size() == 1);

        RcEntry rcEntry = rc.getEntries().get(0);
        expectRc(rcEntry, ApiConsts.RC_NODE_CRT_FAIL_MISSING_STLT_CONN);
    }

    @Test
    public void createFailAlreadyExists() throws Exception
    {
        ApiCallRc rc = apiCallHandler.createNode(
            PUBLIC_CTX,
            null, // peer
            "TestNode",
            ApiConsts.VAL_NODE_TYPE_STLT,
            Arrays.asList(
                createNetInterfaceApi("tcp0", "127.0.0.1")
            ),
            Arrays.asList(
                createStltConnApi("tcp0")
            ),
            null // props
        );

        Assert.assertTrue(rc.getEntries().size() == 1);

        RcEntry rcEntry = rc.getEntries().get(0);
        Assert.assertEquals(rcEntry.getReturnCode(), ApiConsts.RC_NODE_CREATED);



        rc = apiCallHandler.createNode(
            PUBLIC_CTX,
            null, // peer
            "TestNode",
            ApiConsts.VAL_NODE_TYPE_STLT,
            Arrays.asList(
                createNetInterfaceApi("tcp0", "127.0.0.1")
            ),
            Arrays.asList(
                createStltConnApi("tcp0")
            ),
            null // props
        );

        Assert.assertTrue(rc.getEntries().size() == 1);

        rcEntry = rc.getEntries().get(0);
        Assert.assertEquals(rcEntry.getReturnCode(), ApiConsts.RC_NODE_CRT_FAIL_EXISTS_NODE);
    }
}
