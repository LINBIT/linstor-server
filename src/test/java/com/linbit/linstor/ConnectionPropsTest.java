package com.linbit.linstor;

import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.LvmDriver;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectionPropsTest extends GenericDbBase
{
    private NodeName nodeName1;
    private NodeName nodeName2;
    private ResourceName resName;
    private Integer resDfnPort;
    private TransportType resDfnTransportType;
    private StorPoolName storPoolName;
    private NodeId nodeId1;
    private NodeId nodeId2;
    private VolumeNumber volNr;
    private Integer minor;
    private long volSize;
    private String blockDev1;
    private String metaDisk1;
    private String blockDev2;
    private String metaDisk2;

    private NodeData node1;
    private NodeData node2;
    private ResourceDefinitionData resDfn;
    private ResourceData res1;
    private ResourceData res2;
    private StorPoolDefinitionData storPoolDfn;
    private StorPoolData storPool1;
    private StorPoolData storPool2;
    private VolumeDefinitionData volDfn;
    private VolumeData vol1;
    private VolumeData vol2;

    private NodeConnectionData nodeCon;
    private ResourceConnectionData resCon;
    private VolumeConnectionData volCon;

    private Props nodeConProps;
    private Props resConProps;
    private Props volConProps;

    private PriorityProps conProps;

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();

        nodeName1 = new NodeName("Node1");
        nodeName2 = new NodeName("Node2");
        resName = new ResourceName("ResName");
        resDfnPort = 4242;
        resDfnTransportType = TransportType.IP;
        storPoolName = new StorPoolName("StorPool");
        nodeId1 = new NodeId(1);
        nodeId2 = new NodeId(2);
        volNr = new VolumeNumber(13);
        minor = 12;
        volSize = 9001;
        blockDev1 = "/dev/vol1/block";
        metaDisk1 = "/dev/vol1/meta";
        blockDev2 = "/dev/vol2/block";
        metaDisk2 = "/dev/vol2/meta";

        node1 = nodeDataFactory.create(SYS_CTX, nodeName1, NodeType.SATELLITE, null);
        node2 = nodeDataFactory.create(SYS_CTX, nodeName2, NodeType.SATELLITE, null);

        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX, resName, resDfnPort, null, "secret", resDfnTransportType
        );

        res1 = resourceDataFactory.create(SYS_CTX, resDfn, node1, nodeId1, null);
        res2 = resourceDataFactory.create(SYS_CTX, resDfn, node2, nodeId2, null);

        storPoolDfn = storPoolDefinitionDataFactory.create(SYS_CTX, storPoolName);

        storPool1 = storPoolDataFactory.create(
            SYS_CTX, node1, storPoolDfn, LvmDriver.class.getSimpleName(), getFreeSpaceMgr(storPoolDfn, node1)
        );
        storPool2 = storPoolDataFactory.create(
            SYS_CTX, node2, storPoolDfn, LvmDriver.class.getSimpleName(), getFreeSpaceMgr(storPoolDfn, node2)
        );

        volDfn = volumeDefinitionDataFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);

        vol1 = volumeDataFactory.create(SYS_CTX, res1, volDfn, storPool1, blockDev1, metaDisk1, null);
        vol2 = volumeDataFactory.create(SYS_CTX, res2, volDfn, storPool2, blockDev2, metaDisk2, null);

        nodeCon = nodeConnectionDataFactory.create(SYS_CTX, node1, node2);
        resCon = resourceConnectionDataFactory.create(SYS_CTX, res1, res2, null);
        volCon = volumeConnectionDataFactory.create(SYS_CTX, vol1, vol2);

        nodeConProps = nodeCon.getProps(SYS_CTX);
        resConProps = resCon.getProps(SYS_CTX);
        volConProps = volCon.getProps(SYS_CTX);

        conProps = new PriorityProps(SYS_CTX, nodeCon, resCon, volCon);
    }

    @SuppressWarnings("checkstyle:variabledeclarationusagedistance")
    @Test
    public void test() throws InvalidKeyException, AccessDeniedException, InvalidValueException, SQLException
    {
        String testKey = "testKey";
        String testValue1 = "testValue1";
        String testValue2 = "testValue2";
        String testValue3 = "testValue3";
        String testValue4 = "testValue4";

        assertNull(conProps.getProp(testKey));

        volConProps.setProp(testKey, testValue1);
        assertEquals(testValue1, conProps.getProp(testKey));

        resConProps.setProp(testKey, testValue2);
        assertEquals(testValue1, conProps.getProp(testKey));

        nodeConProps.setProp(testKey, testValue3);
        assertEquals(testValue1, conProps.getProp(testKey));

        volConProps.removeProp(testKey);
        assertEquals(testValue2, conProps.getProp(testKey));

        resConProps.removeProp(testKey);
        assertEquals(testValue3, conProps.getProp(testKey));

        volConProps.setProp(testKey, testValue4);
        assertEquals(testValue4, conProps.getProp(testKey));
    }

}
