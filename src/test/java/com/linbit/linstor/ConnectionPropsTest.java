package com.linbit.linstor;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.NodeConnectionData;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.core.objects.ResourceConnectionData;
import com.linbit.linstor.core.objects.ResourceData;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.VolumeConnectionData;
import com.linbit.linstor.core.objects.VolumeData;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.core.objects.Node.NodeType;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

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
    private Integer nodeId1;
    private Integer nodeId2;
    private VolumeNumber volNr;
    private Integer minor;
    private long volSize;

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
        seedDefaultPeerRule.setDefaultPeerAccessContext(SYS_CTX);
        super.setUpAndEnterScope();

        nodeName1 = new NodeName("Node1");
        nodeName2 = new NodeName("Node2");
        resName = new ResourceName("ResName");
        resDfnPort = 4242;
        resDfnTransportType = TransportType.IP;
        storPoolName = new StorPoolName("StorPool");
        nodeId1 = 1;
        nodeId2 = 2;
        volNr = new VolumeNumber(13);
        minor = 12;
        volSize = 9001;

        node1 = nodeDataFactory.create(SYS_CTX, nodeName1, NodeType.SATELLITE, null);
        node2 = nodeDataFactory.create(SYS_CTX, nodeName2, NodeType.SATELLITE, null);

        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName,
            null,
            resDfnPort,
            null,
            "secret",
            resDfnTransportType,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            null
        );

        res1 = resourceDataFactory.create(SYS_CTX, resDfn, node1, nodeId1, null, Collections.emptyList());
        res2 = resourceDataFactory.create(SYS_CTX, resDfn, node2, nodeId2, null, Collections.emptyList());

        storPoolDfn = storPoolDefinitionDataFactory.create(SYS_CTX, storPoolName);

        storPool1 = storPoolDataFactory.create(
            SYS_CTX, node1, storPoolDfn, DeviceProviderKind.LVM, getFreeSpaceMgr(storPoolDfn, node1)
        );
        storPool2 = storPoolDataFactory.create(
            SYS_CTX, node2, storPoolDfn, DeviceProviderKind.LVM, getFreeSpaceMgr(storPoolDfn, node2)
        );

        volDfn = volumeDefinitionDataFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);

        vol1 = volumeDataFactory.create(
            SYS_CTX,
            res1,
            volDfn,
            null,
            Collections.singletonMap("", storPool1)
        );
        vol2 = volumeDataFactory.create(
            SYS_CTX,
            res2,
            volDfn,
            null,
            Collections.singletonMap("", storPool2)
        );

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
    public void test() throws InvalidKeyException, AccessDeniedException, InvalidValueException, DatabaseException
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
