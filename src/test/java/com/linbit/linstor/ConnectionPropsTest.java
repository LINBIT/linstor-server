package com.linbit.linstor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

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

    private Node node1;
    private Node node2;
    private ResourceDefinition resDfn;
    private Resource res1;
    private Resource res2;
    private StorPoolDefinition storPoolDfn;
    private StorPool storPool1;
    private StorPool storPool2;
    private VolumeDefinition volDfn;
    private Volume vol1;
    private Volume vol2;

    private NodeConnection nodeCon;
    private ResourceConnection resCon;
    private VolumeConnection volCon;

    private Props nodeConProps;
    private Props resConProps;
    private Props volConProps;

    private PriorityProps conProps;
    private ResourceGroup dfltRscGrp;

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

        node1 = nodeFactory.create(SYS_CTX, nodeName1, Node.Type.SATELLITE, null);
        node2 = nodeFactory.create(SYS_CTX, nodeName2, Node.Type.SATELLITE, null);

        dfltRscGrp = createDefaultResourceGroup(SYS_CTX);

        resDfn = resourceDefinitionFactory.create(
            SYS_CTX,
            resName,
            null,
            resDfnPort,
            null,
            "secret",
            resDfnTransportType,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            null,
            dfltRscGrp
        );

        res1 = resourceFactory.create(SYS_CTX, resDfn, node1, nodeId1, null, Collections.emptyList());
        res2 = resourceFactory.create(SYS_CTX, resDfn, node2, nodeId2, null, Collections.emptyList());

        storPoolDfn = storPoolDefinitionFactory.create(SYS_CTX, storPoolName);

        storPool1 = storPoolFactory.create(
            SYS_CTX, node1, storPoolDfn, DeviceProviderKind.LVM, getFreeSpaceMgr(storPoolDfn, node1)
        );
        storPool2 = storPoolFactory.create(
            SYS_CTX, node2, storPoolDfn, DeviceProviderKind.LVM, getFreeSpaceMgr(storPoolDfn, node2)
        );

        volDfn = volumeDefinitionFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);

        vol1 = volumeFactory.create(
            SYS_CTX,
            res1,
            volDfn,
            null,
            Collections.singletonMap("", storPool1)
        );
        vol2 = volumeFactory.create(
            SYS_CTX,
            res2,
            volDfn,
            null,
            Collections.singletonMap("", storPool2)
        );

        nodeCon = nodeConnectionFactory.create(SYS_CTX, node1, node2);
        resCon = resourceConnectionFactory.create(SYS_CTX, res1, res2, null);
        volCon = volumeConnectionFactory.create(SYS_CTX, vol1, vol2);

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
