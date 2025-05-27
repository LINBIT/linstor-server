package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.core.objects.VolumeConnectionDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscPayload;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Triple;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VolumeConnectionDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_VLM_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " + NODE_NAME_DST + ", " +
                     RESOURCE_NAME + ", " + VLM_NR +
        " FROM " + TBL_VOLUME_CONNECTIONS;

    private final NodeName sourceName;
    private final NodeName targetName;
    private final ResourceName resName;
    private final Set<Integer> resPorts;
    private final StorPoolName storPoolName;
    private final VolumeNumber volNr;

    private final Integer minor;
    private final long volSize;

    private java.util.UUID uuid;

    private Node nodeSrc;
    private Node nodeDst;
    private ResourceDefinition resDfn;
    private VolumeDefinition volDfn;
    private Resource resSrc;
    private Resource resDst;
    private StorPoolDefinition storPoolDfn;
    private StorPool storPool1;
    private StorPool storPool2;
    private Volume volSrc;
    private Volume volDst;

    @Inject
    private VolumeConnectionDbDriver driver;

    private Integer nodeIdSrc;
    private Integer nodeIdDst;

    @SuppressWarnings("checkstyle:magicnumber")
    public VolumeConnectionDbDriverTest() throws InvalidNameException, ValueOutOfRangeException
    {
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
        resName = new ResourceName("testResourceName");
        resPorts = Collections.singleton(9001);
        storPoolName = new StorPoolName("testStorPool");
        volNr = new VolumeNumber(42);

        minor = 43;
        volSize = 9001;

    }

    @Before
    @SuppressWarnings("checkstyle:magicnumber")
    public void setUp() throws Exception
    {
        seedDefaultPeerRule.setDefaultPeerAccessContext(SYS_CTX);
        super.setUpAndEnterScope();

        assertEquals(
            TBL_VOLUME_CONNECTIONS + " table's column count has changed. Update tests accordingly!",
            5,
            TBL_COL_COUNT_VOLUME_CONNECTIONS
        );

        uuid = randomUUID();

        nodeSrc = nodeFactory.create(SYS_CTX, sourceName, Node.Type.SATELLITE, null);
        nodesMap.put(nodeSrc.getName(), nodeSrc);
        nodeDst = nodeFactory.create(SYS_CTX, targetName, Node.Type.SATELLITE, null);
        nodesMap.put(nodeDst.getName(), nodeDst);

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = "secret";
        drbdRscDfn.transportType = TransportType.IP;
        resDfn = resourceDefinitionFactory.create(
            SYS_CTX,
            resName,
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup(SYS_CTX)
        );
        rscDfnMap.put(resDfn.getName(), resDfn);
        volDfn = volumeDefinitionFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);

        nodeIdSrc = 13;
        nodeIdDst = 14;

        LayerPayload payloadSrc = new LayerPayload();
        DrbdRscPayload drbdRsc1 = payloadSrc.getDrbdRsc();
        drbdRsc1.nodeId = nodeIdSrc;
        drbdRsc1.tcpPorts = resPorts;
        LayerPayload payloadDst = new LayerPayload();
        DrbdRscPayload drbdRsc2 = payloadDst.getDrbdRsc();
        drbdRsc2.nodeId = nodeIdDst;
        drbdRsc2.tcpPorts = resPorts;
        resSrc = resourceFactory.create(SYS_CTX, resDfn, nodeSrc, payloadSrc, null, Collections.emptyList());
        resDst = resourceFactory.create(SYS_CTX, resDfn, nodeDst, payloadDst, null, Collections.emptyList());

        storPoolDfn = storPoolDefinitionFactory.create(SYS_CTX, storPoolName);
        storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

        storPool1 = storPoolFactory.create(
            SYS_CTX,
            nodeSrc,
            storPoolDfn,
            DeviceProviderKind.LVM,
            getFreeSpaceMgr(storPoolDfn, nodeSrc),
            false
        );
        storPool2 = storPoolFactory.create(
            SYS_CTX,
            nodeDst,
            storPoolDfn,
            DeviceProviderKind.LVM,
            getFreeSpaceMgr(storPoolDfn, nodeDst),
            false
        );

        LayerPayload payload1 = new LayerPayload();
        payload1.putStorageVlmPayload("", volDfn.getVolumeNumber().value, storPool1);
        volSrc = volumeFactory.create(
            SYS_CTX,
            resSrc,
            volDfn,
            null,
            payload1,
            null,
            Collections.emptyMap(),
            null
        );
        LayerPayload payload2 = new LayerPayload();
        payload2.putStorageVlmPayload("", volDfn.getVolumeNumber().value, storPool2);
        volDst = volumeFactory.create(
            SYS_CTX,
            resDst,
            volDfn,
            null,
            payload2,
            null,
            Collections.emptyMap(),
            null
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeConnection volCon = TestFactory.createVolumeConnection(
            uuid,
            volSrc,
            volDst,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            SYS_CTX
        );
        driver.create(volCon);
        commit();

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        volumeConnectionFactory.create(SYS_CTX, volSrc, volDst);
        commit();

        checkDbPersist(false);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        VolumeConnection volCon = TestFactory.createVolumeConnection(
            uuid,
            volSrc,
            volDst,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            SYS_CTX
        );
        driver.create(volCon);

        Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume> vlmMap = new HashMap<>();
        addToMap(vlmMap, volSrc);
        addToMap(vlmMap, volDst);
        List<VolumeConnection> cons = driver.loadAllAsList(vlmMap);

        assertNotNull(cons);

        assertEquals(1, cons.size());

        VolumeConnection loadedConDfn = cons.get(0);
        assertNotNull(loadedConDfn);

        checkLoadedConDfn(loadedConDfn, true);
    }

    private void addToMap(
        Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume> vlmMap,
        Volume vol
    )
    {
        vlmMap.put(new Triple<>(
                vol.getAbsResource().getNode().getName(),
                vol.getResourceDefinition().getName(),
                vol.getVolumeDefinition().getVolumeNumber()
            ),
            vol
        );
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        VolumeConnection volCon = TestFactory.createVolumeConnection(
            uuid,
            volSrc,
            volDst,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            SYS_CTX
        );
        driver.create(volCon);
        volSrc.setVolumeConnection(SYS_CTX, volCon);
        volDst.setVolumeConnection(SYS_CTX, volCon);

        VolumeConnection loadedConDfn = VolumeConnection.get(
            SYS_CTX,
            volSrc,
            volDst
        );

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testCache() throws Exception
    {
        VolumeConnection storedInstance = volumeConnectionFactory.create(
            SYS_CTX,
            volSrc,
            volDst
        );

        // no clear-cache

        assertEquals(storedInstance, VolumeConnection.get(
            SYS_CTX,
            volSrc,
            volDst
        ));
    }

    @Test
    public void testDelete() throws Exception
    {
        VolumeConnection volCon = TestFactory.createVolumeConnection(
            uuid,
            volSrc,
            volDst,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            SYS_CTX
        );
        driver.create(volCon);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VLM_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(volCon);
        commit();

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VLM_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        if (checkUuid)
        {
            assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        }
        assertEquals(sourceName.value, resultSet.getString(NODE_NAME_SRC));
        assertEquals(targetName.value, resultSet.getString(NODE_NAME_DST));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    private void checkLoadedConDfn(VolumeConnection loadedConDfn, boolean checkUuid) throws AccessDeniedException
    {
        assertNotNull(loadedConDfn);
        if (checkUuid)
        {
            assertEquals(uuid, loadedConDfn.getUuid());
        }
        Volume sourceVolume = loadedConDfn.getSourceVolume(SYS_CTX);
        Volume targetVolume = loadedConDfn.getTargetVolume(SYS_CTX);

        assertEquals(sourceName, sourceVolume.getAbsResource().getNode().getName());
        assertEquals(targetName, targetVolume.getAbsResource().getNode().getName());
        assertEquals(resName, sourceVolume.getResourceDefinition().getName());
        assertEquals(sourceVolume.getResourceDefinition(), targetVolume.getResourceDefinition());
        assertEquals(volNr, sourceVolume.getVolumeDefinition().getVolumeNumber());
        assertEquals(sourceVolume.getVolumeDefinition(), targetVolume.getVolumeDefinition());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        volumeConnectionFactory.create(SYS_CTX, volSrc, volDst);
        volumeConnectionFactory.create(SYS_CTX, volSrc, volDst);
    }
}
