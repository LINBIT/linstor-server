package com.linbit.linstor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import com.linbit.linstor.core.objects.VolumeConnectionGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinition;
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

import org.junit.Before;
import org.junit.Test;

public class VolumeConnectionGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_VLM_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " + NODE_NAME_DST + ", " +
                     RESOURCE_NAME + ", " + VLM_NR +
        " FROM " + TBL_VOLUME_CONNECTIONS;

    private final NodeName sourceName;
    private final NodeName targetName;
    private final ResourceName resName;
    private final Integer resPort;
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

    @Inject private VolumeConnectionGenericDbDriver driver;

    private Integer nodeIdSrc;
    private Integer nodeIdDst;

    @SuppressWarnings("checkstyle:magicnumber")
    public VolumeConnectionGenericDbDriverTest() throws InvalidNameException, ValueOutOfRangeException
    {
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
        resName = new ResourceName("testResourceName");
        resPort = 9001;
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

        resDfn = resourceDefinitionFactory.create(
            SYS_CTX,
            resName,
            null,
            resPort,
            null,
            "secret",
            TransportType.IP,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            null,
            createDefaultResourceGroup(SYS_CTX)
        );
        rscDfnMap.put(resDfn.getName(), resDfn);
        volDfn = volumeDefinitionFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);

        nodeIdSrc = 13;
        nodeIdDst = 14;

        resSrc = resourceFactory.create(SYS_CTX, resDfn, nodeSrc, nodeIdSrc, null, Collections.emptyList());
        resDst = resourceFactory.create(SYS_CTX, resDfn, nodeDst, nodeIdDst, null, Collections.emptyList());

        storPoolDfn = storPoolDefinitionFactory.create(SYS_CTX, storPoolName);
        storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

        storPool1 = storPoolFactory.create(
            SYS_CTX, nodeSrc, storPoolDfn, DeviceProviderKind.LVM, getFreeSpaceMgr(storPoolDfn, nodeSrc)
        );
        storPool2 = storPoolFactory.create(
            SYS_CTX, nodeDst, storPoolDfn, DeviceProviderKind.LVM, getFreeSpaceMgr(storPoolDfn, nodeDst)
        );

        volSrc = volumeFactory.create(
            SYS_CTX,
            resSrc,
            volDfn,
            null,
            Collections.singletonMap("", storPool1)
        );
        volDst = volumeFactory.create(
            SYS_CTX,
            resDst,
            volDfn,
            null,
            Collections.singletonMap("", storPool2)
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
            transMgrProvider
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
            transMgrProvider
        );
        driver.create(volCon);

        Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume> vlmMap = new HashMap<>();
        addToMap(vlmMap, volSrc);
        addToMap(vlmMap, volDst);
        List<VolumeConnection> cons = driver.loadAll(vlmMap);

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
        vlmMap.put(new Triple<NodeName, ResourceName, VolumeNumber>(
                vol.getResource().getAssignedNode().getName(),
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
            transMgrProvider
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
            transMgrProvider
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

        assertEquals(sourceName, sourceVolume.getResource().getAssignedNode().getName());
        assertEquals(targetName, targetVolume.getResource().getAssignedNode().getName());
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
