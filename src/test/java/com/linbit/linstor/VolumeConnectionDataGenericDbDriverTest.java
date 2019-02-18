package com.linbit.linstor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.utils.Triple;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class VolumeConnectionDataGenericDbDriverTest extends GenericDbBase
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

    private final String volBlockDevSrc;
    private final String volMetaDiskPathSrc;
    private final String volBlockDevDst;
    private final String volMetaDiskPathDst;

    private java.util.UUID uuid;

    private NodeData nodeSrc;
    private NodeData nodeDst;
    private ResourceDefinitionData resDfn;
    private VolumeDefinitionData volDfn;
    private ResourceData resSrc;
    private ResourceData resDst;
    private StorPoolDefinitionData storPoolDfn;
    private StorPoolData storPool1;
    private StorPoolData storPool2;
    private VolumeData volSrc;
    private VolumeData volDst;

    @Inject private VolumeConnectionDataGenericDbDriver driver;

    private NodeId nodeIdSrc;
    private NodeId nodeIdDst;

    @SuppressWarnings("checkstyle:magicnumber")
    public VolumeConnectionDataGenericDbDriverTest() throws InvalidNameException, ValueOutOfRangeException
    {
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
        resName = new ResourceName("testResourceName");
        resPort = 9001;
        storPoolName = new StorPoolName("testStorPool");
        volNr = new VolumeNumber(42);

        minor = 43;
        volSize = 9001;

        volBlockDevSrc = "/dev/src/vol/block";
        volMetaDiskPathSrc = "/dev/src/vol/meta";
        volBlockDevDst = "/dev/dst/vol/block";
        volMetaDiskPathDst = "/dev/dst/vol/meta";
    }

    @Before
    @SuppressWarnings("checkstyle:magicnumber")
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_VOLUME_CONNECTIONS + " table's column count has changed. Update tests accordingly!",
            5,
            TBL_COL_COUNT_VOLUME_CONNECTIONS
        );

        uuid = randomUUID();

        nodeSrc = nodeDataFactory.create(SYS_CTX, sourceName, NodeType.SATELLITE, null);
        nodesMap.put(nodeSrc.getName(), nodeSrc);
        nodeDst = nodeDataFactory.create(SYS_CTX, targetName, NodeType.SATELLITE, null);
        nodesMap.put(nodeDst.getName(), nodeDst);

        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX, resName, resPort, null, "secret", TransportType.IP
        );
        rscDfnMap.put(resDfn.getName(), resDfn);
        volDfn = volumeDefinitionDataFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);

        nodeIdSrc = new NodeId(13);
        nodeIdDst = new NodeId(14);

        resSrc = resourceDataFactory.create(SYS_CTX, resDfn, nodeSrc, nodeIdSrc, null);
        resDst = resourceDataFactory.create(SYS_CTX, resDfn, nodeDst, nodeIdDst, null);

        storPoolDfn = storPoolDefinitionDataFactory.create(SYS_CTX, storPoolName);
        storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

        storPool1 = storPoolDataFactory.create(
            SYS_CTX, nodeSrc, storPoolDfn, "LvmDriver", getFreeSpaceMgr(storPoolDfn, nodeSrc)
        );
        storPool2 = storPoolDataFactory.create(
            SYS_CTX, nodeDst, storPoolDfn, "LvmDriver", getFreeSpaceMgr(storPoolDfn, nodeDst)
        );

        volSrc = volumeDataFactory.create(
            SYS_CTX,
            resSrc,
            volDfn,
            storPool1,
            volBlockDevSrc,
            volMetaDiskPathSrc,
            null
        );
        volDst = volumeDataFactory.create(
            SYS_CTX,
            resDst,
            volDfn,
            storPool2,
            volBlockDevDst,
            volMetaDiskPathDst,
            null
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(
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
        volumeConnectionDataFactory.create(SYS_CTX, volSrc, volDst);
        commit();

        checkDbPersist(false);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(
            uuid,
            volSrc,
            volDst,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(volCon);

        Map<Triple<NodeName, ResourceName, VolumeNumber>, VolumeData> vlmMap = new HashMap<>();
        addToMap(vlmMap, volSrc);
        addToMap(vlmMap, volDst);
        List<VolumeConnectionData> cons = driver.loadAll(vlmMap);

        assertNotNull(cons);

        assertEquals(1, cons.size());

        VolumeConnection loadedConDfn = cons.get(0);
        assertNotNull(loadedConDfn);

        checkLoadedConDfn(loadedConDfn, true);
    }

    private void addToMap(
        Map<Triple<NodeName, ResourceName, VolumeNumber>, VolumeData> vlmMap,
        VolumeData vol
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
        VolumeConnectionData volCon = new VolumeConnectionData(
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

        VolumeConnectionData loadedConDfn = VolumeConnectionData.get(
            SYS_CTX,
            volSrc,
            volDst
        );

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testCache() throws Exception
    {
        VolumeConnectionData storedInstance = volumeConnectionDataFactory.create(
            SYS_CTX,
            volSrc,
            volDst
        );

        // no clear-cache

        assertEquals(storedInstance, VolumeConnectionData.get(
            SYS_CTX,
            volSrc,
            volDst
        ));
    }

    @Test
    public void testDelete() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(
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
        volumeConnectionDataFactory.create(SYS_CTX, volSrc, volDst);
        volumeConnectionDataFactory.create(SYS_CTX, volSrc, volDst);
    }
}
