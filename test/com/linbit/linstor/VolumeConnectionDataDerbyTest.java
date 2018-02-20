package com.linbit.linstor;

import com.google.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.utils.UuidUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VolumeConnectionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_VLM_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " + NODE_NAME_DST + ", " +
                     RESOURCE_NAME + ", " + VLM_NR +
        " FROM " + TBL_VOLUME_CONNECTIONS;

    private final NodeName sourceName;
    private final NodeName targetName;
    private final ResourceName resName;
    private final TcpPortNumber resPort;
    private final StorPoolName storPoolName;
    private final VolumeNumber volNr;

    private final MinorNumber minor;
    private final long volSize;

    private final String volBlockDevSrc;
    private final String volMetaDiskPathSrc;
    private final String volBlockDevDst;
    private final String volMetaDiskPathDst;

    private TransactionMgr transMgr;

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

    @Inject private VolumeConnectionDataDerbyDriver driver;

    private NodeId nodeIdSrc;
    private NodeId nodeIdDst;

    public VolumeConnectionDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
    {
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
        resName = new ResourceName("testResourceName");
        resPort = new TcpPortNumber(9001);
        storPoolName = new StorPoolName("testStorPool");
        volNr = new VolumeNumber(42);

        minor = new MinorNumber(43);
        volSize = 9001;

        volBlockDevSrc="/dev/src/vol/block";
        volMetaDiskPathSrc = "/dev/src/vol/meta";
        volBlockDevDst ="/dev/dst/vol/block";
        volMetaDiskPathDst = "/dev/dst/vol/meta";
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(
            TBL_VOLUME_CONNECTIONS + " table's column count has changed. Update tests accordingly!",
            5,
            TBL_COL_COUNT_VOLUME_CONNECTIONS
        );

        transMgr = new TransactionMgr(getConnection());

        uuid = randomUUID();

        nodeSrc = nodeDataFactory.getInstance(SYS_CTX, sourceName, null, null, transMgr, true, false);
        nodesMap.put(nodeSrc.getName(), nodeSrc);
        nodeDst = nodeDataFactory.getInstance(SYS_CTX, targetName, null, null, transMgr, true, false);
        nodesMap.put(nodeDst.getName(), nodeDst);

        resDfn = resourceDefinitionDataFactory.getInstance(
            SYS_CTX, resName, resPort, null, "secret", TransportType.IP, transMgr, true, false
        );
        rscDfnMap.put(resDfn.getName(), resDfn);
        volDfn = volumeDefinitionDataFactory.getInstance(SYS_CTX, resDfn, volNr, minor, volSize, null, transMgr, true, false);

        nodeIdSrc = new NodeId(13);
        nodeIdDst = new NodeId(14);

        resSrc = resourceDataFactory.getInstance(SYS_CTX, resDfn, nodeSrc, nodeIdSrc, null, transMgr, true, false);
        resDst = resourceDataFactory.getInstance(SYS_CTX, resDfn, nodeDst, nodeIdDst, null, transMgr, true, false);

        storPoolDfn = storPoolDefinitionDataFactory.getInstance(SYS_CTX, storPoolName, transMgr, true, false);
        storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

        storPool1 = storPoolDataFactory.getInstance(SYS_CTX, nodeSrc, storPoolDfn, LvmDriver.class.getSimpleName(), transMgr, true, false);
        storPool2 = storPoolDataFactory.getInstance(SYS_CTX, nodeDst, storPoolDfn, LvmDriver.class.getSimpleName(), transMgr, true, false);

        volSrc = volumeDataFactory.getInstance(SYS_CTX, resSrc, volDfn, storPool1, volBlockDevSrc, volMetaDiskPathSrc, null, transMgr, true, false);
        volDst = volumeDataFactory.getInstance(SYS_CTX, resDst, volDfn, storPool2, volBlockDevDst, volMetaDiskPathDst, null, transMgr, true, false);
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(uuid, SYS_CTX, volSrc, volDst, transMgr, driver, propsContainerFactory);
        driver.create(volCon, transMgr);

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        volumeConnectionDataFactory.getInstance(SYS_CTX, volSrc, volDst, transMgr, true, false);

        checkDbPersist(false);
    }

    @Test
    public void testLoad() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(uuid, SYS_CTX, volSrc, volDst, transMgr, driver, propsContainerFactory);
        driver.create(volCon, transMgr);

        VolumeConnectionData loadedConDfn = driver.load(volSrc , volDst, true, transMgr);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(uuid, SYS_CTX, volSrc, volDst, transMgr, driver, propsContainerFactory);
        driver.create(volCon, transMgr);

        List<VolumeConnectionData> cons = driver.loadAllByVolume(volSrc, transMgr);

        assertNotNull(cons);

        assertEquals(1, cons.size());

        VolumeConnection loadedConDfn = cons.get(0);
        assertNotNull(loadedConDfn);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(uuid, SYS_CTX, volSrc, volDst, transMgr, driver, propsContainerFactory);
        driver.create(volCon, transMgr);

        VolumeConnectionData loadedConDfn = volumeConnectionDataFactory.getInstance(
            SYS_CTX,
            volSrc,
            volDst,
            transMgr,
            false,
            false
        );

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testCache() throws Exception
    {
        VolumeConnectionData storedInstance = volumeConnectionDataFactory.getInstance(
            SYS_CTX,
            volSrc,
            volDst,
            transMgr,
            true,
            false
        );

        // no clear-cache

        assertEquals(storedInstance, driver.load(volSrc, volDst, true, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        VolumeConnectionData volCon = new VolumeConnectionData(uuid, SYS_CTX, volSrc, volDst, transMgr, driver, propsContainerFactory);
        driver.create(volCon, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VLM_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(volCon, transMgr);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    @Test
    public void testSatelliteCreate() throws Exception
    {
        satelliteMode();
        VolumeConnectionData satelliteConDfn = volumeConnectionDataFactory.getInstance(
            SYS_CTX,
            volSrc,
            volDst,
            null,
            true,
            false
        );

        checkLoadedConDfn(satelliteConDfn, false);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VLM_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        satelliteMode();
        VolumeConnectionData satelliteConDfn = volumeConnectionDataFactory.getInstance(
            SYS_CTX,
            volSrc,
            volDst,
            null,
            false,
            false
        );

        assertNull(satelliteConDfn);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VLM_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VLM_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        if (checkUuid)
        {
            assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
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
        VolumeConnectionData volCon = new VolumeConnectionData(uuid, SYS_CTX, volSrc, volDst, transMgr, driver, propsContainerFactory);
       driver.create(volCon, transMgr);

        volumeConnectionDataFactory.getInstance(SYS_CTX, volSrc, volDst, transMgr, false, true);
    }
}
