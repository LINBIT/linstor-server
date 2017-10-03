package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.utils.UuidUtils;

public class VolumeConnectionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RES_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " + NODE_NAME_DST + ", " +
                     RESOURCE_NAME + ", " + VLM_NR +
        " FROM " + TBL_VOLUME_CONNECTIONS;

    private final NodeName sourceName;
    private final NodeName targetName;
    private final ResourceName resName;
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
    private VolumeData volSrc;
    private VolumeData volDst;

    private VolumeConnectionData volCon;
    private VolumeConnectionDataDerbyDriver driver;

    private NodeId nodeIdSrc;
    private NodeId nodeIdDst;

    public VolumeConnectionDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
    {
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
        resName = new ResourceName("testResourceName");
        volNr = new VolumeNumber(42);

        minor = new MinorNumber(43);
        volSize = 9001;

        volBlockDevSrc="/dev/src/vol/block";
        volMetaDiskPathSrc = "/dev/src/vol/meta";
        volBlockDevDst ="/dev/dst/vol/block";
        volMetaDiskPathDst = "/dev/dst/vol/meta";
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_VOLUME_CONNECTIONS + " table's column count has changed. Update tests accordingly!", 5, TBL_COL_COUNT_VOLUME_CONNECTIONS);

        transMgr = new TransactionMgr(getConnection());

        uuid = randomUUID();

        nodeSrc = NodeData.getInstance(sysCtx, sourceName, null, null, transMgr, true, false);
        nodeDst = NodeData.getInstance(sysCtx, targetName, null, null, transMgr, true, false);

        resDfn = ResourceDefinitionData.getInstance(sysCtx, resName, null, transMgr, true, false);
        volDfn = VolumeDefinitionData.getInstance(sysCtx, resDfn, volNr, minor, volSize, null, transMgr, true, false);

        nodeIdSrc = new NodeId(13);
        nodeIdDst = new NodeId(14);

        resSrc = ResourceData.getInstance(sysCtx, resDfn, nodeSrc, nodeIdSrc, null, transMgr, true, false);
        resDst = ResourceData.getInstance(sysCtx, resDfn, nodeDst, nodeIdDst, null, transMgr, true, false);

        volSrc = VolumeData.getInstance(sysCtx, resSrc, volDfn, volBlockDevSrc, volMetaDiskPathSrc, null, transMgr, true, false);
        volDst = VolumeData.getInstance(sysCtx, resDst, volDfn, volBlockDevDst, volMetaDiskPathDst, null, transMgr, true, false);

        volCon = new VolumeConnectionData(uuid, sysCtx, volSrc, volDst, transMgr);
        driver = (VolumeConnectionDataDerbyDriver) DrbdManage.getVolumeConnectionDatabaseDriver();
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(volCon, transMgr);

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        VolumeConnectionData.getInstance(sysCtx, volSrc, volDst, transMgr, true, false);

        checkDbPersist(false);
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(volCon, transMgr);

        VolumeConnectionData loadedConDfn = driver.load(volSrc , volDst, true, transMgr);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadAll() throws Exception
    {
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
        driver.create(volCon, transMgr);

        VolumeConnectionData loadedConDfn = VolumeConnectionData.getInstance(
            sysCtx,
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
        VolumeConnectionData storedInstance = VolumeConnectionData.getInstance(
            sysCtx,
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
        driver.create(volCon, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
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
        VolumeConnectionData satelliteConDfn = VolumeConnectionData.getInstance(
            sysCtx,
            volSrc,
            volDst,
            null,
            true,
            false
        );

        checkLoadedConDfn(satelliteConDfn, false);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        satelliteMode();
        VolumeConnectionData satelliteConDfn = VolumeConnectionData.getInstance(
            sysCtx,
            volSrc,
            volDst,
            null,
            false,
            false
        );

        assertNull(satelliteConDfn);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
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
        Volume sourceVolume = loadedConDfn.getSourceVolume(sysCtx);
        Volume targetVolume = loadedConDfn.getTargetVolume(sysCtx);

        assertEquals(sourceName, sourceVolume.getResource().getAssignedNode().getName());
        assertEquals(targetName, targetVolume.getResource().getAssignedNode().getName());
        assertEquals(resName, sourceVolume.getResourceDefinition().getName());
        assertEquals(sourceVolume.getResourceDefinition(), targetVolume.getResourceDefinition());
        assertEquals(volNr, sourceVolume.getVolumeDefinition().getVolumeNumber(sysCtx));
        assertEquals(sourceVolume.getVolumeDefinition(), targetVolume.getVolumeDefinition());
    }

    @Test (expected = DrbdDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(volCon, transMgr);

        VolumeConnectionData.getInstance(sysCtx, volSrc, volDst, transMgr, false, true);
    }
}
