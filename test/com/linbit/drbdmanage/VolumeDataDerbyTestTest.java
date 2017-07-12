package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.utils.UuidUtils;

public class VolumeDataDerbyTestTest extends DerbyBase
{
    private static final String SELECT_ALL_VOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " +
                     VLM_NR + ", " + BLOCK_DEVICE_PATH + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUMES;

    private Connection con;
    private TransactionMgr transMgr;

    private NodeName nodeName;
    private NodeData node;

    private ResourceName resName;
    private ResourceDefinitionData resDfn;

    private NodeId nodeId;
    private ResourceData res;

    private VolumeNumber volNr;
    private MinorNumber minor;
    private long volSize;
    private VolumeDefinition volDfn;

    private java.util.UUID uuid;
    private String blockDevicePath;
    private VolumeData vol;

    private VolumeDataDerbyDriver driver;


    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_VOLUMES + " table's column count has changed. Update tests accordingly!",
            6,
            TBL_COL_COUNT_VOLUMES
        );

        con = getConnection();
        transMgr = new TransactionMgr(con);

        nodeName = new NodeName("TestNodeName");
        node = NodeData.getInstance(
            sysCtx,
            nodeName,
            null,
            null,
            null,
            transMgr,
            true
        );

        resName = new ResourceName("TestResName");
        resDfn = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            null,
            null,
            transMgr,
            true
        );

        nodeId = new NodeId(7);
        res = ResourceData.getInstance(
            sysCtx,
            resDfn,
            node,
            nodeId,
            null,
            null,
            transMgr,
            true
        );

        volNr = new VolumeNumber(13);
        minor = new MinorNumber(42);
        volSize = 5_000_000;
        volDfn = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            null,
            null,
            transMgr,
            true
            );

        uuid = randomUUID();
        blockDevicePath = "/dev/null";
        vol = new VolumeData(
            uuid,
            res,
            volDfn,
            blockDevicePath,
            VlmFlags.CLEAN.flagValue,
            null,
            transMgr
        );

        driver = new VolumeDataDerbyDriver(sysCtx, res, volDfn);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(con, vol);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(uuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(blockDevicePath, resultSet.getString(BLOCK_DEVICE_PATH));
        assertEquals(VlmFlags.CLEAN.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        VolumeData volData = VolumeData.getInstance(
            res,
            volDfn,
            blockDevicePath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null,
            transMgr,
            true
        );

        assertNotNull(volData);
        assertNotNull(volData.getUuid());
        assertEquals(blockDevicePath, volData.getBlockDevicePath(sysCtx));
        assertNotNull(volData.getFlags());
        assertTrue(volData.getFlags().isSet(sysCtx, VlmFlags.CLEAN));
        assertNotNull(volData.getProps(sysCtx));
        assertEquals(res, volData.getResource());
        assertEquals(resDfn, volData.getResourceDfn());
        assertEquals(volDfn, volData.getVolumeDfn());

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(blockDevicePath, resultSet.getString(BLOCK_DEVICE_PATH));
        assertEquals(VlmFlags.CLEAN.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(con, vol);
        DriverUtils.clearCaches();

        VolumeData loadedVol = driver.load(con, transMgr, null);

        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testLoadStatic() throws Exception
    {
        driver.create(con, vol);
        DriverUtils.clearCaches();

        List<VolumeData> volList = VolumeDataDerbyDriver.loadAllVolumesByResource(con, res, transMgr, null, sysCtx);

        assertEquals(1, volList.size());

        VolumeData loadedVol = volList.get(0);
        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(con, vol);
        DriverUtils.clearCaches();

        VolumeData loadedVol = VolumeData.getInstance(res, volDfn, blockDevicePath, null, null, transMgr, false);
        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(con, vol);
        DriverUtils.clearCaches();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        resultSet.close();

        driver.delete(con);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPropsConPersist() throws Exception
    {
        driver.create(con, vol);
        vol.initialized();
        vol.setConnection(transMgr);

        String testKey = "TestKey";
        String testValue = "TestValue";
        vol.getProps(sysCtx).setProp(testKey, testValue);

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);

        testProps(con, driver.getPropsConDriver().getInstanceName(), map, true);
    }

    @Test
    public void testPropsConLoad() throws Exception
    {
        driver.create(con, vol);
        DriverUtils.clearCaches();
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(con, driver.getPropsConDriver().getInstanceName(), testKey, testValue);

        VolumeData loadedVol = driver.load(con, transMgr, null);

        assertNotNull(loadedVol);
        Props props = loadedVol.getProps(sysCtx);
        assertNotNull(props);
        assertEquals(testValue, props.getProp(testKey));
    }

    @Test
    public void testFlagsUpdate() throws Exception
    {
        driver.create(con, vol);

        vol.initialized();
        vol.setConnection(transMgr);

        vol.getFlags().disableAllFlags(sysCtx);

        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(VLM_FLAGS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();

    }

    @Test
    public void testSatelliteCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        VolumeData volData = VolumeData.getInstance(
            res,
            volDfn,
            blockDevicePath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null,
            null,
            true
        );

        checkLoaded(volData, null);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        DriverUtils.satelliteMode();
        VolumeData volData = VolumeData.getInstance(
            res,
            volDfn,
            blockDevicePath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null,
            null,
            false
        );

        assertNull(volData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }


    private void checkLoaded(VolumeData loadedVol, java.util.UUID expectedUuid) throws AccessDeniedException
    {
        assertNotNull(loadedVol);
        if (expectedUuid == null)
        {
            assertNotNull(loadedVol.getUuid());
        }
        else
        {
            assertEquals(uuid, loadedVol.getUuid());
        }
        assertEquals(blockDevicePath, loadedVol.getBlockDevicePath(sysCtx));
        assertNotNull(loadedVol.getFlags());
        assertTrue(loadedVol.getFlags().isSet(sysCtx, VlmFlags.CLEAN));
        assertNotNull(loadedVol.getProps(sysCtx));
        assertEquals(res.getDefinition().getName(), loadedVol.getResource().getDefinition().getName());
        assertEquals(volDfn.getMinorNr(sysCtx), loadedVol.getVolumeDfn().getMinorNr(sysCtx));
        assertEquals(volDfn.getVolumeNumber(sysCtx), loadedVol.getVolumeDfn().getVolumeNumber(sysCtx));
        assertEquals(volDfn.getVolumeSize(sysCtx), loadedVol.getVolumeDfn().getVolumeSize(sysCtx));
        assertEquals(volDfn.getUuid(), loadedVol.getVolumeDfn().getUuid());
    }
}
