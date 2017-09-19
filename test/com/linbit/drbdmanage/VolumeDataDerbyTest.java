package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.core.CoreUtils;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.utils.UuidUtils;

public class VolumeDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_VOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " +
                     VLM_NR + ", " + BLOCK_DEVICE_PATH + ", " + META_DISK_PATH + ", " +
                     VLM_FLAGS +
        " FROM " + TBL_VOLUMES;

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
    private String metaDiskPath;
    private VolumeData vol;

    private VolumeDataDerbyDriver driver;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_VOLUMES + " table's column count has changed. Update tests accordingly!",
            7,
            TBL_COL_COUNT_VOLUMES
        );

        transMgr = new TransactionMgr(getConnection());

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
        metaDiskPath = "/dev/not-null";
        vol = new VolumeData(
            uuid,
            res,
            volDfn,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            null,
            transMgr
        );

        driver = new VolumeDataDerbyDriver(sysCtx, errorReporter);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(vol, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(blockDevicePath, resultSet.getString(BLOCK_DEVICE_PATH));
        assertEquals(metaDiskPath, resultSet.getString(META_DISK_PATH));
        assertEquals(VlmFlags.CLEAN.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        VolumeData volData = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            blockDevicePath,
            metaDiskPath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null, // serial
            transMgr,
            true
        );

        assertNotNull(volData);
        assertNotNull(volData.getUuid());
        assertEquals(blockDevicePath, volData.getBlockDevicePath(sysCtx));
        assertEquals(metaDiskPath, volData.getMetaDiskPath(sysCtx));
        assertNotNull(volData.getFlags());
        assertTrue(volData.getFlags().isSet(sysCtx, VlmFlags.CLEAN));
        assertNotNull(volData.getProps(sysCtx));
        assertEquals(res, volData.getResource());
        assertEquals(resDfn, volData.getResourceDefinition());
        assertEquals(volDfn, volData.getVolumeDefinition());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(blockDevicePath, resultSet.getString(BLOCK_DEVICE_PATH));
        assertEquals(metaDiskPath, resultSet.getString(META_DISK_PATH));
        assertEquals(VlmFlags.CLEAN.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(vol, transMgr);

        VolumeData loadedVol = driver.load(res, volDfn, null, transMgr);

        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(vol, transMgr);

        List<VolumeData> volList = driver.loadAllVolumesByResource(
            res,
            transMgr,
            null,
            sysCtx
        );

        assertEquals(1, volList.size());

        VolumeData loadedVol = volList.get(0);
        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(vol, transMgr);

        VolumeData loadedVol = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            blockDevicePath,
            metaDiskPath,
            null, // flags
            null, // serial
            transMgr,
            false
        );
        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testCache() throws Exception
    {
        VolumeData storedInstance = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            blockDevicePath,
            metaDiskPath,
            null,
            null,
            transMgr,
            true
        );

        // no clearCaches

        assertEquals(storedInstance, driver.load(res, volDfn, null, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(vol, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        resultSet.close();

        driver.delete(vol, transMgr);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPropsConPersist() throws Exception
    {
        driver.create(vol, transMgr);
        vol.initialized();
        vol.setConnection(transMgr);

        String testKey = "TestKey";
        String testValue = "TestValue";
        vol.getProps(sysCtx).setProp(testKey, testValue);

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);

        testProps(transMgr, PropsContainer.buildPath(nodeName, resName, volNr), map, true);
    }

    @Test
    public void testPropsConLoad() throws Exception
    {
        driver.create(vol, transMgr);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(transMgr, PropsContainer.buildPath(nodeName, resName, volNr), testKey, testValue);

        VolumeData loadedVol = driver.load(res, volDfn, null, transMgr);

        assertNotNull(loadedVol);
        Props props = loadedVol.getProps(sysCtx);
        assertNotNull(props);
        assertEquals(testValue, props.getProp(testKey));
    }

    @Test
    public void testFlagsUpdate() throws Exception
    {
        driver.create(vol, transMgr);

        vol.initialized();
        vol.setConnection(transMgr);

        vol.getFlags().disableAllFlags(sysCtx);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
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
        CoreUtils.satelliteMode();

        VolumeData volData = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            blockDevicePath,
            metaDiskPath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null, // serial
            null, // transMgr
            true
        );

        checkLoaded(volData, null);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        CoreUtils.satelliteMode();
        VolumeData volData = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            blockDevicePath,
            metaDiskPath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null, // serial
            null, // transMgr
            false
        );

        assertNull(volData);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
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
        assertEquals(metaDiskPath, loadedVol.getMetaDiskPath(sysCtx));
        assertNotNull(loadedVol.getFlags());
        assertTrue(loadedVol.getFlags().isSet(sysCtx, VlmFlags.CLEAN));
        assertNotNull(loadedVol.getProps(sysCtx));
        assertEquals(res.getDefinition().getName(), loadedVol.getResource().getDefinition().getName());
        assertEquals(volDfn.getMinorNr(sysCtx), loadedVol.getVolumeDefinition().getMinorNr(sysCtx));
        assertEquals(volDfn.getVolumeNumber(sysCtx), loadedVol.getVolumeDefinition().getVolumeNumber(sysCtx));
        assertEquals(volDfn.getVolumeSize(sysCtx), loadedVol.getVolumeDefinition().getVolumeSize(sysCtx));
        assertEquals(volDfn.getUuid(), loadedVol.getVolumeDefinition().getUuid());
    }
}
