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
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.storage.LvmDriver;
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

    private StorPoolName storPoolName;
    private StorPoolDefinitionData storPoolDfn;
    private StorPoolData storPool;

    private VolumeNumber volNr;
    private MinorNumber minor;
    private long volSize;
    private VolumeDefinition volDfn;

    private java.util.UUID uuid;
    private String blockDevicePath;
    private String metaDiskPath;

    private VolumeDataDerbyDriver driver;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_VOLUMES + " table's column count has changed. Update tests accordingly!",
            8,
            TBL_COL_COUNT_VOLUMES
        );

        transMgr = new TransactionMgr(getConnection());

        nodeName = new NodeName("TestNodeName");
        node = NodeData.getInstance(
            sysCtx,
            nodeName,
            null,
            null,
            transMgr,
            true,
            false
        );

        resName = new ResourceName("TestResName");
        resDfn = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            null,
            transMgr,
            true,
            false
        );

        nodeId = new NodeId(7);
        res = ResourceData.getInstance(
            sysCtx,
            resDfn,
            node,
            nodeId,
            null,
            transMgr,
            true,
            false
        );

        storPoolName = new StorPoolName("TestStorPoolName");
        storPoolDfn = StorPoolDefinitionData.getInstance(
            sysCtx,
            storPoolName,
            transMgr,
            true,
            false
        );

        storPool = StorPoolData.getInstance(
            sysCtx,
            node,
            storPoolDfn,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            true,
            false
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
            transMgr,
            true,
            false
        );

        uuid = randomUUID();
        blockDevicePath = "/dev/null";
        metaDiskPath = "/dev/not-null";

        driver = (VolumeDataDerbyDriver) DrbdManage.getVolumeDataDatabaseDriver();
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
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
            storPool,
            blockDevicePath,
            metaDiskPath,
            new VlmFlags[] { VlmFlags.CLEAN },
            transMgr,
            true,
            false
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
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
        driver.create(vol, transMgr);

        VolumeData loadedVol = driver.load(res, volDfn, true, transMgr);

        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
        driver.create(vol, transMgr);

        List<VolumeData> volList = driver.loadAllVolumesByResource(
            res,
            transMgr
        );

        assertEquals(1, volList.size());

        VolumeData loadedVol = volList.get(0);
        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
        driver.create(vol, transMgr);

        VolumeData loadedVol = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            null, // flags
            transMgr,
            false,
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
            storPool,
            blockDevicePath,
            metaDiskPath,
            null,
            transMgr,
            true,
            false
        );

        // no clearCaches

        assertEquals(storedInstance, driver.load(res, volDfn, true, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
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
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
        driver.create(vol, transMgr);
        vol.initialized();
        vol.setConnection(transMgr);

        String testKey = "TestKey";
        String testValue = "TestValue";
        vol.getProps(sysCtx).setProp(testKey, testValue);

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);

        testProps(transMgr, PropsContainer.buildPath(nodeName, resName, volNr), map);
    }

    @Test
    public void testPropsConLoad() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
        driver.create(vol, transMgr);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(transMgr, PropsContainer.buildPath(nodeName, resName, volNr), testKey, testValue);

        /*
         * This test is a bit hacky.
         * In "new VolumeData(...)" the volume registered itself in volDfn, res and storPool.
         * After that, the driver is told to insert the entry into the database, and the database is also extended with
         * props entry for that volume.
         *
         * The problems start with the load. "driver.load(...)" will try to use the resource as a cache, asking it if
         * it knows about the volume which should be loaded. As "new VolumeData(...)" registered itself, the resource
         * does know about the volume, so the database uses that cached instance and returns.
         * As it assumes the cached instance is valid, it does NOT load the manually inserted props.
         *
         * To avoid this problem, we have to "unregister" the volume.
         */
        res.removeVolume(sysCtx, vol);

        VolumeData loadedVol = driver.load(res, volDfn, true, transMgr);

        /*
         *  NOTE: as the "driver.load(...)" has to create a new instance of VolumeData,
         *  the loaded volume is re-registered now.
         */

        assertNotNull(loadedVol);
        Props props = loadedVol.getProps(sysCtx);
        assertNotNull(props);
        assertEquals(testValue, props.getProp(testKey));
    }

    @Test
    public void testFlagsUpdate() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
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
        satelliteMode();

        VolumeData volData = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null, // transMgr
            true,
            false
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
        satelliteMode();
        VolumeData volData = VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            new VlmFlags[] { VlmFlags.CLEAN },
            null, // transMgr
            false,
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

    @Test (expected = DrbdDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            sysCtx,
            transMgr
        );
        driver.create(vol, transMgr);

        VolumeData.getInstance(
            sysCtx,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            null,
            transMgr,
            false,
            true
        );
    }
}
