package com.linbit.linstor;

import com.linbit.TransactionMgr;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.utils.UuidUtils;
import org.junit.Test;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    private TcpPortNumber resPort;
    private ResourceDefinitionData resDfn;

    private NodeId nodeId;
    private ResourceData res;

    private StorPoolName storPoolName;
    private StorPoolDefinitionData storPoolDfn;
    private StorPoolData storPool;

    private VolumeNumber volNr;
    private Integer minor;
    private long volSize;
    private VolumeDefinition volDfn;

    private java.util.UUID uuid;
    private String blockDevicePath;
    private String metaDiskPath;

    @Inject private VolumeDataDerbyDriver driver;

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
        node = nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName,
            null,
            null,
            transMgr,
            true,
            false
        );

        resName = new ResourceName("TestResName");
        resPort = new TcpPortNumber(9001);
        resDfn = resourceDefinitionDataFactory.getInstance(
            SYS_CTX,
            resName,
            resPort,
            null,
            "secret",
            TransportType.IP,
            transMgr,
            true,
            false
        );

        nodeId = new NodeId(7);
        res = resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            transMgr,
            true,
            false
        );

        storPoolName = new StorPoolName("TestStorPoolName");
        storPoolDfn = storPoolDefinitionDataFactory.getInstance(
            SYS_CTX,
            storPoolName,
            transMgr,
            true,
            false
        );

        storPool = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            storPoolDfn,
            LvmDriver.class.getSimpleName(),
            transMgr,
            true,
            false
        );

        volNr = new VolumeNumber(13);
        minor = 42;
        volSize = 5_000_000;
        volDfn = volumeDefinitionDataFactory.create(
            SYS_CTX,
            resDfn,
            volNr,
            minor,
            volSize,
            null,
            transMgr
        );

        uuid = randomUUID();
        blockDevicePath = "/dev/null";
        metaDiskPath = "/dev/not-null";
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
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
        VolumeData volData = volumeDataFactory.getInstance(
            SYS_CTX,
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
        assertEquals(blockDevicePath, volData.getBlockDevicePath(SYS_CTX));
        assertEquals(metaDiskPath, volData.getMetaDiskPath(SYS_CTX));
        assertNotNull(volData.getFlags());
        assertTrue(volData.getFlags().isSet(SYS_CTX, VlmFlags.CLEAN));
        assertNotNull(volData.getProps(SYS_CTX));
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
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
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
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
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
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
        );
        driver.create(vol, transMgr);

        VolumeData loadedVol = volumeDataFactory.getInstance(
            SYS_CTX,
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
        VolumeData storedInstance = volumeDataFactory.getInstance(
            SYS_CTX,
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
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
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
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
        );
        driver.create(vol, transMgr);
        vol.initialized();
        vol.setConnection(transMgr);

        String testKey = "TestKey";
        String testValue = "TestValue";
        vol.getProps(SYS_CTX).setProp(testKey, testValue);

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);

        testProps(transMgr, PropsContainer.buildPath(nodeName, resName, volNr), map);
    }

    @Test
    public void testPropsConLoad() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
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
        res.removeVolume(SYS_CTX, vol);

        VolumeData loadedVol = driver.load(res, volDfn, true, transMgr);

        /*
         *  NOTE: as the "driver.load(...)" has to create a new instance of VolumeData,
         *  the loaded volume is re-registered now.
         */

        assertNotNull(loadedVol);
        Props props = loadedVol.getProps(SYS_CTX);
        assertNotNull(props);
        assertEquals(testValue, props.getProp(testKey));
    }

    @Test
    public void testFlagsUpdate() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
        );
        driver.create(vol, transMgr);

        vol.initialized();
        vol.setConnection(transMgr);

        vol.getFlags().disableAllFlags(SYS_CTX);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(VLM_FLAGS));
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
        assertEquals(blockDevicePath, loadedVol.getBlockDevicePath(SYS_CTX));
        assertEquals(metaDiskPath, loadedVol.getMetaDiskPath(SYS_CTX));
        assertNotNull(loadedVol.getFlags());
        assertTrue(loadedVol.getFlags().isSet(SYS_CTX, VlmFlags.CLEAN));
        assertNotNull(loadedVol.getProps(SYS_CTX));
        assertEquals(res.getDefinition().getName(), loadedVol.getResource().getDefinition().getName());
        assertEquals(volDfn.getMinorNr(SYS_CTX), loadedVol.getVolumeDefinition().getMinorNr(SYS_CTX));
        assertEquals(volDfn.getVolumeNumber(), loadedVol.getVolumeDefinition().getVolumeNumber());
        assertEquals(volDfn.getVolumeSize(SYS_CTX), loadedVol.getVolumeDefinition().getVolumeSize(SYS_CTX));
        assertEquals(volDfn.getUuid(), loadedVol.getVolumeDefinition().getUuid());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            transMgr,
            driver,
            propsContainerFactory
        );
        driver.create(vol, transMgr);

        volumeDataFactory.getInstance(
            SYS_CTX,
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

    @Test
    /**
     * Checks that after a transaction commit, all transaction objects are cleared
     */
    public void testTransactionObjectsCommit() throws Exception
    {
        assertNotEquals(transMgr.sizeObjects(), 0);
        transMgr.commit();
        assertEquals(0, transMgr.sizeObjects());
        assertFalse(node.hasTransMgr());
        assertFalse(resDfn.hasTransMgr());
        assertFalse(res.hasTransMgr());
        assertFalse(storPoolDfn.hasTransMgr());
        assertFalse(storPool.hasTransMgr());
        assertFalse(volDfn.hasTransMgr());
    }

    @Test
    /**
     * Checks that after a transaction rollback, all transaction objects are cleared
     */
    public void testTransactionObjectsRollback() throws Exception
    {
        assertNotEquals(transMgr.sizeObjects(), 0);
        transMgr.rollback();
        assertEquals(0, transMgr.sizeObjects());
        assertFalse(node.hasTransMgr());
        assertFalse(resDfn.hasTransMgr());
        assertFalse(res.hasTransMgr());
        assertFalse(storPoolDfn.hasTransMgr());
        assertFalse(storPool.hasTransMgr());
        assertFalse(volDfn.hasTransMgr());
    }

    @Test (expected = AssertionError.class)
    public void testObjectCommitFail() throws Exception
    {
        node.commit();
    }

    @Test
    /**
     * Checks that after a transaction rollback, all transaction objects are cleared
     */
    public void testTransactionObjectsRollbackDirty() throws Exception
    {
        assertNotEquals(transMgr.sizeObjects(), 0);
        resDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");
        transMgr.rollback();
        assertEquals(0, transMgr.sizeObjects());
        assertFalse(node.hasTransMgr());
        assertFalse(resDfn.hasTransMgr());
        assertFalse(res.hasTransMgr());
        assertFalse(storPoolDfn.hasTransMgr());
        assertFalse(storPool.hasTransMgr());
        assertFalse(volDfn.hasTransMgr());
    }
}
