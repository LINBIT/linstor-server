package com.linbit.linstor;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.Volume.InitMaps;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.utils.Pair;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VolumeDataGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_VOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " +
                     VLM_NR + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUMES;

    private NodeName nodeName;
    private NodeData node;

    private ResourceName resName;
    private Integer resPort;
    private ResourceDefinitionData resDfn;

    private NodeId nodeId;
    private ResourceData res;

    private StorPoolName storPoolName;
    private StorPoolDefinitionData storPoolDfn;
    private StorPoolData storPool;

    private VolumeNumber volNr;
    private Integer minor;
    private long volSize;
    private VolumeDefinitionData volDfn;

    private java.util.UUID uuid;
    private String blockDevicePath;
    private String metaDiskPath;

    @Inject private VolumeDataGenericDbDriver driver;

    @Before
    @SuppressWarnings("checkstyle:magicnumber")
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(TBL_VOLUMES + " table's column count has changed. Update tests accordingly!",
            8,
            TBL_COL_COUNT_VOLUMES
        );

        nodeName = new NodeName("TestNodeName");
        node = nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName,
            null,
            null,
            true,
            false
        );

        resName = new ResourceName("TestResName");
        resPort = 9001;
        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName,
            resPort,
            null,
            "secret",
            TransportType.IP
        );

        nodeId = new NodeId(7);
        res = resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            true,
            false
        );

        storPoolName = new StorPoolName("TestStorPoolName");
        storPoolDfn = storPoolDefinitionDataFactory.getInstance(
            SYS_CTX,
            storPoolName,
            true,
            false
        );

        storPool = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            storPoolDfn,
            LvmDriver.class.getSimpleName(),
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
            null
        );

        uuid = randomUUID();
        blockDevicePath = null;
        metaDiskPath = null;
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            null,
            null,
            VlmFlags.CLEAN.flagValue,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(blockDevicePath, null);
        assertEquals(metaDiskPath, null);
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
            new VlmFlags[] {VlmFlags.CLEAN},
            true,
            false
        );
        commit();

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

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(blockDevicePath, null);
        assertEquals(metaDiskPath, null);
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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);

        VolumeData loadedVol = driver.load(res, volDfn, storPool, true);

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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);

        Map<Pair<NodeName, ResourceName>, ResourceData> rscMap = new HashMap<>();
        Map<Pair<ResourceName, VolumeNumber>, VolumeDefinitionData> vlmDfnMap = new HashMap<>();
        Map<Pair<NodeName, StorPoolName>, StorPoolData> storPoolMap = new HashMap<>();

        rscMap.put(new Pair<>(nodeName, resName), res);
        vlmDfnMap.put(new Pair<>(resName, volNr), volDfn);
        storPoolMap.put(new Pair<>(nodeName, storPoolName), storPool);

        Map<VolumeData, InitMaps> vlmMap = driver.loadAll(rscMap, vlmDfnMap, storPoolMap);

        assertEquals(1, vlmMap.size());

        VolumeData loadedVol = vlmMap.keySet().iterator().next();
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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);

        VolumeData loadedVol = volumeDataFactory.getInstance(
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            null, // flags
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
            true,
            false
        );

        // no clearCaches

        assertEquals(storedInstance, driver.load(res, volDfn, storPool, true));
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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOLS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        resultSet.close();

        driver.delete(vol);
        commit();

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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);
        commit();

        String testKey = "TestKey";
        String testValue = "TestValue";
        vol.getProps(SYS_CTX).setProp(testKey, testValue);

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);

        testProps(PropsContainer.buildPath(nodeName, resName, volNr), map);
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
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);
        commit();
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(PropsContainer.buildPath(nodeName, resName, volNr), testKey, testValue);

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

        VolumeData loadedVol = driver.load(res, volDfn, storPool, true);

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
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);

        vol.getFlags().disableAllFlags(SYS_CTX);

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOLS);
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
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            VlmFlags.CLEAN.flagValue,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);

        volumeDataFactory.getInstance(
            SYS_CTX,
            res,
            volDfn,
            storPool,
            blockDevicePath,
            metaDiskPath,
            null,
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
        assertNotEquals(transMgrProvider.get().sizeObjects(), 0);
        commit();
        assertEquals(0, transMgrProvider.get().sizeObjects());
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
        assertNotEquals(transMgrProvider.get().sizeObjects(), 0);
        transMgrProvider.get().rollback();
        assertEquals(0, transMgrProvider.get().sizeObjects());
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
        assertNotEquals(transMgrProvider.get().sizeObjects(), 0);
        resDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");
        transMgrProvider.get().rollback();
        assertEquals(0, transMgrProvider.get().sizeObjects());
        assertFalse(node.hasTransMgr());
        assertFalse(resDfn.hasTransMgr());
        assertFalse(res.hasTransMgr());
        assertFalse(storPoolDfn.hasTransMgr());
        assertFalse(storPool.hasTransMgr());
        assertFalse(volDfn.hasTransMgr());
    }
}
