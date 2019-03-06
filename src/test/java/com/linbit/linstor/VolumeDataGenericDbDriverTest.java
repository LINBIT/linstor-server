package com.linbit.linstor;

import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.Volume.InitMaps;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.utils.Pair;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
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

    private Integer nodeId;
    private ResourceData res;

    private StorPoolName storPoolName;
    private StorPoolDefinitionData storPoolDfn;
    private StorPoolData storPool;

    private VolumeNumber volNr;
    private Integer minor;
    private long volSize;
    private VolumeDefinitionData volDfn;

    private java.util.UUID uuid;

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
        node = nodeDataFactory.create(
            SYS_CTX,
            nodeName,
            NodeType.SATELLITE,
            null
        );

        resName = new ResourceName("TestResName");
        resPort = 9001;
        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName,
            resPort,
            null,
            "secret",
            TransportType.IP,
            new ArrayList<>()
        );

        nodeId = 7;
        res = resourceDataFactory.create(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            Collections.emptyList()
        );

        storPoolName = new StorPoolName("TestStorPoolName");
        storPoolDfn = storPoolDefinitionDataFactory.create(
            SYS_CTX,
            storPoolName
        );

        storPool = storPoolDataFactory.create(
            SYS_CTX,
            node,
            storPoolDfn,
            "LvmDriver",
            getFreeSpaceMgr(storPoolDfn, node)
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
    }

    @Test
    public void testPersist() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            VlmFlags.DELETE.flagValue,
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
        assertEquals(VlmFlags.DELETE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        VolumeData volData = volumeDataFactory.create(
            SYS_CTX,
            res,
            volDfn,
            storPool,
            new VlmFlags[] {VlmFlags.DELETE}
        );
        commit();

        assertNotNull(volData);
        assertNotNull(volData.getUuid());
        assertNotNull(volData.getFlags());
        assertTrue(volData.getFlags().isSet(SYS_CTX, VlmFlags.DELETE));
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
        assertEquals(VlmFlags.DELETE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadAll() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            VlmFlags.DELETE.flagValue,
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
            VlmFlags.DELETE.flagValue,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);
        volDfn.putVolume(SYS_CTX, vol);
        res.putVolume(SYS_CTX, vol);

        VolumeData loadedVol = (VolumeData) res.getVolume(volDfn.getVolumeNumber());
        checkLoaded(loadedVol, uuid);
    }

    @Test
    public void testCache() throws Exception
    {
        VolumeData storedInstance = volumeDataFactory.create(
            SYS_CTX,
            res,
            volDfn,
            storPool,
            null
        );

        // no clearCaches

        assertEquals(storedInstance, res.getVolume(volDfn.getVolumeNumber()));
    }

    @Test
    public void testDelete() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            0L,
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
            0L,
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
    public void testFlagsUpdate() throws Exception
    {
        VolumeData vol = new VolumeData(
            uuid,
            res,
            volDfn,
            storPool,
            0L,
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
        assertNotNull(loadedVol.getFlags());
        assertTrue(loadedVol.getFlags().isSet(SYS_CTX, VlmFlags.DELETE));
        assertNotNull(loadedVol.getProps(SYS_CTX));
        assertEquals(res.getDefinition().getName(), loadedVol.getResource().getDefinition().getName());
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
            0L,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(vol);
        volDfn.putVolume(SYS_CTX, vol);
        res.putVolume(SYS_CTX, vol);

        volumeDataFactory.create(
            SYS_CTX,
            res,
            volDfn,
            storPool,
            null
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
