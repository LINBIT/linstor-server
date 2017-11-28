package com.linbit.linstor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDataDerbyDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.utils.UuidUtils;

public class StorPoolDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + POOL_NAME + ", " + DRIVER_NAME +
        " FROM " + TBL_NODE_STOR_POOL;

    private final NodeName nodeName;
    private final StorPoolName spName;

    private TransactionMgr transMgr;
    private NodeData node;

    private java.util.UUID uuid;

    private StorPoolDefinitionData spdd;
    private StorPoolDataDerbyDriver driver;

    public StorPoolDataDerbyTest() throws InvalidNameException
    {
        nodeName = new NodeName("TestNodeName");
        spName = new StorPoolName("TestStorPoolDefinition");
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_NODE_STOR_POOL + " table's column count has changed. Update tests accordingly!", 4, TBL_COL_COUNT_NODE_STOR_POOL);

        transMgr = new TransactionMgr(getConnection());

        node = NodeData.getInstance(sysCtx, nodeName, null, null, transMgr, true, false);
        spdd = StorPoolDefinitionData.getInstance(sysCtx, spName, transMgr, true, false);

        driver = (StorPoolDataDerbyDriver) LinStor.getStorPoolDataDatabaseDriver();

        uuid = randomUUID();
    }

    @Test
    public void testPersist() throws Exception
    {
        StorPoolData storPool = new StorPoolData(uuid, sysCtx, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(spName.value, resultSet.getString(POOL_NAME));
        assertEquals(LvmDriver.class.getSimpleName(), resultSet.getString(DRIVER_NAME));
        assertFalse("Database persisted too many storPools", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistStorGetInstance() throws Exception
    {
        StorPool pool = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            true, // create
            false
        );
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(pool.getUuid(), UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(pool.getName().value, resultSet.getString(POOL_NAME));
        assertEquals(LvmDriver.class.getSimpleName(), resultSet.getString(DRIVER_NAME));
        assertFalse("Database persisted too many storPools", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        StorPoolData loadedStorPool = driver.load(node, spdd, false, transMgr);
        assertNull(loadedStorPool);

        StorPoolData storPool = new StorPoolData(uuid, sysCtx, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        loadedStorPool = driver.load(node, spdd, true, transMgr);
        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(sysCtx).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(sysCtx));
        assertNull(loadedStorPool.getDriver(sysCtx));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testLoadAll() throws Exception
    {
        StorPoolData storPool = new StorPoolData(uuid, sysCtx, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        List<StorPoolData> storPools = driver.loadStorPools(node, transMgr);

        assertNotNull(storPools);
        assertEquals(1, storPools.size());
        StorPoolData storPoolData = storPools.get(0);
        assertNotNull(storPoolData);
        assertNotNull(storPoolData.getConfiguration(sysCtx));
        StorPoolDefinition spDfn = storPoolData.getDefinition(sysCtx);
        assertNotNull(spDfn);
        assertEquals(spName, spDfn.getName());
        assertNull(storPoolData.getDriver(sysCtx));
        assertEquals(LvmDriver.class.getSimpleName(), storPoolData.getDriverName());
        assertEquals(spName, storPoolData.getName());
    }

    @Test
    public void testCache() throws Exception
    {
        StorPoolData storedInstance = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            true,
            false
        );

        // no clearCaches

        assertEquals(storedInstance, driver.load(node, spdd, true, transMgr));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        StorPoolData loadedStorPool = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            false
        );

        assertNull(loadedStorPool);

        StorPoolData storPool = new StorPoolData(uuid, sysCtx, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);
        loadedStorPool = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            false
        );

        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(sysCtx).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(sysCtx));
        assertNull(loadedStorPool.getDriver(sysCtx));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testDelete() throws Exception
    {
        StorPoolData storPool = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            true, // create
            false
        );
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());

        resultSet.close();

        driver.delete(storPool, transMgr);

        resultSet = stmt.executeQuery();

        assertFalse("Database did not delete storPool", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExist() throws Exception
    {
        StorPoolData storPool = new StorPoolData(uuid, sysCtx, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();

        driver.ensureEntryExists(storPool, transMgr);

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        driver.ensureEntryExists(storPool, transMgr);

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        satelliteMode();

        StorPoolName spName2 = new StorPoolName("OtherStorPool");
        StorPoolDefinitionData spdd2 = StorPoolDefinitionData.getInstance(sysCtx, spName2, transMgr, true, false);

        StorPoolData storPoolData = StorPoolData.getInstanceSatellite(
            sysCtx,
            uuid,
            node,
            spdd2,
            LvmDriver.class.getSimpleName(),
            null
        );

        assertNotNull(storPoolData);

        assertNotNull(storPoolData.getConfiguration(sysCtx));
        assertEquals(spdd2, storPoolData.getDefinition(sysCtx));
        assertNotNull(storPoolData.getDriver(sysCtx));
        assertTrue(storPoolData.getDriver(sysCtx) instanceof LvmDriver);
        assertEquals(LvmDriver.class.getSimpleName(), storPoolData.getDriverName());
        assertEquals(spName2, storPoolData.getName());
        assertNotNull(storPoolData.getUuid());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        satelliteMode();

        StorPoolName spName2 = new StorPoolName("OtherStorPool");
        StorPoolDefinitionData spdd2 = StorPoolDefinitionData.getInstance(sysCtx, spName2, transMgr, true, false);

        StorPoolData storPoolData = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd2,
            LvmDriver.class.getSimpleName(),
            null,
            false,
            false
        );

        assertNull(storPoolData);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        StorPoolData storPool = new StorPoolData(uuid, sysCtx, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            true
        );
    }
}
