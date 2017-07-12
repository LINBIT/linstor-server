package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.storage.LvmDriver;
import com.linbit.utils.UuidUtils;

public class StorPoolDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + POOL_NAME + ", " + DRIVER_NAME +
        " FROM " + TBL_NODE_STOR_POOL;

    private final NodeName nodeName;
    private final StorPoolName spName;

    private Connection con;
    private TransactionMgr transMgr;
    private NodeData node;

    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private StorPoolData storPool;

    private StorPoolDefinitionData spdd;
    private StorPoolDataDatabaseDriver driver;

    public StorPoolDataDerbyTest() throws InvalidNameException
    {
        nodeName = new NodeName("TestNodeName");
        spName = new StorPoolName("TestStorPoolDefinition");
    }

    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_NODE_STOR_POOL + " table's column count has changed. Update tests accordingly!", 4, TBL_COL_COUNT_NODE_STOR_POOL);

        con = getConnection();
        transMgr = new TransactionMgr(con);

        node = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, true);
        spdd = StorPoolDefinitionData.getInstance(sysCtx, spName, transMgr, true);

        driver = new StorPoolDataDerbyDriver(node, spdd);

        uuid = randomUUID();
        objProt = ObjectProtection.getInstance(sysCtx, transMgr, ObjectProtection.buildPathSP(spName), true);
        storPool = new StorPoolData(uuid, objProt, node, spdd, null, LvmDriver.class.getSimpleName(), null, transMgr);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(con, storPool);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(uuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
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
            null, // serialGen
            transMgr,
            true // create
        );
        con.commit();
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(pool.getUuid(), UuidUtils.asUUID(resultSet.getBytes(UUID)));
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
        StorPoolData loadedStorPool = driver.load(con, transMgr, null);
        assertNull(loadedStorPool);

        driver.create(con, storPool);
        DriverUtils.clearCaches();

        loadedStorPool = driver.load(con, transMgr, null);
        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(sysCtx).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(sysCtx));
        assertNull(loadedStorPool.getDriver(sysCtx));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testLoadStatic() throws Exception
    {
        driver.create(con, storPool);
        DriverUtils.clearCaches();

        List<StorPoolData> storPools = StorPoolDataDerbyDriver.loadStorPools(con, node, transMgr, null);

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
    public void testLoadProps() throws Exception
    {
        driver.create(con, storPool);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(con, PropsContainer.buildPath(spName, nodeName), testKey, testValue);

    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        StorPoolData loadedStorPool = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            null,
            transMgr,
            false
        );

        assertNull(loadedStorPool);

        driver.create(con, storPool);
        DriverUtils.clearCaches();
        loadedStorPool = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            null,
            transMgr,
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
        StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            null, // serialGen
            transMgr,
            true // create
        );
        con.commit();
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());

        resultSet.close();

        driver.delete(con);

        resultSet = stmt.executeQuery();

        assertFalse("Database did not delete storPool", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExist() throws Exception
    {
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();

        driver.ensureEntryExists(con, storPool);

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        driver.ensureEntryExists(con, storPool);

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        StorPoolData storPoolData = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            null,
            null,
            true
        );

        assertNotNull(storPoolData);

        assertNotNull(storPoolData.getConfiguration(sysCtx));
        assertEquals(spdd, storPoolData.getDefinition(sysCtx));
        assertNotNull(storPoolData.getDriver(sysCtx));
        assertTrue(storPoolData.getDriver(sysCtx) instanceof LvmDriver);
        assertEquals(LvmDriver.class.getSimpleName(), storPoolData.getDriverName());
        assertEquals(spName, storPoolData.getName());
        assertNotNull(storPoolData.getObjProt());
        assertNotNull(storPoolData.getUuid());

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        StorPoolData storPoolData = StorPoolData.getInstance(
            sysCtx,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            null,
            null,
            false
        );

        assertNull(storPoolData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

}
