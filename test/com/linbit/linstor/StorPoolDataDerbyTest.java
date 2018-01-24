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
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.drbdstate.DrbdStateTracker;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;
import com.linbit.timer.Timer;
import com.linbit.utils.UuidUtils;

public class StorPoolDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + POOL_NAME + ", " + DRIVER_NAME +
        " FROM " + TBL_NODE_STOR_POOL +
        " WHERE " + POOL_NAME + " <> '" + LinStor.DISKLESS_STOR_POOL_NAME.toUpperCase() + "'";

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

        node = NodeData.getInstance(SYS_CTX, nodeName, null, null, transMgr, true, false);
        spdd = StorPoolDefinitionData.getInstance(SYS_CTX, spName, transMgr, true, false);

        driver = (StorPoolDataDerbyDriver) LinStor.getStorPoolDataDatabaseDriver();

        uuid = randomUUID();
    }

    @Test
    public void testPersist() throws Exception
    {
        StorPoolData storPool = new StorPoolData(uuid, SYS_CTX, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
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
            SYS_CTX,
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

        StorPoolData storPool = new StorPoolData(uuid, SYS_CTX, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        loadedStorPool = driver.load(node, spdd, true, transMgr);
        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(SYS_CTX).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(SYS_CTX));
        assertNull(loadedStorPool.getDriver(SYS_CTX));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testLoadAll() throws Exception
    {
        StorPoolData storPool = new StorPoolData(uuid, SYS_CTX, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        List<StorPoolData> storPools = driver.loadStorPools(node, transMgr);

        assertNotNull(storPools);
        assertEquals(2, storPools.size());
        StorPoolData storPoolData = storPools.get(1); // the [0] should be the default diskless stor pool, we just skip that
        assertNotNull(storPoolData);
        assertNotNull(storPoolData.getProps(SYS_CTX));
        StorPoolDefinition spDfn = storPoolData.getDefinition(SYS_CTX);
        assertNotNull(spDfn);
        assertEquals(spName, spDfn.getName());
        assertNull(storPoolData.getDriver(SYS_CTX));
        assertEquals(LvmDriver.class.getSimpleName(), storPoolData.getDriverName());
        assertEquals(spName, storPoolData.getName());
    }

    @Test
    public void testCache() throws Exception
    {
        StorPoolData storedInstance = StorPoolData.getInstance(
            SYS_CTX,
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
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            false
        );

        assertNull(loadedStorPool);

        StorPoolData storPool = new StorPoolData(uuid, SYS_CTX, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);
        loadedStorPool = StorPoolData.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            false
        );

        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(SYS_CTX).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(SYS_CTX));
        assertNull(loadedStorPool.getDriver(SYS_CTX));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testDelete() throws Exception
    {
        StorPoolData storPool = StorPoolData.getInstance(
            SYS_CTX,
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
        StorPoolData storPool = new StorPoolData(uuid, SYS_CTX, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);

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
        StorPoolDefinitionData spdd2 = StorPoolDefinitionData.getInstance(SYS_CTX, spName2, transMgr, true, false);

        StorPoolData storPoolData = StorPoolData.getInstanceSatellite(
            SYS_CTX,
            uuid,
            node,
            spdd2,
            LvmDriver.class.getSimpleName(),
            null,
            new SatelliteCoreServices()
            {

                @Override
                public Timer<String, Action<String>> getTimer()
                {
                    return new GenericTimer<>();
                }

                @Override
                public ErrorReporter getErrorReporter()
                {
                    return null;
                }

                @Override
                public FileSystemWatch getFsWatch()
                {
                    return null;
                }

                @Override
                public DrbdStateTracker getDrbdStateTracker()
                {
                    return null;
                }

                @Override
                public DeviceManager getDeviceManager()
                {
                    return null;
                }
            }
        );

        assertNotNull(storPoolData);

        assertNotNull(storPoolData.getProps(SYS_CTX));
        assertEquals(spdd2, storPoolData.getDefinition(SYS_CTX));
        assertNotNull(storPoolData.getDriver(SYS_CTX));
        assertTrue(storPoolData.getDriver(SYS_CTX) instanceof LvmDriver);
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
        StorPoolDefinitionData spdd2 = StorPoolDefinitionData.getInstance(SYS_CTX, spName2, transMgr, true, false);

        StorPoolData storPoolData = StorPoolData.getInstance(
            SYS_CTX,
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
        StorPoolData storPool = new StorPoolData(uuid, SYS_CTX, node, spdd, null, LvmDriver.class.getSimpleName(), transMgr);
        driver.create(storPool, transMgr);

        StorPoolData.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            transMgr,
            false,
            true
        );
    }
}
