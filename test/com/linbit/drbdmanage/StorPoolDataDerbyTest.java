package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Before;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
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

        driver = DrbdManage.getStorPoolDataDatabaseDriver(node, spdd);

        uuid = randomUUID();
        objProt = ObjectProtection.getInstance(sysCtx, transMgr, ObjectProtection.buildPathSP(spName), true);
        storPool = new StorPoolData(uuid, objProt, spdd, transMgr, null, LvmDriver.class.getSimpleName(), null, node);
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
            spdd,
            transMgr,
            null, // storageDriver
            LvmDriver.class.getSimpleName(),
            null, // serialGen
            node,
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

        loadedStorPool = driver.load(con, transMgr, null);
        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(sysCtx).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(sysCtx));
        assertNull(loadedStorPool.getDriver(sysCtx));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        StorPoolData loadedStorPool = StorPoolData.getInstance(
            sysCtx,
            spdd,
            transMgr,
            null,
            LvmDriver.class.getSimpleName(),
            null,
            node,
            false
        );

        assertNull(loadedStorPool);

        driver.create(con, storPool);
        loadedStorPool = StorPoolData.getInstance(
            sysCtx,
            spdd,
            transMgr,
            null,
            LvmDriver.class.getSimpleName(),
            null,
            node,
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
            spdd,
            transMgr,
            null, // storageDriver
            LvmDriver.class.getSimpleName(),
            null, // serialGen
            node,
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
}
