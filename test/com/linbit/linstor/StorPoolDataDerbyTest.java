package com.linbit.linstor;

import javax.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.storage.LvmDriver;
import com.linbit.utils.UuidUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StorPoolDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + POOL_NAME + ", " + DRIVER_NAME +
        " FROM " + TBL_NODE_STOR_POOL +
        " WHERE " + POOL_NAME + " <> '" + LinStor.DISKLESS_STOR_POOL_NAME.toUpperCase() + "'";

    private final NodeName nodeName;
    private final StorPoolName spName;

    private NodeData node;

    private java.util.UUID uuid;

    private StorPoolDefinitionData spdd;
    @Inject private StorPoolDataDerbyDriver driver;

    public StorPoolDataDerbyTest() throws InvalidNameException
    {
        nodeName = new NodeName("TestNodeName");
        spName = new StorPoolName("TestStorPoolDefinition");
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_NODE_STOR_POOL + " table's column count has changed. Update tests accordingly!",
            4,
            TBL_COL_COUNT_NODE_STOR_POOL
        );

        node = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, true, false);
        spdd = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, true, false);

        uuid = randomUUID();
    }

    @Test
    public void testPersist() throws Exception
    {
        StorPoolData storPool = new StorPoolData(
            uuid,
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(storPool);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
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
        StorPool pool = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            true, // create
            false
        );
        commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(pool.getUuid(), java.util.UUID.fromString(resultSet.getString(UUID)));
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
        StorPoolData loadedStorPool = driver.load(node, spdd, false);
        assertNull(loadedStorPool);

        StorPoolData storPool = new StorPoolData(
            uuid,
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(storPool);

        loadedStorPool = driver.load(node, spdd, true);
        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(SYS_CTX).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(SYS_CTX));
        assertNull(loadedStorPool.getDriver(SYS_CTX, null, null, null));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testLoadAll() throws Exception
    {
        StorPoolData storPool = new StorPoolData(
            uuid,
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(storPool);

        List<StorPoolData> storPools = driver.loadStorPools(node);

        assertNotNull(storPools);
        assertEquals(2, storPools.size());

        // the [0] should be the default diskless stor pool, we just skip that
        StorPoolData storPoolData = storPools.get(1);

        assertNotNull(storPoolData);
        assertNotNull(storPoolData.getProps(SYS_CTX));
        StorPoolDefinition spDfn = storPoolData.getDefinition(SYS_CTX);
        assertNotNull(spDfn);
        assertEquals(spName, spDfn.getName());
        assertNull(storPoolData.getDriver(SYS_CTX, null, null, null));
        assertEquals(LvmDriver.class.getSimpleName(), storPoolData.getDriverName());
        assertEquals(spName, storPoolData.getName());
    }

    @Test
    public void testCache() throws Exception
    {
        StorPoolData storedInstance = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            true,
            false
        );

        // no clearCaches

        assertEquals(storedInstance, driver.load(node, spdd, true));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        StorPoolData loadedStorPool = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            false
        );

        assertNull(loadedStorPool);

        StorPoolData storPool = new StorPoolData(
            uuid,
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(storPool);
        loadedStorPool = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            false
        );

        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(SYS_CTX).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(SYS_CTX));
        assertNull(loadedStorPool.getDriver(SYS_CTX, null, null, null));
        assertEquals(LvmDriver.class.getSimpleName(), loadedStorPool.getDriverName());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testDelete() throws Exception
    {
        StorPoolData storPool = storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            true, // create
            false
        );
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());

        resultSet.close();

        driver.delete(storPool);
        commit();

        resultSet = stmt.executeQuery();

        assertFalse("Database did not delete storPool", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExist() throws Exception
    {
        StorPoolData storPool = new StorPoolData(
            uuid,
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();

        driver.ensureEntryExists(storPool);
        commit();

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        driver.ensureEntryExists(storPool);

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        StorPoolData storPool = new StorPoolData(
            uuid,
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(storPool);

        storPoolDataFactory.getInstance(
            SYS_CTX,
            node,
            spdd,
            LvmDriver.class.getSimpleName(),
            false,
            true
        );
    }
}
