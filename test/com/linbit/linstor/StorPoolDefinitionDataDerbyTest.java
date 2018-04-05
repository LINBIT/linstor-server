package com.linbit.linstor;

import javax.inject.Inject;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StorPoolDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT =
        " SELECT " + UUID + ", " + POOL_NAME + ", " + POOL_DSP_NAME +
        " FROM " + TBL_STOR_POOL_DEFINITIONS +
        " WHERE " + UUID + " <> 'f51611c6-528f-4793-a87a-866d09e6733a' AND " + // default storage pool
                    UUID + " <> '622807eb-c8c4-44f0-b03d-a08173c8fa1b'"; // default diskless storage pool

    private StorPoolName spName;
    private java.util.UUID uuid;
    private ObjectProtection objProt;

    private StorPoolDefinitionData spdd;

    @Inject private StorPoolDefinitionDataDerbyDriver driver;

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_STOR_POOL_DEFINITIONS + " table's column count has changed. Update tests accordingly!",
            3,
            TBL_COL_COUNT_STOR_POOL_DEFINITIONS
        );

        uuid = randomUUID();
        spName = new StorPoolName("TestStorPool");
        objProt = objectProtectionFactory.getInstance(SYS_CTX, ObjectProtection.buildPathSPD(spName), true);
        spdd = new StorPoolDefinitionData(
            uuid,
            objProt,
            spName,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(spdd);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(spName.value, resultSet.getString(POOL_NAME));
        assertEquals(spName.displayValue, resultSet.getString(POOL_DSP_NAME));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        StorPoolDefinitionData spd = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, true, false);
        commit();

        assertNotNull(spd);
        assertNotNull(spd.getUuid());
        assertEquals(spName, spd.getName());
        assertNotNull(spd.getObjProt());

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(spd.getUuid(), java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(spName.value, resultSet.getString(POOL_NAME));
        assertEquals(spName.displayValue, resultSet.getString(POOL_DSP_NAME));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(spdd);

        StorPoolDefinitionData loadedSpdd = driver.load(spName, true);

        assertNotNull(loadedSpdd);
        assertEquals(uuid, loadedSpdd.getUuid());
        assertEquals(spName, loadedSpdd.getName());
        assertNotNull(loadedSpdd.getObjProt());
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(spdd);

        StorPoolDefinitionData loadedSpdd = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, false, false);
        assertNotNull(loadedSpdd);
        assertEquals(uuid, loadedSpdd.getUuid());
        assertEquals(spName, loadedSpdd.getName());
        assertNotNull(loadedSpdd.getObjProt());
    }

    @Test
    public void testCache() throws Exception
    {
        driver.create(spdd);
        super.storPoolDfnMap.put(spName, spdd);
        // no clearCaches

        assertEquals(spdd, driver.load(spName, true));
    }

    @Test
    public void testCacheGetInstance() throws Exception
    {
        driver.create(spdd);
        storPoolDfnMap.put(spName, spdd);

        // no clearCaches
        assertEquals(spdd, storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, false, false));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(spdd);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        resultSet.close();

        driver.delete(spdd);
        commit();

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();

        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        driver.create(spdd);

        StorPoolName halfValidName = new StorPoolName(spdd.getName().value);

        StorPoolDefinitionData loadedSpdd = driver.load(halfValidName, true);

        assertNotNull(loadedSpdd);
        assertEquals(spdd.getName(), loadedSpdd.getName());
        assertEquals(spdd.getUuid(), loadedSpdd.getUuid());
    }

    private StorPoolDefinitionData findStorPoolDefinitionDatabyName(
        List<StorPoolDefinitionData> listStorPoolDefs,
        StorPoolName spNameRef
    )
    {
        StorPoolDefinitionData data = null;
        for (StorPoolDefinitionData storPoolDFn : listStorPoolDefs)
        {
            if (storPoolDFn.getName().equals(spNameRef))
            {
                data = storPoolDFn;
                break;
            }
        }
        return data;
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(spdd);
        StorPoolName spName2 = new StorPoolName("StorPoolName2");
        storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName2, true, false);

        List<StorPoolDefinitionData> storpools = driver.loadAll();

        assertNotNull(
            findStorPoolDefinitionDatabyName(
                storpools,
                new StorPoolName(ConfigModule.DEFAULT_STOR_POOL_NAME)
            )
        );
        assertNotNull(findStorPoolDefinitionDatabyName(storpools, spName));
        assertNotNull(findStorPoolDefinitionDatabyName(storpools, spName2));
        assertNotEquals(
                findStorPoolDefinitionDatabyName(storpools, spName),
                findStorPoolDefinitionDatabyName(storpools, spName2)
        );
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(spdd);

        storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, false, true);
    }
}
