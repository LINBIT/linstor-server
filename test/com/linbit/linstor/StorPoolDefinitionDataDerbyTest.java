package com.linbit.linstor;

import com.google.inject.Inject;
import com.linbit.TransactionMgr;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.UuidUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StorPoolDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT =
        " SELECT " + UUID + ", " + POOL_NAME + ", " + POOL_DSP_NAME +
        " FROM " + TBL_STOR_POOL_DEFINITIONS +
        " WHERE " + UUID + " <> x'f51611c6528f4793a87a866d09e6733a' AND " + // default storage pool
                    UUID + " <> x'622807ebc8c444f0b03da08173c8fa1b'"; // default diskless storage pool

    private TransactionMgr transMgr;
    private StorPoolName spName;
    private java.util.UUID uuid;
    private ObjectProtection objProt;

    private StorPoolDefinitionData spdd;

    @Inject private StorPoolDefinitionDataDerbyDriver driver;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_STOR_POOL_DEFINITIONS + " table's column count has changed. Update tests accordingly!", 3, TBL_COL_COUNT_STOR_POOL_DEFINITIONS);

        transMgr = new TransactionMgr(getConnection());

        uuid = randomUUID();
        spName = new StorPoolName("TestStorPool");
        objProt = objectProtectionFactory.getInstance(SYS_CTX, ObjectProtection.buildPathSPD(spName), true, transMgr);
        spdd = new StorPoolDefinitionData(uuid, objProt, spName, driver, propsContainerFactory);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(spdd, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(spName.value, resultSet.getString(POOL_NAME));
        assertEquals(spName.displayValue, resultSet.getString(POOL_DSP_NAME));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        StorPoolDefinitionData spd = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, transMgr, true, false);

        assertNotNull(spd);
        assertNotNull(spd.getUuid());
        assertEquals(spName, spd.getName());
        assertNotNull(spd.getObjProt());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(spd.getUuid(), UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(spName.value, resultSet.getString(POOL_NAME));
        assertEquals(spName.displayValue, resultSet.getString(POOL_DSP_NAME));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(spdd, transMgr);

        StorPoolDefinitionData loadedSpdd = driver.load(spName, true, transMgr);

        assertNotNull(loadedSpdd);
        assertEquals(uuid, loadedSpdd.getUuid());
        assertEquals(spName, loadedSpdd.getName());
        assertNotNull(loadedSpdd.getObjProt());
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(spdd, transMgr);

        StorPoolDefinitionData loadedSpdd = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, transMgr, false, false);
        assertNotNull(loadedSpdd);
        assertEquals(uuid, loadedSpdd.getUuid());
        assertEquals(spName, loadedSpdd.getName());
        assertNotNull(loadedSpdd.getObjProt());
    }

    @Test
    public void testCache() throws Exception
    {
        driver.create(spdd, transMgr);
        super.storPoolDfnMap.put(spName, spdd);
        // no clearCaches

        assertEquals(spdd, driver.load(spName, true, transMgr));
    }

    @Test
    public void testCacheGetInstance() throws Exception
    {
        driver.create(spdd, transMgr);
        storPoolDfnMap.put(spName, spdd);

        // no clearCaches
        assertEquals(spdd, storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, transMgr, false, false));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(spdd, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        resultSet.close();

        driver.delete(spdd, transMgr);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();

        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        satelliteMode();

        StorPoolDefinitionData spddSat = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, null, true, false);

        assertNotNull(spddSat);
        assertNotNull(spddSat.getUuid());
        assertEquals(spName, spddSat.getName());
        assertNotNull(spddSat.getObjProt());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        satelliteMode();

        StorPoolDefinitionData spddSat = storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, null, false, false);

        assertNull(spddSat);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        driver.create(spdd, transMgr);

        StorPoolName halfValidName = new StorPoolName(spdd.getName().value);

        StorPoolDefinitionData loadedSpdd = driver.load(halfValidName, true, transMgr);

        assertNotNull(loadedSpdd);
        assertEquals(spdd.getName(), loadedSpdd.getName());
        assertEquals(spdd.getUuid(), loadedSpdd.getUuid());
    }

    private StorPoolDefinitionData findStorPoolDefinitionDatabyName(
            List<StorPoolDefinitionData> listStorPoolDefs,
            StorPoolName spName)
    {
        StorPoolDefinitionData data = null;
        for (StorPoolDefinitionData spdd : listStorPoolDefs)
        {
            if (spdd.getName().equals(spName))
            {
                data = spdd;
                break;
            }
        }
        return data;
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(spdd, transMgr);
        StorPoolName spName2 = new StorPoolName("StorPoolName2");
        storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName2, transMgr, true, false);

        List<StorPoolDefinitionData> storpools = driver.loadAll(transMgr);

        assertNotNull(findStorPoolDefinitionDatabyName(storpools, new StorPoolName(ConfigModule.DEFAULT_STOR_POOL_NAME)));
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
        driver.create(spdd, transMgr);

        storPoolDefinitionDataFactory.getInstance(SYS_CTX, spName, transMgr, false, true);
    }
}
