package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Before;
import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.CoreUtils;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

public class StorPoolDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_STOR_POOL_DFNS =
        " SELECT " + UUID + ", " + POOL_NAME + ", " + POOL_DSP_NAME +
        " FROM " + TBL_STOR_POOL_DEFINITIONS;

    private TransactionMgr transMgr;
    private StorPoolName spName;
    private java.util.UUID uuid;
    private ObjectProtection objProt;

    private StorPoolDefinitionData spdd;

    private StorPoolDefinitionDataDerbyDriver driver;

    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_STOR_POOL_DEFINITIONS + " table's column count has changed. Update tests accordingly!", 3, TBL_COL_COUNT_STOR_POOL_DEFINITIONS);

        transMgr = new TransactionMgr(getConnection());

        uuid = randomUUID();
        spName = new StorPoolName("TestStorPool");
        objProt = ObjectProtection.getInstance(sysCtx, ObjectProtection.buildPathSPD(spName), true, transMgr);
        spdd = new StorPoolDefinitionData(uuid, objProt, spName);

        driver = new StorPoolDefinitionDataDerbyDriver(errorReporter);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(spdd, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS);
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
        StorPoolDefinitionData spd = StorPoolDefinitionData.getInstance(sysCtx, spName, transMgr, true);

        assertNotNull(spd);
        assertNotNull(spd.getUuid());
        assertEquals(spName, spd.getName());
        assertNotNull(spd.getObjProt());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS);
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
        DriverUtils.clearCaches();

        StorPoolDefinitionData loadedSpdd = driver.load(spName, transMgr);

        assertNotNull(loadedSpdd);
        assertEquals(uuid, loadedSpdd.getUuid());
        assertEquals(spName, loadedSpdd.getName());
        assertNotNull(loadedSpdd.getObjProt());
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(spdd, transMgr);
        DriverUtils.clearCaches();

        StorPoolDefinitionData loadedSpdd = StorPoolDefinitionData.getInstance(sysCtx, spName, transMgr, false);
        assertNotNull(loadedSpdd);
        assertEquals(uuid, loadedSpdd.getUuid());
        assertEquals(spName, loadedSpdd.getName());
        assertNotNull(loadedSpdd.getObjProt());
    }

    @Test
    public void testCache() throws Exception
    {
        driver.create(spdd, transMgr);

        // no clearCaches

        assertEquals(spdd, driver.load(spName, transMgr));
    }

    @Test
    public void testCacheGetInstance() throws Exception
    {
        driver.create(spdd, transMgr);

        // no clearCaches
        assertEquals(spdd, StorPoolDefinitionData.getInstance(sysCtx, spName, transMgr, false));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(spdd, transMgr);
        DriverUtils.clearCaches();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        resultSet.close();

        driver.delete(spdd, transMgr);
        DriverUtils.clearCaches();

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();

        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        CoreUtils.satelliteMode();

        StorPoolDefinitionData spddSat = StorPoolDefinitionData.getInstance(sysCtx, spName, null, true);

        assertNotNull(spddSat);
        assertNotNull(spddSat.getUuid());
        assertEquals(spName, spddSat.getName());
        assertNotNull(spddSat.getObjProt());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        CoreUtils.satelliteMode();

        StorPoolDefinitionData spddSat = StorPoolDefinitionData.getInstance(sysCtx, spName, null, false);

        assertNull(spddSat);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_STOR_POOL_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        driver.create(spdd, transMgr);
        DriverUtils.clearCaches();

        StorPoolName halfValidName = new StorPoolName(spdd.getName().value);

        StorPoolDefinitionData loadedSpdd = driver.load(halfValidName, transMgr);

        assertNotNull(loadedSpdd);
        assertEquals(spdd.getName(), loadedSpdd.getName());
        assertEquals(spdd.getUuid(), loadedSpdd.getUuid());
    }

}
