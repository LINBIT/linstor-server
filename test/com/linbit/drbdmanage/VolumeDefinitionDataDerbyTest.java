package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.utils.UuidUtils;

public class VolumeDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_VOL_DFN =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " + VLM_NR + ", " +
                     VLM_SIZE + ", " + VLM_MINOR_NR + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUME_DEFINITIONS;

    private Connection con;
    private TransactionMgr transMgr;

    private ResourceName resName;
    private ResourceDefinition resDfn;

    private java.util.UUID uuid;
    private VolumeNumber volNr;
    private MinorNumber minor;
    private long volSize;

    private VolumeDefinitionData volDfn;

    private VolumeDefinitionDataDerbyDriver driver;

    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_VOLUME_DEFINITIONS + " table's column count has changed. Update tests accordingly!",
            6,
            TBL_COL_COUNT_VOLUME_DEFINITIONS
        );

        con = getConnection();
        transMgr = new TransactionMgr(con);

        resName = new ResourceName("TestResource");
        resDfn = ResourceDefinitionData.getInstance(sysCtx, resName, null, null, transMgr, true);

        uuid = randomUUID();
        volNr = new VolumeNumber(13);
        minor = new MinorNumber(42);
        volSize = 5_000_000;
        volDfn = new VolumeDefinitionData(
            uuid,
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            VlmDfnFlags.REMOVE.flagValue,
            null,
            transMgr
        );

        driver = new VolumeDefinitionDataDerbyDriver(sysCtx, resDfn, volNr);
    }

    @Test
    public void testPersist() throws Exception
    {
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        driver.create(con, volDfn);

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(volSize, resultSet.getLong(VLM_SIZE));
        assertEquals(minor.value, resultSet.getInt(VLM_MINOR_NR));
        assertEquals(VlmDfnFlags.REMOVE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] { VlmDfnFlags.REMOVE },
            null,
            transMgr,
            true
        );

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(volSize, resultSet.getLong(VLM_SIZE));
        assertEquals(minor.value, resultSet.getInt(VLM_MINOR_NR));
        assertEquals(VlmDfnFlags.REMOVE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(con, volDfn);
        DriverUtils.clearCaches();

        VolumeDefinitionData loadedVd = driver.load(null, transMgr);
        assertNotNull(loadedVd);
        assertEquals(uuid, loadedVd.getUuid());
        assertEquals(resName, loadedVd.getResourceDfn().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVd.getVolumeSize(sysCtx));
        assertEquals(minor, loadedVd.getMinorNr(sysCtx));
        assertTrue(loadedVd.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(con, volDfn);
        DriverUtils.clearCaches();

        VolumeDefinitionData loadedVd = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            null,
            null,
            transMgr,
            false
        );

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDfn().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVd.getVolumeSize(sysCtx));
        assertEquals(minor, loadedVd.getMinorNr(sysCtx));
        assertTrue(loadedVd.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
    }

    @Test
    public void testLoadStatic() throws Exception
    {
        driver.create(con, volDfn);
        DriverUtils.clearCaches();

        List<VolumeDefinition> volDfnList = VolumeDefinitionDataDerbyDriver.loadAllVolumeDefinitionsByResourceDefinition(
            resDfn,
            null,
            transMgr,
            sysCtx
        );

        assertNotNull(volDfnList);
        assertEquals(1, volDfnList.size());
        VolumeDefinition loadedVd = volDfnList.get(0);

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDfn().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVd.getVolumeSize(sysCtx));
        assertEquals(minor, loadedVd.getMinorNr(sysCtx));
        assertTrue(loadedVd.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
    }

    @Test
    public void testCache() throws Exception
    {
        driver.create(con, volDfn);

        // no clearCaches

        assertEquals(volDfn, driver.load(null, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(con, volDfn);
        DriverUtils.clearCaches();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(con);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPropsConPersist() throws Exception
    {
        driver.create(con, volDfn);
        volDfn.initialized();
        volDfn.setConnection(transMgr);

        Props props = volDfn.getProps(sysCtx);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);

        transMgr.commit();

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);
        testProps(con, driver.getPropsDriver().getInstanceName(), map, true);
    }

    @Test
    public void testPropsConLoad() throws Exception
    {
        driver.create(con, volDfn);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(con, driver.getPropsDriver().getInstanceName(), testKey, testValue);

        DriverUtils.clearCaches();

        VolumeDefinitionData loadedVd = driver.load(null, transMgr);
        Props props = loadedVd.getProps(sysCtx);

        assertEquals(testValue, props.getProp(testKey));
        assertNotNull(props.getProp(SerialGenerator.KEY_SERIAL));
        assertEquals(2, props.size());
    }

    @Test
    public void testMinorNrDriverUpdate() throws Exception
    {
        driver.create(con, volDfn);
        MinorNumber newMinorNr = new MinorNumber(32);
        driver.getMinorNumberDriver().update(con, newMinorNr);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(newMinorNr.value, resultSet.getInt(VLM_MINOR_NR));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testMinorNrInstanceUpdate() throws Exception
    {
        driver.create(con, volDfn);
        volDfn.initialized();
        volDfn.setConnection(transMgr);

        MinorNumber minor2 = new MinorNumber(32);
        volDfn.setMinorNr(sysCtx, minor2);

        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(minor2.value, resultSet.getInt(VLM_MINOR_NR));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testFlagsUpdateInstance() throws Exception
    {
        driver.create(con, volDfn);
        volDfn.initialized();
        volDfn.setConnection(transMgr);

        volDfn.getFlags().disableAllFlags(sysCtx);

        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(VLM_FLAGS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testFlagsUpdateDriver() throws Exception
    {
        driver.create(con, volDfn);
        long newFlagMask = 0;
        driver.getStateFlagsPersistence().persist(con, newFlagMask);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(newFlagMask, resultSet.getLong(VLM_FLAGS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testVolSizeUpdateInstance() throws Exception
    {
        driver.create(con, volDfn);

        volDfn.initialized();
        volDfn.setConnection(transMgr);

        long size = 9001;
        volDfn.setVolumeSize(sysCtx, size);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(size, resultSet.getLong(VLM_SIZE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testVolSizeUpdateDriver() throws Exception
    {
        driver.create(con, volDfn);
        long size = 9001;
        driver.getVolumeSizeDriver().update(con, size);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(size, resultSet.getLong(VLM_SIZE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        VolumeDefinitionData volDfnSat = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] { VlmDfnFlags.REMOVE },
            null,
            null,
            true
        );

        assertNotNull(volDfnSat);
        assertEquals(resName, volDfnSat.getResourceDfn().getName());
        assertEquals(volNr, volDfnSat.getVolumeNumber(sysCtx));
        assertEquals(volSize, volDfnSat.getVolumeSize(sysCtx));
        assertEquals(minor, volDfnSat.getMinorNr(sysCtx));
        assertTrue(volDfnSat.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        VolumeDefinitionData volDfnSat = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] { VlmDfnFlags.REMOVE },
            null,
            null,
            false
        );

        assertNull(volDfnSat);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }
}
