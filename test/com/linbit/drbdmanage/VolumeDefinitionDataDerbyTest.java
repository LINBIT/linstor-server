package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.core.CoreUtils;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.utils.UuidUtils;

public class VolumeDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_VOL_DFN =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " + VLM_NR + ", " +
                     VLM_SIZE + ", " + VLM_MINOR_NR + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUME_DEFINITIONS;

    private TransactionMgr transMgr;

    private ResourceName resName;
    private ResourceDefinition resDfn;

    private java.util.UUID uuid;
    private VolumeNumber volNr;
    private MinorNumber minor;
    private long volSize;

    private VolumeDefinitionData volDfn;

    private VolumeDefinitionDataDerbyDriver driver;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_VOLUME_DEFINITIONS + " table's column count has changed. Update tests accordingly!",
            6,
            TBL_COL_COUNT_VOLUME_DEFINITIONS
        );

        transMgr = new TransactionMgr(getConnection());

        resName = new ResourceName("TestResource");
        resDfn = ResourceDefinitionData.getInstance(sysCtx, resName, null, transMgr, true);

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
            transMgr
        );

        driver = new VolumeDefinitionDataDerbyDriver(sysCtx, errorReporter);
    }

    @Test
    public void testPersist() throws Exception
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        driver.create(volDfn, transMgr);

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
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn, transMgr);

        VolumeDefinitionData loadedVd = driver.load(resDfn, volNr, transMgr);
        assertNotNull(loadedVd);
        assertEquals(uuid, loadedVd.getUuid());
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVd.getVolumeSize(sysCtx));
        assertEquals(minor, loadedVd.getMinorNr(sysCtx));
        assertTrue(loadedVd.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(volDfn, transMgr);

        VolumeDefinitionData loadedVd = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            null,
            transMgr,
            false
        );

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVd.getVolumeSize(sysCtx));
        assertEquals(minor, loadedVd.getMinorNr(sysCtx));
        assertTrue(loadedVd.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(volDfn, transMgr);

        List<VolumeDefinition> volDfnList = driver.loadAllVolumeDefinitionsByResourceDefinition(
            resDfn,
            transMgr,
            sysCtx
        );

        assertNotNull(volDfnList);
        assertEquals(1, volDfnList.size());
        VolumeDefinition loadedVd = volDfnList.get(0);

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVd.getVolumeSize(sysCtx));
        assertEquals(minor, loadedVd.getMinorNr(sysCtx));
        assertTrue(loadedVd.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(volDfn, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(volDfn, transMgr);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPropsConPersist() throws Exception
    {
        driver.create(volDfn, transMgr);
        volDfn.initialized();
        volDfn.setConnection(transMgr);

        Props props = volDfn.getProps(sysCtx);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);

        transMgr.commit();

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);
        testProps(transMgr, PropsContainer.buildPath(resName, volNr), map, true);
    }

    @Test
    public void testPropsConLoad() throws Exception
    {
        driver.create(volDfn, transMgr);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(transMgr, PropsContainer.buildPath(resName, volNr), testKey, testValue);

        VolumeDefinitionData loadedVd = driver.load(resDfn, volNr, transMgr);
        Props props = loadedVd.getProps(sysCtx);

        assertEquals(testValue, props.getProp(testKey));
        assertEquals(1, props.size());
    }

    @Test
    public void testMinorNrDriverUpdate() throws Exception
    {
        driver.create(volDfn, transMgr);
        MinorNumber newMinorNr = new MinorNumber(32);
        driver.getMinorNumberDriver().update(volDfn, newMinorNr, transMgr);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn, transMgr);
        volDfn.initialized();
        volDfn.setConnection(transMgr);

        MinorNumber minor2 = new MinorNumber(32);
        volDfn.setMinorNr(sysCtx, minor2);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn, transMgr);
        volDfn.initialized();
        volDfn.setConnection(transMgr);

        volDfn.getFlags().disableAllFlags(sysCtx);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn, transMgr);
        long newFlagMask = 0;
        driver.getStateFlagsPersistence().persist(volDfn, newFlagMask, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn, transMgr);

        volDfn.initialized();
        volDfn.setConnection(transMgr);

        long size = 9001;
        volDfn.setVolumeSize(sysCtx, size);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn, transMgr);
        long size = 9001;
        driver.getVolumeSizeDriver().update(volDfn, size, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
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
        CoreUtils.satelliteMode();

        VolumeDefinitionData volDfnSat = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] { VlmDfnFlags.REMOVE },
            null,
            true
        );

        assertNotNull(volDfnSat);
        assertEquals(resName, volDfnSat.getResourceDefinition().getName());
        assertEquals(volNr, volDfnSat.getVolumeNumber(sysCtx));
        assertEquals(volSize, volDfnSat.getVolumeSize(sysCtx));
        assertEquals(minor, volDfnSat.getMinorNr(sysCtx));
        assertTrue(volDfnSat.getFlags().isSet(sysCtx, VlmDfnFlags.REMOVE));

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        CoreUtils.satelliteMode();

        VolumeDefinitionData volDfnSat = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] { VlmDfnFlags.REMOVE },
            null,
            false
        );

        assertNull(volDfnSat);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }
}
