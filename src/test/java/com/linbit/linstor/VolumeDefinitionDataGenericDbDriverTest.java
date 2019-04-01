package com.linbit.linstor;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.VolumeDefinition.InitMaps;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import org.junit.Before;
import org.junit.Test;
import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VolumeDefinitionDataGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_VOL_DFN =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " + VLM_NR + ", " +
                     VLM_SIZE + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUME_DEFINITIONS;

    private ResourceName resName;
    private Integer resPort;
    private ResourceDefinition resDfn;

    private java.util.UUID uuid;
    private VolumeNumber volNr;
    private int minor;
    private long volSize;

    private VolumeDefinitionData volDfn;

    @Inject private VolumeDefinitionDataGenericDbDriver driver;

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(TBL_VOLUME_DEFINITIONS + " table's column count has changed. Update tests accordingly!",
            6,
            TBL_COL_COUNT_VOLUME_DEFINITIONS
        );

        resName = new ResourceName("TestResource");
        resPort = 9001;
        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX, resName, null, resPort, null, "secret", TransportType.IP, new ArrayList<>(), null
        );

        uuid = randomUUID();
        volNr = new VolumeNumber(13);
        minor = 42;
        volSize = 5_000_000;
        volDfn = new VolumeDefinitionData(
            uuid,
            resDfn,
            volNr,
            volSize,
            VlmDfnFlags.DELETE.flagValue,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        driver.create(volDfn);
        commit();

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(volSize, resultSet.getLong(VLM_SIZE));
        assertEquals(VlmDfnFlags.DELETE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        ResourceDefinition resDefinitionTest = resourceDefinitionDataFactory.create(
            SYS_CTX,
            new ResourceName("TestResource2"),
            null,
            resPort + 1, // prevent tcp-port-conflict
            null,
            "secret",
            TransportType.IP,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            null
        );

        volumeDefinitionDataFactory.create(
            SYS_CTX,
            resDefinitionTest,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] {VlmDfnFlags.DELETE}
        );
        commit();

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals("TESTRESOURCE2", resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(volSize, resultSet.getLong(VLM_SIZE));
        assertEquals(VlmDfnFlags.DELETE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(volDfn);
        ((ResourceDefinitionData) resDfn).putVolumeDefinition(SYS_CTX, volDfn);

        VolumeDefinitionData loadedVd = (VolumeDefinitionData) resDfn.getVolumeDfn(SYS_CTX, volNr);

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber());
        assertEquals(volSize, loadedVd.getVolumeSize(SYS_CTX));
        assertTrue(loadedVd.getFlags().isSet(SYS_CTX, VlmDfnFlags.DELETE));
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(volDfn);
        rscDfnMap.put(resName, resDfn);

        Map<VolumeDefinitionData, InitMaps> volDfnList = driver.loadAll(rscDfnMap);

        assertNotNull(volDfnList);
        assertEquals(1, volDfnList.size());
        VolumeDefinition loadedVd = volDfnList.keySet().iterator().next();

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber());
        assertEquals(volSize, loadedVd.getVolumeSize(SYS_CTX));
        assertTrue(loadedVd.getFlags().isSet(SYS_CTX, VlmDfnFlags.DELETE));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(volDfn);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(volDfn);
        commit();

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPropsConPersist() throws Exception
    {
        driver.create(volDfn);

        Props props = volDfn.getProps(SYS_CTX);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);

        commit();

        Map<String, String> map = new HashMap<>();
        map.put(testKey, testValue);
        testProps(PropsContainer.buildPath(resName, volNr), map);
    }

    @Test
    public void testFlagsUpdateInstance() throws Exception
    {
        driver.create(volDfn);

        volDfn.getFlags().disableAllFlags(SYS_CTX);

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
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
        driver.create(volDfn);
        commit();
        long newFlagMask = 0;
        driver.getStateFlagsPersistence().persist(volDfn, newFlagMask);

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(newFlagMask, resultSet.getLong(VLM_FLAGS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testVolSizeUpdateInstance() throws Exception
    {
        driver.create(volDfn);

        long size = 9001;
        volDfn.setVolumeSize(SYS_CTX, size);

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(size, resultSet.getLong(VLM_SIZE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testVolSizeUpdateDriver() throws Exception
    {
        driver.create(volDfn);
        commit();
        long size = 9001;
        driver.getVolumeSizeDriver().update(volDfn, size);

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(size, resultSet.getLong(VLM_SIZE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(volDfn);
        ((ResourceDefinitionData) resDfn).putVolumeDefinition(SYS_CTX, volDfn);

        volumeDefinitionDataFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);
    }
}
