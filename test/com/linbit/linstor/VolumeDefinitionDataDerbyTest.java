package com.linbit.linstor;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.utils.UuidUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VolumeDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_VOL_DFN =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " + VLM_NR + ", " +
                     VLM_SIZE + ", " + VLM_MINOR_NR + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUME_DEFINITIONS;

    private ResourceName resName;
    private Integer resPort;
    private ResourceDefinition resDfn;

    private java.util.UUID uuid;
    private VolumeNumber volNr;
    private int minor;
    private long volSize;

    private VolumeDefinitionData volDfn;

    @Inject private VolumeDefinitionDataDerbyDriver driver;

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
            SYS_CTX, resName, resPort, null, "secret", TransportType.IP
        );

        uuid = randomUUID();
        volNr = new VolumeNumber(13);
        minor = 42;
        volSize = 5_000_000;
        volDfn = new VolumeDefinitionData(
            uuid,
            SYS_CTX,
            resDfn,
            volNr,
            new MinorNumber(minor),
            minorNrPoolMock,
            volSize,
            VlmDfnFlags.DELETE.flagValue,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
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
        assertEquals(minor, resultSet.getInt(VLM_MINOR_NR));
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
                resPort + 1, // prevent tcp-port-conflict
                null,
                "secret",
                TransportType.IP
        );

        volumeDefinitionDataFactory.create(
            SYS_CTX,
            resDefinitionTest,
            volNr,
            minor,
            volSize,
            new VlmDfnFlags[] { VlmDfnFlags.DELETE }
        );
        commit();

        resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals("TESTRESOURCE2", resultSet.getString(RESOURCE_NAME));
        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
        assertEquals(volSize, resultSet.getLong(VLM_SIZE));
        assertEquals(minor, resultSet.getInt(VLM_MINOR_NR));
        assertEquals(VlmDfnFlags.DELETE.flagValue, resultSet.getLong(VLM_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(volDfn);

        VolumeDefinitionData loadedVd = driver.load(resDfn, volNr, true);
        assertNotNull(loadedVd);
        assertEquals(uuid, loadedVd.getUuid());
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber());
        assertEquals(volSize, loadedVd.getVolumeSize(SYS_CTX));
        assertEquals(minor, loadedVd.getMinorNr(SYS_CTX).value);
        assertTrue(loadedVd.getFlags().isSet(SYS_CTX, VlmDfnFlags.DELETE));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(volDfn);

        VolumeDefinitionData loadedVd = volumeDefinitionDataFactory.load(
            SYS_CTX,
            resDfn,
            volNr
        );

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber());
        assertEquals(volSize, loadedVd.getVolumeSize(SYS_CTX));
        assertEquals(minor, loadedVd.getMinorNr(SYS_CTX).value);
        assertTrue(loadedVd.getFlags().isSet(SYS_CTX, VlmDfnFlags.DELETE));
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(volDfn);

        List<VolumeDefinition> volDfnList = driver.loadAllVolumeDefinitionsByResourceDefinition(
            resDfn,
            SYS_CTX
        );

        assertNotNull(volDfnList);
        assertEquals(1, volDfnList.size());
        VolumeDefinition loadedVd = volDfnList.get(0);

        assertNotNull(loadedVd);
        assertEquals(resName, loadedVd.getResourceDefinition().getName());
        assertEquals(volNr, loadedVd.getVolumeNumber());
        assertEquals(volSize, loadedVd.getVolumeSize(SYS_CTX));
        assertEquals(minor, loadedVd.getMinorNr(SYS_CTX).value);
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

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testMinorNrDriverUpdate() throws Exception
    {
        driver.create(volDfn);
        MinorNumber newMinorNr = new MinorNumber(32);
        driver.getMinorNumberDriver().update(volDfn, newMinorNr);

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(newMinorNr.value, resultSet.getInt(VLM_MINOR_NR));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testMinorNrInstanceUpdate() throws Exception
    {
        driver.create(volDfn);

        MinorNumber minor2 = new MinorNumber(32);
        volDfn.setMinorNr(SYS_CTX, minor2);

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VOL_DFN);
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

        volumeDefinitionDataFactory.create(SYS_CTX, resDfn, volNr, minor, volSize, null);
    }

    @Test
    public void testAutoAllocateMinorNumber() throws Exception
    {
        final int testMinorNumber = 9876;
        final VolumeNumber testVolumeNumber = new VolumeNumber(99);

        Mockito.when(minorNrPoolMock.autoAllocate()).thenReturn(testMinorNumber);

        VolumeDefinitionData newvolDfn = volumeDefinitionDataFactory.create(
            SYS_CTX,
            resDfn,
            testVolumeNumber,
            null, // auto allocate
            volSize,
            null
        );

        assertThat(newvolDfn.getMinorNr(SYS_CTX).value).isEqualTo(testMinorNumber);
    }

    @Test
    public void testDeleteDeallocateMinorNumber() throws Exception
    {
        driver.create(volDfn);
        volDfn.delete(SYS_CTX);

        Mockito.verify(minorNrPoolMock).deallocate(minor);
    }

    @Test
    public void testModifyMinorNumber() throws Exception
    {
        final int newMinorNumber = 9876;

        driver.create(volDfn);
        volDfn.setMinorNr(SYS_CTX, new MinorNumber(newMinorNumber));

        Mockito.verify(minorNrPoolMock).deallocate(minor);
        Mockito.verify(minorNrPoolMock).allocate(newMinorNumber);
    }
}
