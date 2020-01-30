package com.linbit.linstor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

public class StorPoolDefinitionDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_STOR_POOL_DFNS_EXCEPT_DEFAULT =
        " SELECT " + UUID + ", " + POOL_NAME + ", " + POOL_DSP_NAME +
        " FROM " + TBL_STOR_POOL_DEFINITIONS +
        " WHERE " + UUID + " <> 'f51611c6-528f-4793-a87a-866d09e6733a' AND " + // default storage pool
                    UUID + " <> '622807eb-c8c4-44f0-b03d-a08173c8fa1b'"; // default diskless storage pool

    private StorPoolName spName;
    private java.util.UUID uuid;
    private ObjectProtection objProt;

    private StorPoolDefinition spdd;

    @Inject
    private StorPoolDefinitionDbDriver driver;

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
        objProt = objectProtectionFactory.getInstance(SYS_CTX, ObjectProtection.buildPath(spName), true);
        spdd = TestFactory.createStorPoolDefinition(
            uuid,
            objProt,
            spName,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
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
        StorPoolDefinition spd = storPoolDefinitionFactory.create(SYS_CTX, spName);
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

    private StorPoolDefinition findStorPoolDefinitionbyName(
        Map<StorPoolDefinition, StorPoolDefinition.InitMaps> storPoolDfnMap,
        StorPoolName spNameRef
    )
    {
        StorPoolDefinition data = null;
        for (StorPoolDefinition spddRef : storPoolDfnMap.keySet())
        {
            if (spddRef.getName().equals(spNameRef))
            {
                data = spddRef;
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
        storPoolDefinitionFactory.create(SYS_CTX, spName2);

        Map<StorPoolDefinition, StorPoolDefinition.InitMaps> storpools = driver.loadAll(null);

        StorPoolName disklessStorPoolName = new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME);
        assertNotNull(findStorPoolDefinitionbyName(storpools, disklessStorPoolName));
        assertNotNull(findStorPoolDefinitionbyName(storpools, spName));
        assertNotNull(findStorPoolDefinitionbyName(storpools, spName2));
        assertNotEquals(
            findStorPoolDefinitionbyName(storpools, spName),
            findStorPoolDefinitionbyName(storpools, spName2)
        );
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(spdd);
        storPoolDfnMap.put(spName, spdd);
        storPoolDefinitionFactory.create(SYS_CTX, spName);
    }
}
