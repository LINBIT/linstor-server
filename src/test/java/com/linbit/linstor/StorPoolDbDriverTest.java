package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StorPoolDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_STOR_POOLS =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + POOL_NAME + ", " + DRIVER_NAME +
        " FROM " + TBL_NODE_STOR_POOL +
        " WHERE " + POOL_NAME + " <> '" + LinStor.DISKLESS_STOR_POOL_NAME.toUpperCase() + "'";

    private final NodeName nodeName;
    private final StorPoolName spName;

    private Node node;

    private java.util.UUID uuid;

    private StorPoolDefinition spdd;
    @Inject
    private StorPoolDefinitionDbDriver spdDriver;
    @Inject
    private StorPoolDbDriver driver;

    private FreeSpaceMgr fsm;
    private FreeSpaceMgr disklessFsm;

    public StorPoolDbDriverTest() throws InvalidNameException
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

        node = nodeFactory.create(SYS_CTX, nodeName, Node.Type.SATELLITE, null);
        spdd = storPoolDefinitionFactory.create(SYS_CTX, spName);

        fsm = freeSpaceMgrFactory.getInstance(SYS_CTX, new SharedStorPoolName(node.getName(), spdd.getName()));
        disklessFsm = freeSpaceMgrFactory.getInstance(
            SYS_CTX, new SharedStorPoolName(node.getName(), new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME)));

        uuid = randomUUID();
    }

    @Test
    public void testPersist() throws Exception
    {
        StorPool storPool = TestFactory.createStorPool(
            uuid,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(storPool);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_POOLS);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist storPool", resultSet.next());
        assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(spName.value, resultSet.getString(POOL_NAME));
        assertEquals("LVM", resultSet.getString(DRIVER_NAME));
        assertFalse("Database persisted too many storPools", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistStorGetInstance() throws Exception
    {
        StorPool pool = storPoolFactory.create(
            SYS_CTX,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
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
        assertEquals("LVM", resultSet.getString(DRIVER_NAME));
        assertFalse("Database persisted too many storPools", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadAll() throws Exception
    {
        StorPool storPool = TestFactory.createStorPool(
            uuid,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(storPool);

        Map<NodeName, Node> tmpNodesMap = new HashMap<>();
        Map<StorPoolName, StorPoolDefinition> tmpStorPoolDfnMap = new HashMap<>();

        tmpNodesMap.put(nodeName, node);

        tmpStorPoolDfnMap = spdDriver.loadAll(null).keySet().stream().collect(
            Collectors.toMap(
                tmpSpdd -> tmpSpdd.getName(),
                Function.identity()
            )
        );

        Map<SharedStorPoolName, FreeSpaceMgr> tmpFreeSpaceMgrMap = new HashMap<>();
        tmpFreeSpaceMgrMap.put(fsm.getName(), fsm);
        tmpFreeSpaceMgrMap.put(disklessFsm.getName(), disklessFsm);

        Map<StorPool, StorPool.InitMaps> storPools = driver.loadAll(
            new PairNonNull<>(
                tmpNodesMap,
                tmpStorPoolDfnMap
            )
        );

        assertNotNull(storPools);
        assertEquals(1, storPools.size()); // we didn't create the dfltDisklessStorPool here

        // one of the entries should be the default diskless stor pool, we just skip that
        StorPool storPoolLoaded = storPools.keySet().stream()
            .filter(sp -> sp.getName().equals(spName))
            .findFirst()
            .get();

        assertNotNull(storPoolLoaded);
        assertNotNull(storPoolLoaded.getProps(SYS_CTX));
        StorPoolDefinition spDfn = storPoolLoaded.getDefinition(SYS_CTX);
        assertNotNull(spDfn);
        assertEquals(spName, spDfn.getName());
        assertEquals(DeviceProviderKind.LVM, storPoolLoaded.getDeviceProviderKind());
        assertEquals(spName, storPoolLoaded.getName());
    }

    @Test
    public void testCache() throws Exception
    {
        StorPool storedInstance = storPoolFactory.create(
            SYS_CTX,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
            false
        );

        // no clearCaches

        assertEquals(storedInstance, node.getStorPool(SYS_CTX, spdd.getName()));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        StorPool loadedStorPool = node.getStorPool(SYS_CTX, spdd.getName());

        assertNull(loadedStorPool);

        StorPool storPool = TestFactory.createStorPool(
            uuid,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(storPool);
        node.addStorPool(SYS_CTX, storPool);
        spdd.addStorPool(SYS_CTX, storPool);

        loadedStorPool = node.getStorPool(SYS_CTX, spdd.getName());

        assertEquals(uuid, loadedStorPool.getUuid());
        assertEquals(spName, loadedStorPool.getDefinition(SYS_CTX).getName());
        assertEquals(spdd, loadedStorPool.getDefinition(SYS_CTX));
        assertEquals(DeviceProviderKind.LVM, loadedStorPool.getDeviceProviderKind());
        assertEquals(spName, loadedStorPool.getName());
    }

    @Test
    public void testDelete() throws Exception
    {
        StorPool storPool = storPoolFactory.create(
            SYS_CTX,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
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

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        StorPool storPool = TestFactory.createStorPool(
            uuid,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(storPool);
        node.addStorPool(SYS_CTX, storPool);
        spdd.addStorPool(SYS_CTX, storPool);

        storPoolFactory.create(
            SYS_CTX,
            node,
            spdd,
            DeviceProviderKind.LVM,
            fsm,
            false
        );
    }
}
