package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.Pair;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ResourceConnectionDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_RES_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " +
                     NODE_NAME_DST + ", " + RESOURCE_NAME +
        " FROM " + TBL_RESOURCE_CONNECTIONS;

    private final ResourceName resName;
    private final Integer resPort;
    private final NodeName sourceName;
    private final NodeName targetName;

    private java.util.UUID uuid;
    private ResourceDefinition resDfn;
    private Node nodeSrc;
    private Node nodeDst;

    private ResourceConnection resCon;

    @Inject
    private ResourceConnectionDbDriver driver;

    private Integer nodeIdSrc;
    private Integer nodeIdDst;

    private Resource resSrc;
    private Resource resDst;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResourceConnectionDbDriverTest() throws InvalidNameException
    {
        resName = new ResourceName("testResourceName");
        resPort = 9001;

        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_RESOURCE_CONNECTIONS + " table's column count has changed. Update tests accordingly!",
            6,
            TBL_COL_COUNT_RESOURCE_CONNECTIONS
        );

        uuid = randomUUID();

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.tcpPort = resPort;
        drbdRscDfn.sharedSecret = "secret";
        drbdRscDfn.transportType = TransportType.IP;
        resDfn = resourceDefinitionFactory.create(
            SYS_CTX,
            resName,
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup(SYS_CTX)
        );
        rscDfnMap.put(resDfn.getName(), resDfn);
        nodeSrc = nodeFactory.create(SYS_CTX, sourceName, null, null);
        nodeDst = nodeFactory.create(SYS_CTX, targetName, null, null);

        nodeIdSrc = 13;
        nodeIdDst = 14;

        LayerPayload payLoadSrc = new LayerPayload();
        payLoadSrc.getDrbdRsc().nodeId = nodeIdSrc;
        LayerPayload payLoadDst = new LayerPayload();
        payLoadDst.getDrbdRsc().nodeId = nodeIdDst;

        resSrc = resourceFactory.create(SYS_CTX, resDfn, nodeSrc, payLoadSrc, null, Collections.emptyList());
        resDst = resourceFactory.create(SYS_CTX, resDfn, nodeDst, payLoadDst, null, Collections.emptyList());

        resCon = TestFactory.createResourceConnection(
            uuid,
            resSrc,
            resDst,
            null,
            tcpPortPoolMock,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            0,
            SYS_CTX
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(resCon);
        commit();

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        resourceConnectionFactory.create(SYS_CTX, resSrc, resDst, null);
        commit();

        checkDbPersist(false);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(resCon);

        Map<Pair<NodeName, ResourceName>, Resource> rscmap = new HashMap<>();
        rscmap.put(new Pair<NodeName, ResourceName>(sourceName, resName), resSrc);
        rscmap.put(new Pair<NodeName, ResourceName>(targetName, resName), resDst);
        List<ResourceConnection> cons = driver.loadAllAsList(rscmap);

        assertNotNull(cons);

        assertEquals(1, cons.size());

        ResourceConnection loadedConDfn = cons.get(0);
        assertNotNull(loadedConDfn);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(resCon);
        resSrc.setAbsResourceConnection(SYS_CTX, resCon);
        resDst.setAbsResourceConnection(SYS_CTX, resCon);

        ResourceConnection loadedSrcConDfn = resSrc.getAbsResourceConnection(SYS_CTX, resDst);
        ResourceConnection loadedDstConDfn = resDst.getAbsResourceConnection(SYS_CTX, resSrc);

        checkLoadedConDfn(loadedSrcConDfn, true);
        checkLoadedConDfn(loadedDstConDfn, true);
        assertEquals(loadedSrcConDfn, loadedDstConDfn);
    }

    @Test
    public void testCache() throws Exception
    {
        ResourceConnection storedInstance = resourceConnectionFactory.create(
            SYS_CTX,
            resSrc,
            resDst,
            null
        );

        // no clear-cache

        assertEquals(storedInstance, resSrc.getAbsResourceConnection(SYS_CTX, resDst));
        assertEquals(storedInstance, resDst.getAbsResourceConnection(SYS_CTX, resSrc));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(resCon);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(resCon);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        if (checkUuid)
        {
            assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        }
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(sourceName.value, resultSet.getString(NODE_NAME_SRC));
        assertEquals(targetName.value, resultSet.getString(NODE_NAME_DST));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    private void checkLoadedConDfn(ResourceConnection loadedConDfn, boolean checkUuid) throws AccessDeniedException
    {
        assertNotNull(loadedConDfn);
        if (checkUuid)
        {
            assertEquals(uuid, loadedConDfn.getUuid());
        }
        Resource sourceResource = loadedConDfn.getSourceResource(SYS_CTX);
        Resource targetResource = loadedConDfn.getTargetResource(SYS_CTX);

        assertEquals(resName, sourceResource.getResourceDefinition().getName());
        assertEquals(sourceName, sourceResource.getNode().getName());
        assertEquals(targetName, targetResource.getNode().getName());
        assertEquals(sourceResource.getResourceDefinition().getName(), targetResource.getResourceDefinition().getName());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(resCon);
        resSrc.setAbsResourceConnection(SYS_CTX, resCon);
        resDst.setAbsResourceConnection(SYS_CTX, resCon);

        resourceConnectionFactory.create(SYS_CTX, resSrc, resDst, null);
    }
}
