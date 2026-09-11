package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import jakarta.inject.Inject;

import java.util.Collections;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests for the atomic snapshot/resource guards that decide whether the
 * resource-definition survives a truncate or an "if empty" delete.
 *
 * <p>These tests exercise the decision logic without driving the satellite-based
 * resource-deletion machinery: an empty resource-definition is removed via a pure
 * DB path ({@code truncateRscDfnInTransaction} short-circuits at resource count 0),
 * and every "keep" path is a no-op that never contacts a satellite. Truncation of a
 * resource-definition that actually holds resources is covered indirectly by the
 * pre-existing {@code rd delete} usage of {@link CtrlRscDfnTruncateApiCallHandler}.
 */
public class CtrlRscDfnDeleteApiCallHandlerTest extends ApiTestBase
{
    @Inject private CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandler;

    @Mock
    protected Peer mockSatellite;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    private final NodeName satelliteName;
    private Node satelliteNode;

    public CtrlRscDfnDeleteApiCallHandlerTest() throws Exception
    {
        satelliteName = new NodeName("TestSatellite");
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(mockSatellite.apiCall(anyString(), any())).thenReturn(Flux.empty());
        satelliteNode = nodeFactory.create(satelliteName, Node.Type.SATELLITE, new Node.Flags[0]);
        satelliteNode.setPeer(mockSatellite);
        nodesMap.put(satelliteName, satelliteNode);

        commitAndCleanUp(true);
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void deleteIfEmptyRemovesEmptyRscDfn() throws Exception
    {
        ResourceName rscName = new ResourceName("EmptyRscDfn");
        ResourceDefinition rscDfn = seedRscDfn(rscName, false, false);

        ApiCallRc rc = execute(ctrlRscDfnDeleteApiCallHandler.deleteResourceDefinitionIfEmpty(rscName));

        assertThat(rscDfn.isDeleted()).isTrue();
        assertThat(hasCode(rc, ApiConsts.DELETED)).isTrue();
    }

    @Test
    public void deleteIfEmptyKeepsRscDfnWithResource() throws Exception
    {
        ResourceName rscName = new ResourceName("RscDfnWithRsc");
        ResourceDefinition rscDfn = seedRscDfn(rscName, true, false);

        ApiCallRc rc = execute(ctrlRscDfnDeleteApiCallHandler.deleteResourceDefinitionIfEmpty(rscName));

        assertThat(rscDfn.isDeleted()).isFalse();
        assertThat(rc).isEmpty();
    }

    @Test
    public void deleteIfEmptyKeepsRscDfnWithSnapshot() throws Exception
    {
        ResourceName rscName = new ResourceName("RscDfnWithSnap");
        ResourceDefinition rscDfn = seedRscDfn(rscName, false, true);

        ApiCallRc rc = execute(ctrlRscDfnDeleteApiCallHandler.deleteResourceDefinitionIfEmpty(rscName));

        assertThat(rscDfn.isDeleted()).isFalse();
        assertThat(rc).isEmpty();
    }

    @Test
    public void truncateWithDeleteIfNoSnapshotsRemovesEmptyRscDfn() throws Exception
    {
        ResourceName rscName = new ResourceName("TruncEmpty");
        ResourceDefinition rscDfn = seedRscDfn(rscName, false, false);

        ApiCallRc rc = execute(ctrlRscDfnDeleteApiCallHandler.truncateResourceDefinition(rscName.displayValue, true));

        assertThat(rscDfn.isDeleted()).isTrue();
        assertThat(hasCode(rc, ApiConsts.DELETED)).isTrue();
    }

    @Test
    public void truncateWithDeleteIfNoSnapshotsKeepsRscDfnWithSnapshot() throws Exception
    {
        ResourceName rscName = new ResourceName("TruncSnap");
        ResourceDefinition rscDfn = seedRscDfn(rscName, false, true);

        execute(ctrlRscDfnDeleteApiCallHandler.truncateResourceDefinition(rscName.displayValue, true));

        assertThat(rscDfn.isDeleted()).isFalse();
    }

    @Test
    public void truncateWithoutDeleteFlagKeepsEmptyRscDfn() throws Exception
    {
        ResourceName rscName = new ResourceName("TruncKeep");
        ResourceDefinition rscDfn = seedRscDfn(rscName, false, false);

        execute(ctrlRscDfnDeleteApiCallHandler.truncateResourceDefinition(rscName.displayValue, false));

        assertThat(rscDfn.isDeleted()).isFalse();
    }

    private ResourceDefinition seedRscDfn(ResourceName rscName, boolean withResource, boolean withSnapshot)
        throws Exception
    {
        enterScope();

        ResourceDefinition rscDfn = resourceDefinitionFactory.create(
            rscName,
            null,
            new ResourceDefinition.Flags[0],
            null,
            new LayerPayload(),
            createDefaultResourceGroup()
        );
        rscDfnMap.put(rscName, rscDfn);

        if (withResource)
        {
            resourceFactory.create(
                rscDfn,
                satelliteNode,
                null,
                new Resource.Flags[0],
                Collections.singletonList(DeviceLayerKind.STORAGE)
            );
        }
        if (withSnapshot)
        {
            snapshotDefinitionFactory.create(
                rscDfn,
                new SnapshotName("snap1"),
                new SnapshotDefinition.Flags[0]
            );
        }

        commitAndCleanUp(true);
        return rscDfn;
    }

    private ApiCallRc execute(Flux<ApiCallRc> flux)
    {
        ApiCallRcImpl rc = new ApiCallRcImpl();
        flux.contextWrite(contextWrite()).toStream().forEach(rc::addEntries);
        return rc;
    }

    private static boolean hasCode(ApiCallRc rc, long code)
    {
        return rc.stream().anyMatch(entry -> (entry.getReturnCode() & code) == code);
    }
}
