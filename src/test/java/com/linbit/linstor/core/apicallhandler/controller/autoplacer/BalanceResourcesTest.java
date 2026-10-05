package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.layer.drbd.drbdstate.DiskState;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.test.factories.VolumeDefinitionTestFactory;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.Pair;

import jakarta.inject.Inject;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BalanceResources}.
 *
 * The tests use a small DSL to describe the cluster:
 *
 * <pre>
 * cluster().replicaCount(2)
 *     .rsc("node1")                            // diskful, UpToDate, old enough, not in use
 *     .rsc("node2").diskState("Inconsistent")  // diskful with custom DRBD disk state
 *     .disklessRsc("node3");                   // DRBD_DISKLESS tiebreaker
 * expectBalance(-1);                           // + adjust, - delete, 0 noop
 * </pre>
 *
 * The flux-based deletion / auto-place handlers are mocked, so {@link BalanceResources#balanceResources(long)}
 * only decides what to do; the returned {@code Pair<adjustedRscDfnCount, deletedRscCount>} is asserted via
 * {@link #expectBalance(int)}.
 */
public class BalanceResourcesTest extends GenericDbBase
{
    private static final String DFLT_STOR_POOL = "DfltStorPool";
    private static final String DFLT_DISKLESS_STOR_POOL = "DfltDisklessStorPool";
    private static final String DFLT_RSC_GRP = InternalApiConsts.DEFAULT_RSC_GRP_NAME;
    private static final String RSC_NAME_STR = "testRsc";
    private static final long SIZE_100_MB = 100 * 1024;
    private static final long TIMEOUT_SECS = 10;
    private static final int VLM_NR = 0;

    /**
     * Default creation timestamp age. Has to be older than the default grace period (1h) so that the resources
     * are deletable by default.
     */
    private static final long OLD_ENOUGH_SECS = 24 * 3600;
    /**
     * Age of resources created via {@link RscBuilder#inGracePeriod()}. A few seconds in the past to avoid
     * issues with second-based rounding of the grace period calculation, but still well within the default
     * grace period.
     */
    private static final long IN_GRACE_PERIOD_SECS = 5;

    @Inject
    private AutoUnplacer autoUnplacer;
    @Inject
    private SystemConfRepository systemConfRepository;
    @Inject
    private LockGuardFactory lockGuardFactory;

    private BalanceResources balanceResources;

    /** One satellite state per node, shared by all resources of that node. */
    private final Map<NodeName, SatelliteState> satelliteStates = new HashMap<>();
    private VolumeNumber vlmNr;

    @Before
    public void setup() throws Exception
    {
        setUpAndEnterScope();
        satelliteStates.clear();
        vlmNr = new VolumeNumber(VLM_NR);

        ResourceStateEvent rscStateEventMock = Mockito.mock(ResourceStateEvent.class);

        EventWaiter eventWaiterMock = Mockito.mock(EventWaiter.class);
        Mockito.when(eventWaiterMock.waitForStream(Mockito.any(), Mockito.any()))
            .thenReturn(Flux.empty());

        ScopeRunner scopeRunnerMock = Mockito.mock(ScopeRunner.class);
        Mockito.when(scopeRunnerMock.fluxInTransactionalScope(Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(Flux.empty());

        CtrlRscDeleteApiCallHandler rscDeleteHandlerMock = Mockito.mock(CtrlRscDeleteApiCallHandler.class);

        CtrlRscAutoPlaceApiCallHandler autoPlaceHandlerMock = Mockito.mock(CtrlRscAutoPlaceApiCallHandler.class);
        Mockito.when(
            autoPlaceHandlerMock.autoPlace(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anyList()
            )
        )
            .thenReturn(Flux.empty());

        balanceResources = new BalanceResources(
            errorReporter,
            systemConfRepository,
            resourceDefinitionRepository,
            autoUnplacer,
            rscStateEventMock,
            eventWaiterMock,
            scopeRunnerMock,
            lockGuardFactory,
            rscDeleteHandlerMock,
            autoPlaceHandlerMock
        );
    }

    @Test
    public void noopWhenBalancedTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2");

        expectBalance(0);
    }

    @Test
    public void adjustWhenTooFewDiskfulTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1");

        expectBalance(1);
    }

    @Test
    public void deleteWhenTooManyDiskfulTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(-1);
    }

    @Test
    public void deleteMultipleExcessTest() throws Exception
    {
        cluster().replicaCount(1)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(-2);
    }

    @Test
    public void disklessNotCountedAsDiskfulTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .disklessRsc("node3");

        expectBalance(0);
    }

    @Test
    public void disklessNotCountedForAdjustTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .disklessRsc("node2");

        expectBalance(1);
    }

    @Test
    public void gracePeriodBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").inGracePeriod();

        expectBalance(0);
    }

    @Test
    public void missingCreateTimestampBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").withoutCreateTimestamp();

        expectBalance(0);
    }

    @Test
    public void reducedGracePeriodAllowsDeleteTest() throws Exception
    {
        cluster().replicaCount(2)
            .ctrlProp(ApiConsts.KEY_BALANCE_RESOURCES_GRACE_PERIOD, "1")
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").inGracePeriod(); // 5 secs old, no longer within the reduced grace period

        expectBalance(-1);
    }

    @Test
    public void disabledOnCtrlBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .ctrlProp(ApiConsts.KEY_BALANCE_RESOURCES_ENABLED, "false")
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(0);
    }

    @Test
    public void disabledOnRscGrpBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rscGrpProp(ApiConsts.KEY_BALANCE_RESOURCES_ENABLED, "false")
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(0);
    }

    @Test
    public void disabledOnRscDfnBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rscDfnProp(ApiConsts.KEY_BALANCE_RESOURCES_ENABLED, "false")
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(0);
    }

    @Test
    public void noUpToDateBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1").diskState(DiskState.INCONSISTENT.toString())
            .rsc("node2").diskState(DiskState.INCONSISTENT.toString())
            .rsc("node3").diskState(DiskState.INCONSISTENT.toString());

        expectBalance(0);
    }

    @Test
    public void syncBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").replStateTo("node1", ReplState.SYNC_TARGET);

        expectBalance(0);
    }

    @Test
    public void establishedReplStateAllowsDeleteTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1").replStateTo("node2", ReplState.ESTABLISHED)
            .rsc("node2").replStateTo("node1", ReplState.ESTABLISHED)
            .rsc("node3").replStateTo("node1", ReplState.ESTABLISHED);

        expectBalance(-1);
    }

    @Test
    public void rscDfnInDeleteBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rscDfnFlags(ResourceDefinition.Flags.DELETE)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(0);
    }

    @Test
    public void nonDrbdRscDfnIgnoredTest() throws Exception
    {
        cluster().replicaCount(2)
            .storageOnly()
            .rsc("node1")
            .rsc("node2")
            .rsc("node3");

        expectBalance(0);
    }

    @Test
    public void rscInDeleteCausesRscDfnToBeIgnoredTest() throws Exception
    {
        // too many diskfuls also get ignored if one of them is currently in a deleting state
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3")
            .rsc("node4")
            .rsc("node5")
            .rsc("node6").flags(Resource.Flags.DELETE);

        expectBalance(0);
    }

    /**
     * A resource that the model considers diskful (e.g. a failed / in-progress toggle-disk) but whose DRBD
     * disk state is not UpToDate must not inflate the diskful count and trigger a deletion of a healthy peer.
     */
    @Test
    public void failedToggleDiskBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").diskState(DiskState.DISKLESS.toString());

        expectBalance(0);
    }

    @Test
    public void skipDiskNotCountedAsDiskfulTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").skipDisk().diskState(DiskState.DISKLESS.toString());

        expectBalance(0);
    }

    @Test
    public void skipDiskCausesAdjustTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2").skipDisk().diskState(DiskState.DISKLESS.toString());

        expectBalance(1);
    }

    @Test
    public void skipDiskLimitBlocksRscDfnTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2").skipDisk().diskState(DiskState.DISKLESS.toString())
            .rsc("node3").skipDisk().diskState(DiskState.DISKLESS.toString());

        expectBalance(0);
    }

    @Test
    public void increasedSkipDiskLimitAllowsAdjustTest() throws Exception
    {
        cluster().replicaCount(2)
            .ctrlProp(ApiConsts.KEY_BALANCE_RESOURCES_SKIP_DISK_LIMIT, "2")
            .rsc("node1")
            .rsc("node2").skipDisk().diskState(DiskState.DISKLESS.toString())
            .rsc("node3").skipDisk().diskState(DiskState.DISKLESS.toString());

        expectBalance(1);
    }

    @Test
    public void inUseResourcesNotDeletedTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1").inUse()
            .rsc("node2").inUse()
            .rsc("node3").inUse();

        expectBalance(0);
    }

    @Test
    public void deleteSkipsInUseResourceTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1").inUse()
            .rsc("node2")
            .rsc("node3")
            .rsc("node4");

        expectBalance(-2);
    }

    @Test
    public void missingStltRscStateIsFixedButDeletableElsewhereTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1")
            .rsc("node2")
            .rsc("node3").withoutStltRscState();

        expectBalance(-1);
    }

    @Test
    public void multipleRscDfnsBalancedIndependentlyTest() throws Exception
    {
        cluster().replicaCount(2)
            .rsc("node1", "rscA")
            .rsc("node1", "rscB")
            .rsc("node2", "rscB")
            .rsc("node3", "rscB");

        expectBalance(1, 1);
    }

    private ClusterBuilder cluster()
    {
        return new ClusterBuilder();
    }

    /**
     * Runs {@link BalanceResources#balanceResources(long)} and asserts the result.
     *
     * @param expectedRef positive: number of resource definitions that need additional diskful resources,
     *     negative: number of resources to be deleted, 0: nothing to do
     */
    private void expectBalance(int expectedRef)
    {
        expectBalance(Math.max(expectedRef, 0), Math.max(-expectedRef, 0));
    }

    private void expectBalance(int expectedAdjustRef, int expectedDeleteRef)
    {
        Pair<Integer, Integer> result = balanceResources.balanceResources(TIMEOUT_SECS);
        assertEquals("unexpected number of rscDfns to adjust", expectedAdjustRef, (int) result.objA);
        assertEquals("unexpected number of resources to delete", expectedDeleteRef, (int) result.objB);
    }

    /**
     * Returns (and lazily creates) the {@link SatelliteState} of the given node. On first call a mocked
     * {@link Peer} returning that satellite state is attached to the node.
     */
    private SatelliteState satelliteState(Node node) throws Exception
    {
        NodeName nodeName = node.getName();
        SatelliteState state = satelliteStates.get(nodeName);
        if (state == null)
        {
            state = new SatelliteState();
            satelliteStates.put(nodeName, state);

            Peer peerMock = Mockito.mock(Peer.class);
            Mockito.when(peerMock.getSatelliteState()).thenReturn(state);
            Mockito.when(peerMock.getExtToolsManager()).thenReturn(new ExtToolsManager());
            Mockito.when(peerMock.getConnectionStatus()).thenReturn(ApiConsts.ConnectionStatus.ONLINE);
            Mockito.when(peerMock.isOnline()).thenReturn(true);
            Mockito.when(peerMock.getSatelliteStateLock()).thenReturn(new ReentrantReadWriteLock());
            node.setPeer(peerMock);
        }
        return state;
    }

    /**
     * Cluster-wide settings and resource creation. Resources are created eagerly with sane defaults
     * (see {@link RscBuilder}) which can be modified afterwards via the returned {@link RscBuilder}.
     */
    private class ClusterBuilder
    {
        private List<DeviceLayerKind> layerStack = Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);

        ClusterBuilder replicaCount(int replicaCountRef) throws Exception
        {
            resourceGroupTestFactory.get(DFLT_RSC_GRP, false)
                .getAutoPlaceConfig()
                .applyChanges(new AutoSelectFilterBuilder().setPlaceCount(replicaCountRef).build());
            return this;
        }

        ClusterBuilder ctrlProp(String key, String value) throws Exception
        {
            systemConfRepository.setCtrlProp(key, value, null);
            return this;
        }

        ClusterBuilder rscGrpProp(String key, String value) throws Exception
        {
            resourceGroupTestFactory.get(DFLT_RSC_GRP, false).getProps().setProp(key, value);
            return this;
        }

        ClusterBuilder rscDfnProp(String key, String value) throws Exception
        {
            resourceDefinitionTestFactory.get(RSC_NAME_STR, true).getProps().setProp(key, value);
            return this;
        }

        ClusterBuilder rscDfnFlags(ResourceDefinition.Flags... flagsRef) throws Exception
        {
            resourceDefinitionTestFactory.get(RSC_NAME_STR, true).getFlags().enableFlags(flagsRef);
            return this;
        }

        /**
         * The default rscDfn and all subsequently created resources will only use the STORAGE layer
         * (i.e. no DRBD).
         */
        ClusterBuilder storageOnly() throws Exception
        {
            layerStack = Arrays.asList(DeviceLayerKind.STORAGE);
            // also create the rscDfn without DRBD layer data, otherwise rscDfn.usesLayer(DRBD) is still true.
            // secret and transport type have to be nulled, otherwise the layer data factory still creates
            // DRBD rscDfn layer data
            resourceDefinitionTestFactory.builder(RSC_NAME_STR)
                .setLayerStack(layerStack)
                .setSecret(null)
                .setTransType(null)
                .build();
            return this;
        }

        RscBuilder rsc(String nodeNameRef) throws Exception
        {
            return rsc(nodeNameRef, RSC_NAME_STR);
        }

        RscBuilder rsc(String nodeNameRef, String rscNameRef) throws Exception
        {
            return new RscBuilder(this, createRsc(nodeNameRef, rscNameRef, false));
        }

        RscBuilder disklessRsc(String nodeNameRef) throws Exception
        {
            return disklessRsc(nodeNameRef, RSC_NAME_STR);
        }

        RscBuilder disklessRsc(String nodeNameRef, String rscNameRef) throws Exception
        {
            return new RscBuilder(this, createRsc(nodeNameRef, rscNameRef, true));
        }

        private Resource createRsc(String nodeNameRef, String rscNameRef, boolean disklessRef) throws Exception
        {
            Resource rsc = resourceTestFactory.builder(nodeNameRef, rscNameRef)
                .setLayerStack(layerStack)
                .setFlags(
                    disklessRef ?
                        new Resource.Flags[] {Resource.Flags.DRBD_DISKLESS} :
                        new Resource.Flags[0]
                )
                .build();

            // the test factories do not register the rscDfn in the repository, but BalanceResources
            // iterates over the repository's map
            ResourceDefinition rscDfn = rsc.getResourceDefinition();
            if (resourceDefinitionRepository.get(rscDfn.getName()) == null)
            {
                resourceDefinitionRepository.put(rscDfn);
            }

            if (volumeDefinitionTestFactory.get(rscNameRef, VLM_NR, SIZE_100_MB, false) == null)
            {
                VolumeDefinitionTestFactory.VolumeDefinitionBuilder vlmDfnBuilder = volumeDefinitionTestFactory
                    .builder(rscNameRef, VLM_NR)
                    .setSize(SIZE_100_MB);
                if (!layerStack.contains(DeviceLayerKind.DRBD))
                {
                    // a minor number would implicitly create DRBD layer data on the rscDfn
                    vlmDfnBuilder.setMinorNr(null);
                }
                vlmDfnBuilder.build();
            }
            if (disklessRef)
            {
                rsc.getProps().setProp(ApiConsts.KEY_STOR_POOL_NAME, DFLT_DISKLESS_STOR_POOL);
                @Nullable StorPool storPool = storPoolTestFactory.get(nodeNameRef, DFLT_DISKLESS_STOR_POOL, false);
                if (storPool == null)
                {
                    storPoolTestFactory.builder(nodeNameRef, DFLT_DISKLESS_STOR_POOL)
                        .setDriverKind(DeviceProviderKind.DISKLESS)
                        .build()
                        .getFreeSpaceTracker()
                        .setCapacityInfo(Long.MAX_VALUE, Long.MAX_VALUE);
                }
            }
            else
            {
                rsc.getProps().setProp(ApiConsts.KEY_STOR_POOL_NAME, DFLT_STOR_POOL);
                storPoolTestFactory.get(nodeNameRef, DFLT_STOR_POOL, true);
            }
            volumeTestFactory.get(nodeNameRef, rscNameRef, VLM_NR, true);

            // by default old enough so that the resource is not within the grace period
            rsc.setCreateTimestamp(Instant.now().minusSeconds(OLD_ENOUGH_SECS));

            satelliteState(rsc.getNode()).setOnVolume(
                rsc.getResourceDefinition().getName(),
                vlmNr,
                SatelliteVolumeState::setDiskState,
                disklessRef ? DiskState.DISKLESS.toString() : DiskState.UP_TO_DATE.toString()
            );
            return rsc;
        }
    }

    /**
     * Per-resource settings. Also delegates {@code rsc(...)} / {@code disklessRsc(...)} back to the
     * {@link ClusterBuilder} for fluent chaining.
     */
    private class RscBuilder
    {
        private final ClusterBuilder cluster;
        private final Resource rsc;

        private RscBuilder(ClusterBuilder clusterRef, Resource rscRef)
        {
            cluster = clusterRef;
            rsc = rscRef;
        }

        RscBuilder rsc(String nodeNameRef) throws Exception
        {
            return cluster.rsc(nodeNameRef);
        }

        RscBuilder rsc(String nodeNameRef, String rscNameRef) throws Exception
        {
            return cluster.rsc(nodeNameRef, rscNameRef);
        }

        RscBuilder disklessRsc(String nodeNameRef) throws Exception
        {
            return cluster.disklessRsc(nodeNameRef);
        }

        ClusterBuilder and()
        {
            return cluster;
        }

        /**
         * Sets the DRBD disk state reported by the satellite for this resource's volume.
         */
        RscBuilder diskState(String diskStateRef) throws Exception
        {
            satelliteState(rsc.getNode()).setOnVolume(
                rsc.getResourceDefinition().getName(),
                vlmNr,
                SatelliteVolumeState::setDiskState,
                diskStateRef
            );
            return this;
        }

        /**
         * Sets the DRBD replication state of this resource's volume towards the given peer node.
         */
        RscBuilder replStateTo(String peerNodeNameRef, ReplState replStateRef) throws Exception
        {
            satelliteState(rsc.getNode())
                .getResourceStates()
                .get(rsc.getResourceDefinition().getName())
                .getVolumeStates()
                .get(vlmNr)
                .getReplicationStateMap()
                .put(new NodeName(peerNodeNameRef), replStateRef);
            return this;
        }

        RscBuilder inUse() throws Exception
        {
            satelliteState(rsc.getNode()).setOnResource(
                rsc.getResourceDefinition().getName(),
                SatelliteResourceState::setInUse,
                Boolean.TRUE
            );
            return this;
        }

        RscBuilder inGracePeriod() throws Exception
        {
            rsc.setCreateTimestamp(Instant.now().minusSeconds(IN_GRACE_PERIOD_SECS));
            return this;
        }

        RscBuilder withoutCreateTimestamp() throws Exception
        {
            rsc.setCreateTimestamp(null);
            return this;
        }

        /**
         * Removes the satellite resource state, simulating a satellite that did not (yet) report any state
         * for this resource.
         */
        RscBuilder withoutStltRscState() throws Exception
        {
            satelliteState(rsc.getNode()).getResourceStates().remove(rsc.getResourceDefinition().getName());
            return this;
        }

        RscBuilder skipDisk() throws Exception
        {
            rsc.getProps().setProp(
                ApiConsts.KEY_DRBD_SKIP_DISK,
                ApiConsts.VAL_TRUE,
                ApiConsts.NAMESPC_DRBD_OPTIONS
            );
            return this;
        }

        RscBuilder flags(Resource.Flags... flagsRef) throws Exception
        {
            rsc.getStateFlags().enableFlags(flagsRef);
            return this;
        }
    }
}
