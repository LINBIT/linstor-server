package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DrbdStateChange;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the parsing of 'drbdsetup events2 all' lines and the resulting
 * state updates of the {@link DrbdStateTracker}.
 */
public class DrbdEventsMonitorTest
{
    private static final String RSC = "rsc0";
    private static final String PEER = "node-b";

    private static final String[] INITIAL_STATE = {
        "exists resource name:" + RSC + " role:Secondary suspended:no may_promote:no promotion_score:10102",
        "exists connection name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " connection:Connected role:Secondary",
        "exists device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no quorum:yes",
        "exists peer-device name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " volume:0 " +
            "replication:Established peer-disk:UpToDate peer-client:no resync-suspended:no",
        "exists -"
    };

    private DrbdStateTracker tracker;
    private DrbdEventsMonitor monitor;

    @Before
    public void setUp()
    {
        tracker = new DrbdStateTracker();
        monitor = new DrbdEventsMonitor(tracker, new EmptyErrorReporter(), new TestRscDfnMap());
    }

    private void receive(String... eventLines) throws EventsSourceException
    {
        for (String line : eventLines)
        {
            monitor.receiveEvent(line);
        }
    }

    private DrbdResource rsc()
    {
        DrbdResource resource = tracker.getResource(RSC);
        assertThat(resource).isNotNull();
        return resource;
    }

    private DrbdConnection conn()
    {
        DrbdConnection connection = rsc().getConnection(PEER);
        assertThat(connection).isNotNull();
        return connection;
    }

    private DrbdVolume vlm() throws ValueOutOfRangeException
    {
        DrbdVolume volume = rsc().getVolume(new VolumeNumber(0));
        assertThat(volume).isNotNull();
        return volume;
    }

    private DrbdVolume peerVlm() throws ValueOutOfRangeException
    {
        DrbdVolume volume = conn().getVolume(new VolumeNumber(0));
        assertThat(volume).isNotNull();
        return volume;
    }

    @Test
    public void initialStateIsTracked() throws Exception
    {
        receive(INITIAL_STATE);

        assertThat(monitor.isStateAvailable()).isTrue();

        DrbdResource resource = rsc();
        assertThat(resource.getNameString()).isEqualTo(RSC);
        assertThat(resource.getResName()).isEqualTo(new ResourceName(RSC));
        assertThat(resource.getRole()).isEqualTo(DrbdResource.Role.SECONDARY);
        assertThat(resource.mayPromote()).isFalse();
        assertThat(resource.getPromotionScore()).isEqualTo(10102);
        assertThat(resource.getSuspendedUser()).isFalse();

        DrbdConnection connection = conn();
        assertThat(connection.getState()).isEqualTo(DrbdConnection.State.CONNECTED);
        assertThat(connection.getPeerRole()).isEqualTo(DrbdResource.Role.SECONDARY);
        assertThat(connection.getPeerNodeId()).isEqualTo(1);
        assertThat(connection.getResource()).isSameAs(resource);

        DrbdVolume volume = vlm();
        assertThat(volume.getVolNr()).isEqualTo(new VolumeNumber(0));
        assertThat(volume.getMinorNr()).isEqualTo(new MinorNumber(1000));
        assertThat(volume.getDiskState()).isEqualTo(DiskState.UP_TO_DATE);
        assertThat(volume.isClient()).isFalse();
        assertThat(volume.getConnection()).isNull();

        DrbdVolume peerVolume = peerVlm();
        assertThat(peerVolume.getReplState()).isEqualTo(ReplState.ESTABLISHED);
        assertThat(peerVolume.getDiskState()).isEqualTo(DiskState.UP_TO_DATE);
        assertThat(peerVolume.getConnection()).isSameAs(connection);
    }

    @Test
    public void stateNotAvailableBeforeEndOfInitialEvents() throws Exception
    {
        receive(INITIAL_STATE[0], INITIAL_STATE[1], INITIAL_STATE[2], INITIAL_STATE[3]);

        assertThat(monitor.isStateAvailable()).isFalse();
    }

    @Test
    public void nonExistsEventsAreQueuedUntilEndOfInitialEvents() throws Exception
    {
        receive(INITIAL_STATE[0]);
        // arrives while the initial 'exists' dump is still in progress -> must be queued
        receive("change resource name:" + RSC + " role:Primary");

        assertThat(rsc().getRole()).isEqualTo(DrbdResource.Role.SECONDARY);

        receive("exists -");

        assertThat(monitor.isStateAvailable()).isTrue();
        assertThat(rsc().getRole()).isEqualTo(DrbdResource.Role.PRIMARY);
    }

    @Test
    public void drbdStateChangeObserversAreNotified() throws Exception
    {
        List<String> stateChanges = new ArrayList<>();
        tracker.addDrbdStateChangeObserver(new DrbdStateChange()
        {
            @Override
            public void drbdStateAvailable()
            {
                stateChanges.add("available");
            }

            @Override
            public void drbdStateUnavailable()
            {
                stateChanges.add("unavailable");
            }
        });

        receive(INITIAL_STATE);
        assertThat(stateChanges).containsExactly("available");

        monitor.reinitializing();
        assertThat(stateChanges).containsExactly("available", "unavailable");
        assertThat(monitor.isStateAvailable()).isFalse();
    }

    @Test
    public void resourceRoleChange() throws Exception
    {
        receive(INITIAL_STATE);

        List<String> roleChanges = new ArrayList<>();
        tracker.addObserver(
            new ResourceObserver()
            {
                @Override
                public void roleChanged(DrbdResource resource, DrbdResource.Role previous, DrbdResource.Role current)
                {
                    roleChanges.add(previous + "->" + current);
                }
            },
            DrbdStateTracker.OBS_ROLE
        );

        receive("change resource name:" + RSC + " role:Primary may_promote:no promotion_score:10103");

        assertThat(rsc().getRole()).isEqualTo(DrbdResource.Role.PRIMARY);
        assertThat(rsc().getPromotionScore()).isEqualTo(10103);
        assertThat(roleChanges).containsExactly("Secondary->Primary");

        // same role again -> no additional notification
        receive("change resource name:" + RSC + " role:Primary");
        assertThat(roleChanges).containsExactly("Secondary->Primary");
    }

    @Test
    public void resourceSuspendedUser() throws Exception
    {
        receive(INITIAL_STATE);

        receive("change resource name:" + RSC + " suspended:user");
        assertThat(rsc().getSuspendedUser()).isTrue();

        receive("change resource name:" + RSC + " suspended:no");
        assertThat(rsc().getSuspendedUser()).isFalse();
    }

    @Test
    public void resourceMayPromoteChange() throws Exception
    {
        receive(INITIAL_STATE);

        receive("change resource name:" + RSC + " may_promote:yes");
        assertThat(rsc().mayPromote()).isTrue();

        // any other value than yes/no resets to null
        receive("change resource name:" + RSC + " may_promote:maybe");
        assertThat(rsc().mayPromote()).isNull();
    }

    @Test
    public void connectionStateChange() throws Exception
    {
        receive(INITIAL_STATE);

        List<String> connStateChanges = new ArrayList<>();
        tracker.addObserver(
            new ResourceObserver()
            {
                @Override
                public void connectionStateChanged(
                    DrbdResource resource,
                    DrbdConnection connection,
                    DrbdConnection.State previous,
                    DrbdConnection.State current
                )
                {
                    connStateChanges.add(previous + "->" + current);
                }
            },
            DrbdStateTracker.OBS_CONN
        );

        receive("change connection name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " connection:StandAlone");

        assertThat(conn().getState()).isEqualTo(DrbdConnection.State.STANDALONE);
        assertThat(connStateChanges).containsExactly("Connected->StandAlone");
    }

    @Test
    public void peerRoleChange() throws Exception
    {
        receive(INITIAL_STATE);

        receive("change connection name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " role:Primary");

        assertThat(conn().getPeerRole()).isEqualTo(DrbdResource.Role.PRIMARY);
    }

    @Test
    public void diskStateChange() throws Exception
    {
        receive(INITIAL_STATE);

        List<String> diskStateChanges = new ArrayList<>();
        tracker.addObserver(
            new ResourceObserver()
            {
                @Override
                public void diskStateChanged(
                    DrbdResource resource,
                    DrbdConnection connection,
                    DrbdVolume volume,
                    DiskState previous,
                    DiskState current
                )
                {
                    diskStateChanges.add(previous + "->" + current);
                }
            },
            DrbdStateTracker.OBS_DISK
        );

        receive("change device name:" + RSC + " volume:0 minor:1000 disk:Inconsistent");

        assertThat(vlm().getDiskState()).isEqualTo(DiskState.INCONSISTENT);
        assertThat(diskStateChanges).containsExactly("UpToDate->Inconsistent");
    }

    @Test
    public void minorNumberChange() throws Exception
    {
        receive(INITIAL_STATE);

        receive("change device name:" + RSC + " volume:0 minor:1001 disk:UpToDate");

        assertThat(vlm().getMinorNr()).isEqualTo(new MinorNumber(1001));
    }

    @Test
    public void openStateChange() throws Exception
    {
        receive(INITIAL_STATE);

        // INITIAL_STATE has no "open" field, like the output of drbd-utils without it
        assertThat(vlm().getOpen()).isNull();

        List<String> openChanges = new ArrayList<>();
        tracker.addObserver(
            new ResourceObserver()
            {
                @Override
                public void openChanged(
                    DrbdResource resource,
                    DrbdVolume volume,
                    @Nullable Boolean previous,
                    @Nullable Boolean current
                )
                {
                    openChanges.add(previous + "->" + current);
                }
            },
            DrbdStateTracker.OBS_OPEN
        );

        receive(
            "change device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no open:yes quorum:yes",
            "change device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no open:yes quorum:yes"
        );
        assertThat(vlm().getOpen()).isTrue();

        receive("change device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no open:no quorum:yes");
        assertThat(vlm().getOpen()).isFalse();

        receive("change device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no open:unknown quorum:yes");
        assertThat(vlm().getOpen()).isNull();

        assertThat(openChanges).containsExactly("null->true", "true->false", "false->null");
        assertThat(peerVlm().getOpen()).isNull();
    }

    @Test
    public void resourceIsOpenIfAnyVolumeIsOpen() throws Exception
    {
        receive(INITIAL_STATE);
        receive("create device name:" + RSC + " volume:1 minor:1001 disk:UpToDate client:no open:no quorum:yes");

        // volume 0 does not report "open": unknown as long as no volume is open
        assertThat(DrbdEventPublisher.isAnyVolumeOpen(rsc().getVolumesMap())).isNull();

        receive("change device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no open:no quorum:yes");
        assertThat(DrbdEventPublisher.isAnyVolumeOpen(rsc().getVolumesMap())).isFalse();

        receive("change device name:" + RSC + " volume:1 minor:1001 disk:UpToDate client:no open:yes quorum:yes");
        assertThat(DrbdEventPublisher.isAnyVolumeOpen(rsc().getVolumesMap())).isTrue();

        receive("change device name:" + RSC + " volume:0 minor:1000 disk:UpToDate client:no open:unknown quorum:yes");
        assertThat(DrbdEventPublisher.isAnyVolumeOpen(rsc().getVolumesMap())).isTrue();

        assertThat(DrbdEventPublisher.isAnyVolumeOpen(new TreeMap<>())).isNull();
    }

    @Test
    public void peerVolumeReplicationAndSyncProgress() throws Exception
    {
        receive(INITIAL_STATE);

        receive(
            "change peer-device name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " volume:0 " +
                "replication:SyncTarget peer-disk:Inconsistent done:12.34"
        );

        DrbdVolume peerVolume = peerVlm();
        assertThat(peerVolume.getReplState()).isEqualTo(ReplState.SYNC_TARGET);
        assertThat(peerVolume.getDiskState()).isEqualTo(DiskState.INCONSISTENT);
        assertThat(peerVolume.diskStateInfo()).startsWith("SyncTarget(");

        receive(
            "change peer-device name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " volume:0 " +
                "replication:Established peer-disk:UpToDate"
        );
        assertThat(peerVolume.getReplState()).isEqualTo(ReplState.ESTABLISHED);
        assertThat(peerVolume.getDiskState()).isEqualTo(DiskState.UP_TO_DATE);
        assertThat(peerVolume.diskStateInfo()).isEqualTo(DiskState.UP_TO_DATE.toString());
    }

    @Test
    public void destroyPeerVolume() throws Exception
    {
        receive(INITIAL_STATE);

        receive("destroy peer-device name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " volume:0");

        assertThat(conn().getVolume(new VolumeNumber(0))).isNull();
    }

    @Test
    public void destroyVolume() throws Exception
    {
        receive(INITIAL_STATE);

        receive("destroy device name:" + RSC + " volume:0");

        assertThat(rsc().getVolume(new VolumeNumber(0))).isNull();
    }

    @Test
    public void destroyConnection() throws Exception
    {
        receive(INITIAL_STATE);

        receive("destroy connection name:" + RSC + " peer-node-id:1 conn-name:" + PEER);

        assertThat(rsc().getConnection(PEER)).isNull();
    }

    @Test
    public void destroyResource() throws Exception
    {
        receive(INITIAL_STATE);

        receive("destroy resource name:" + RSC);

        assertThat(tracker.getResource(RSC)).isNull();
    }

    @Test
    public void createSecondResource() throws Exception
    {
        receive(INITIAL_STATE);

        receive("create resource name:rsc1 role:Secondary suspended:no");

        assertThat(tracker.getAllResources()).hasSize(2);
        assertThat(tracker.getResource("rsc1")).isNotNull();
    }

    @Test
    public void helperActionsAreIgnored() throws Exception
    {
        receive(INITIAL_STATE);

        // helper script calls emit lines with other action types, which are not tracked
        receive("call helper name:" + RSC + " helper:before-resync-target");
        receive("response helper name:" + RSC + " helper:before-resync-target status:0");

        assertThat(tracker.getAllResources()).hasSize(1);
    }

    @Test
    public void unknownObjectTypesAreIgnored() throws Exception
    {
        receive(INITIAL_STATE);

        // connection paths are reported by events2 but not tracked
        receive("change path name:" + RSC + " peer-node-id:1 conn-name:" + PEER + " established:yes");

        assertThat(tracker.getAllResources()).hasSize(1);
    }

    @Test
    public void emptyLinesAreIgnored() throws Exception
    {
        receive("");

        assertThat(tracker.getAllResources()).isEmpty();
    }

    @Test
    public void nullEventLineThrows()
    {
        assertThatThrownBy(() -> monitor.receiveEvent(null))
            .isInstanceOf(ImplementationError.class);
    }

    @Test
    public void eventLineWithoutObjectTypeThrows()
    {
        assertThatThrownBy(() -> receive("exists"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("object type");
    }

    @Test
    public void createResourceWithoutNameThrows()
    {
        assertThatThrownBy(() -> receive("exists resource role:Secondary"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("resource name");
    }

    @Test
    public void changeOfUnknownResourceThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("change resource name:ghost role:Primary"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("non-existent resource");
    }

    @Test
    public void destroyOfUnknownVolumeThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("destroy device name:" + RSC + " volume:7"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("non-existent volume");
    }

    @Test
    public void createVolumeWithoutVolumeNumberThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("create device name:" + RSC + " minor:1002 disk:UpToDate"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("volume number");
    }

    @Test
    public void unparsableVolumeNumberThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("change device name:" + RSC + " volume:abc disk:UpToDate"))
            .isInstanceOf(EventsSourceException.class);
    }

    @Test
    public void invalidVolumeNumberThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("change device name:" + RSC + " volume:-1 disk:UpToDate"))
            .isInstanceOf(EventsSourceException.class);
    }

    @Test
    public void createConnectionWithoutPeerNodeIdThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("create connection name:" + RSC + " conn-name:node-c"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("peer node id");
    }

    @Test
    public void unparsableMinorNumberThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("change device name:" + RSC + " volume:0 minor:abc"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("minor number");
    }

    @Test
    public void unparsablePromotionScoreThrows() throws Exception
    {
        receive(INITIAL_STATE);

        assertThatThrownBy(() -> receive("change resource name:" + RSC + " promotion_score:abc"))
            .isInstanceOf(EventsSourceException.class)
            .hasMessageContaining("promotion_score");
    }

    private static class TestRscDfnMap extends TreeMap<ResourceName, ResourceDefinition>
        implements CoreModule.ResourceDefinitionMap
    {
        private static final long serialVersionUID = 1L;
    }
}
