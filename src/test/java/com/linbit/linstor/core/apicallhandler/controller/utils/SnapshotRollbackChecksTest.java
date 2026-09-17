package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SnapshotRollbackChecks} decides between "zfs rollback" style rollback (old, {@code true}) and "rollback via
 * restore with safety snapshot" (new, {@code false}). Everything ZFS specific lives in {@link ZfsChecks}; this test
 * only covers the EBS rule and the delegation, so {@code ZfsChecks} is mocked.
 */
public class SnapshotRollbackChecksTest
{
    private static final String NO_CLIENT_STRATEGY = null;

    private ZfsChecks zfsChecks;
    private SnapshotRollbackChecks checks;
    private SnapshotDefinition snapDfn;

    @Before
    public void setUp()
    {
        zfsChecks = Mockito.mock(ZfsChecks.class);
        checks = new SnapshotRollbackChecks(zfsChecks);
        snapDfn = Mockito.mock(SnapshotDefinition.class);
    }

    @Test
    public void ebsSnapshotForcesOldRollback()
    {
        // an EBS snapshot only becomes usable once AWS reports "completed", which can take very long; the safety
        // snapshot of "rollback via restore" would have to wait for that, so EBS must use the old rollback
        // build the mocks before stubbing: creating a mock inside a thenReturn(...) argument is nested stubbing
        List<Snapshot> snapshots = Arrays.asList(snapshotOn(Node.Type.EBS_TARGET));
        Mockito.when(zfsChecks.isZfsSnapshot(snapDfn)).thenReturn(false);
        Mockito.when(snapDfn.getAllSnapshots()).thenReturn(snapshots);

        assertThat(checks.useOldRollback(snapDfn, NO_CLIENT_STRATEGY)).isTrue();
        Mockito.verify(zfsChecks, Mockito.never()).useOldRollback(Mockito.any(), Mockito.any());
    }

    @Test
    public void partiallyEbsSnapshotForcesOldRollback()
    {
        List<Snapshot> snapshots = Arrays.asList(snapshotOn(Node.Type.SATELLITE), snapshotOn(Node.Type.EBS_TARGET));
        Mockito.when(zfsChecks.isZfsSnapshot(snapDfn)).thenReturn(false);
        Mockito.when(snapDfn.getAllSnapshots()).thenReturn(snapshots);

        assertThat(checks.useOldRollback(snapDfn, NO_CLIENT_STRATEGY)).isTrue();
    }

    @Test
    public void nonZfsNonEbsSnapshotUsesRollbackViaRestore()
    {
        List<Snapshot> snapshots = Arrays.asList(snapshotOn(Node.Type.SATELLITE));
        Mockito.when(zfsChecks.isZfsSnapshot(snapDfn)).thenReturn(false);
        Mockito.when(snapDfn.getAllSnapshots()).thenReturn(snapshots);

        assertThat(checks.useOldRollback(snapDfn, NO_CLIENT_STRATEGY)).isFalse();
        Mockito.verify(zfsChecks, Mockito.never()).useOldRollback(Mockito.any(), Mockito.any());
    }

    @Test
    public void snapshotDefinitionWithoutSnapshotsUsesRollbackViaRestore()
    {
        Mockito.when(zfsChecks.isZfsSnapshot(snapDfn)).thenReturn(false);
        Mockito.when(snapDfn.getAllSnapshots()).thenReturn(Collections.emptyList());

        assertThat(checks.useOldRollback(snapDfn, NO_CLIENT_STRATEGY)).isFalse();
    }

    @Test
    public void zfsSnapshotDelegatesToZfsChecks()
    {
        Mockito.when(zfsChecks.isZfsSnapshot(snapDfn)).thenReturn(true);
        Mockito.when(zfsChecks.useOldRollback(snapDfn, "rollback")).thenReturn(true);
        Mockito.when(zfsChecks.useOldRollback(snapDfn, "clone")).thenReturn(false);

        assertThat(checks.useOldRollback(snapDfn, "rollback")).isTrue();
        assertThat(checks.useOldRollback(snapDfn, "clone")).isFalse();
        // the EBS rule is only consulted for non-ZFS snapshots
        Mockito.verify(snapDfn, Mockito.never()).getAllSnapshots();
    }

    private static Snapshot snapshotOn(Node.Type nodeType)
    {
        Node node = Mockito.mock(Node.class);
        Mockito.when(node.getNodeType()).thenReturn(nodeType);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.getNode()).thenReturn(node);
        return snapshot;
    }
}
