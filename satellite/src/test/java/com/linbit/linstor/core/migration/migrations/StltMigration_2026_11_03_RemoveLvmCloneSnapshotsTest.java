package com.linbit.linstor.core.migration.migrations;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.linstor.api.protobuf.FullSync.FullSyncStatus;
import com.linbit.linstor.core.migration.SatelliteMigrations;
import com.linbit.linstor.core.migration.StltMigrationHandler.StltMigrationResult;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.ExceptionThrowingFunction;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:typename")
public class StltMigration_2026_11_03_RemoveLvmCloneSnapshotsTest
{
    /** the LVM filter config LvmUtils.execWithRetry would build for the volume group */
    private static final String LVM_CONFIG = "devices { filter=['a|/dev/vdb|','r|.*|'] }";
    private static final int LVM_EXIT_FAILED = 5;

    private TestExtCmd extCmd;
    private ExtCmdFactory extCmdFactory;
    private MockedStatic<LvmUtils> lvmUtils;
    private StltMigration_2026_11_03_RemoveLvmCloneSnapshots migration;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp()
    {
        ErrorReporter errorReporter = mock(ErrorReporter.class);
        extCmd = new TestExtCmd(errorReporter);
        extCmdFactory = mock(ExtCmdFactory.class);
        when(extCmdFactory.create()).thenReturn(extCmd);
        lvmUtils = mockStatic(LvmUtils.class);
        lvmUtils.when(() -> LvmUtils.execWithRetry(any(), any(), any())).thenAnswer(
            inv -> ((ExceptionThrowingFunction<String, ?, ?>) inv.getArgument(2)).accept(LVM_CONFIG)
        );
        migration = new StltMigration_2026_11_03_RemoveLvmCloneSnapshots(errorReporter);
    }

    @After
    public void tearDown()
    {
        lvmUtils.close();
    }

    private static Node node(StorPool... storPools)
    {
        Node node = mock(Node.class);
        when(node.streamStorPools()).thenAnswer(ignored -> Stream.of(storPools));
        return node;
    }

    private static StorPool storPool(DeviceProviderKind kind, String storPoolName) throws Exception
    {
        StorPool storPool = mock(StorPool.class);
        when(storPool.getDeviceProviderKind()).thenReturn(kind);
        ReadOnlyProps props = mock(ReadOnlyProps.class);
        ReadOnlyProps storDriverProps = mock(ReadOnlyProps.class);
        when(storPool.getReadOnlyProps()).thenReturn(props);
        when(props.getNamespaceOrEmpty(DeviceLayer.STOR_DRIVER_NAMESPACE)).thenReturn(storDriverProps);
        when(storDriverProps.getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY)).thenReturn(storPoolName);
        return storPool;
    }

    private static String[] lvs(String volumeGroup)
    {
        return new String[]
        {
            "lvs", "--config", LVM_CONFIG, "-o", "lv_name", "-S", "lv_tags=linstor_clone_snapshot", "--noheadings",
            volumeGroup
        };
    }

    private static String[] lvremove(String volumeGroup, String lvName)
    {
        return new String[] {"lvremove", "--config", LVM_CONFIG, "-f", volumeGroup + "/" + lvName};
    }

    private void expect(String[] cmd, String stdOut, int exitCode)
    {
        extCmd.setExpectedBehavior(new Command(cmd), new TestOutputData(cmd, stdOut, "", exitCode));
    }

    private static void assertSucceeded(StltMigrationResult result)
    {
        assertThat(result.status).isEqualTo(FullSyncStatus.SUCCESS);
        // the migration is not run again
        assertThat(result.stltPropNamespacesToDelete)
            .containsExactly(SatelliteMigrations.MIG_REMOVE_LVM_CLONE_SNAPS.getPropNamespaceFull());
    }

    @Test
    public void removesTaggedLvsOfLvmStorPools() throws Exception
    {
        Node node = node(
            storPool(DeviceProviderKind.LVM, "scratch"),
            storPool(DeviceProviderKind.LVM_THIN, "thinvg/thin"),
            storPool(DeviceProviderKind.ZFS_THIN, "tank")
        );
        expect(lvs("scratch"), "  rsc_00000_CF_1a2b3c4d\n", 0);
        expect(lvs("thinvg"), "  other_00000_CF_5e6f7a8b\n  other_00001_CF_5e6f7a8b\n", 0);
        expect(lvremove("scratch", "rsc_00000_CF_1a2b3c4d"), "", 0);
        expect(lvremove("thinvg", "other_00000_CF_5e6f7a8b"), "", 0);
        expect(lvremove("thinvg", "other_00001_CF_5e6f7a8b"), "", 0);

        assertSucceeded(migration.migrate(extCmdFactory, node));
        assertThat(extCmd.getUncalledCommands()).isEmpty();
        // with the LVM filter of each volume group
        lvmUtils.verify(() -> LvmUtils.execWithRetry(any(), eq(Set.of("scratch")), any()), atLeastOnce());
        lvmUtils.verify(() -> LvmUtils.execWithRetry(any(), eq(Set.of("thinvg")), any()), atLeastOnce());
    }

    @Test
    public void nothingToRemove() throws Exception
    {
        expect(lvs("scratch"), "", 0);

        assertSucceeded(migration.migrate(extCmdFactory, node(storPool(DeviceProviderKind.LVM, "scratch"))));
        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    @Test
    public void noLvmStorPools() throws Exception
    {
        assertSucceeded(migration.migrate(extCmdFactory, node(storPool(DeviceProviderKind.ZFS, "tank"))));
        lvmUtils.verify(() -> LvmUtils.execWithRetry(any(), any(), any()), never());
    }

    @Test
    public void failuresDoNotFailTheMigration() throws Exception
    {
        Node node = node(
            storPool(DeviceProviderKind.LVM, "broken"),
            storPool(DeviceProviderKind.LVM, "scratch")
        );
        // a volume group that cannot be listed does not keep the LVs of the others from being removed
        expect(lvs("broken"), "", LVM_EXIT_FAILED);
        expect(lvs("scratch"), "  rsc_00000_CF_1a2b3c4d\n  rsc_00001_CF_1a2b3c4d\n", 0);
        expect(lvremove("scratch", "rsc_00000_CF_1a2b3c4d"), "", LVM_EXIT_FAILED);
        expect(lvremove("scratch", "rsc_00001_CF_1a2b3c4d"), "", 0);

        assertSucceeded(migration.migrate(extCmdFactory, node));
        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }
}
