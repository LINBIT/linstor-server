package com.linbit.linstor.layer.storage.zfs.utils;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands.ZfsVolumeType;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

/**
 * Tests the parsing of "zfs list" / "zfs get" / "zpool get" output done by {@link ZfsUtils} by feeding
 * canned command output through a {@link TestExtCmd}.
 */
public class ZfsUtilsExtCmdTest
{
    private static final String[] ZFS_LIST_BASE_CMD =
    {
        "zfs", "list", "-r", "-H", "-p",
        "-o", "name,refer,volsize,type,volblocksize,origin,clones",
        "-t", "volume,snapshot"
    };

    private static final String[] ZFS_LIST_FILESYSTEMS_BASE_CMD =
    {
        "zfs", "list", "-d", "0", "-H", "-p",
        "-o", "name,available,type",
        "-t", "filesystem"
    };

    private TestExtCmd extCmd;
    private ExtCmdFactory extCmdFactory;

    @Before
    public void setUp()
    {
        extCmd = new TestExtCmd(new EmptyErrorReporter());
        extCmdFactory = Mockito.mock(ExtCmdFactory.class);
        Mockito.when(extCmdFactory.create()).thenReturn(extCmd);
    }

    @After
    public void tearDown()
    {
        StringBuilder sb = new StringBuilder();
        HashSet<Command> uncalledCommands = extCmd.getUncalledCommands();
        if (!uncalledCommands.isEmpty())
        {
            for (Command cmd : uncalledCommands)
            {
                sb.append(cmd).append("\n");
            }
            sb.setLength(sb.length() - 1);
            fail("Not all expected commands were called: \n" + sb.toString());
        }
    }

    @Test
    public void zfsListParsesVolumesSnapshotsAndClones() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool"),
            "rpool/rsc0_00000\t57344\t2147483648\tvolume\t16384\t-\t-\n" +
                "rpool/rsc0_00000@snap1\t20480\t2147483648\tsnapshot\t-\t-\trpool/clone0\n" +
                "rpool/clone0\t12288\t2147483648\tvolume\t16384\trpool/rsc0_00000@snap1\t-\n"
        );

        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            Collections.singletonList("rpool"),
            DeviceProviderKind.ZFS_THIN,
            Collections.emptyMap()
        );

        assertThat(zfsMap).containsOnlyKeys("rpool/rsc0_00000", "rpool/rsc0_00000@snap1", "rpool/clone0");

        ZfsUtils.ZfsInfo vlm = zfsMap.get("rpool/rsc0_00000");
        assertThat(vlm.poolName).isEqualTo("rpool");
        assertThat(vlm.identifier).isEqualTo("rsc0_00000");
        assertThat(vlm.typeStr).isEqualTo("volume");
        assertThat(vlm.type).isEqualTo(ZfsVolumeType.VOLUME);
        assertThat(vlm.path).isEqualTo("/dev/zvol/rpool/rsc0_00000");
        // thin volumes report the "refer" column as allocated size, converted from bytes to KiB
        assertThat(vlm.allocatedSize).isEqualTo(56L);
        assertThat(vlm.usableSize).isEqualTo(2097152L);
        assertThat(vlm.volBlockSize).isEqualTo(16L);
        assertThat(vlm.originStr).isNull();
        assertThat(vlm.clones).isEmpty();

        ZfsUtils.ZfsInfo snap = zfsMap.get("rpool/rsc0_00000@snap1");
        assertThat(snap.identifier).isEqualTo("rsc0_00000@snap1");
        assertThat(snap.type).isEqualTo(ZfsVolumeType.SNAPSHOT);
        assertThat(snap.allocatedSize).isEqualTo(20L);
        assertThat(snap.usableSize).isEqualTo(2097152L);
        // snapshots do not report a volblocksize
        assertThat(snap.volBlockSize).isNull();
        // listed in the "clones" column and referenced by the clone's origin, deduplicated
        assertThat(snap.clones).containsExactly("rpool/clone0");

        // snapshots are registered at their base volume
        assertThat(vlm.snapshots).containsExactly(snap);

        ZfsUtils.ZfsInfo clone = zfsMap.get("rpool/clone0");
        assertThat(clone.originStr).isEqualTo("rpool/rsc0_00000@snap1");
        assertThat(clone.allocatedSize).isEqualTo(12L);
    }

    @Test
    public void zfsListThickKindUsesVolsizeAsAllocatedSize() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "tank"),
            "tank/vol0\t57344\t1073741824\tvolume\t8192\t-\t-\n" +
                "tank/vol0@s1\t4096\t1073741824\tsnapshot\t-\t-\t-\n"
        );

        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            Collections.singletonList("tank"),
            DeviceProviderKind.ZFS,
            Collections.emptyMap()
        );

        // thick volumes report the "volsize" column as allocated size
        ZfsUtils.ZfsInfo vlm = zfsMap.get("tank/vol0");
        assertThat(vlm.allocatedSize).isEqualTo(1048576L);
        assertThat(vlm.usableSize).isEqualTo(1048576L);
        assertThat(vlm.volBlockSize).isEqualTo(8L);

        // snapshots still report the "refer" column
        ZfsUtils.ZfsInfo snap = zfsMap.get("tank/vol0@s1");
        assertThat(snap.allocatedSize).isEqualTo(4L);
    }

    @Test
    public void zfsListSupportsOldZfsSnapshotLinesAndSkipsInvalidLines() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool"),
            // clone listed before its origin snapshot
            "rpool/clone1\t12288\t1073741824\tvolume\t8192\trpool/base@s1\t-\n" +
                "rpool/base\t57344\t1073741824\tvolume\t8192\t-\t-\n" +
                // zfs < 2.0.5 omits the "clones" column for snapshots without clones
                "rpool/base@s1\t20480\t1073741824\tsnapshot\t-\t-\n" +
                "rpool/badvol\tnot-a-number\t1073741824\tvolume\t8192\t-\t-\n" +
                // volume line with a missing column is not valid
                "rpool/shortvol\t12288\t1073741824\tvolume\t8192\t-\n" +
                "\n"
        );

        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            Collections.singletonList("rpool"),
            DeviceProviderKind.ZFS_THIN,
            Collections.emptyMap()
        );

        assertThat(zfsMap).containsOnlyKeys("rpool/clone1", "rpool/base", "rpool/base@s1");

        ZfsUtils.ZfsInfo snap = zfsMap.get("rpool/base@s1");
        assertThat(snap.type).isEqualTo(ZfsVolumeType.SNAPSHOT);
        assertThat(snap.allocatedSize).isEqualTo(20L);
        // the clone was seen before its origin snapshot and has to be linked afterwards
        assertThat(snap.clones).containsExactly("rpool/clone1");

        assertThat(zfsMap.get("rpool/base").snapshots).containsExactly(snap);
        assertThat(zfsMap.get("rpool/clone1").originStr).isEqualTo("rpool/base@s1");
    }

    @Test
    public void zfsListEmptyOutputWithExitCodeOneResultsInEmptyMap() throws StorageException
    {
        // "zfs list" exits with 1 if a given dataset does not exist, which is an allowed exit code
        String[] cmd = StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool");
        extCmd.setExpectedBehavior(
            new Command(cmd),
            new TestOutputData(cmd, "", "cannot open 'rpool': dataset does not exist\n", 1)
        );

        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            Collections.singletonList("rpool"),
            DeviceProviderKind.ZFS_THIN,
            Collections.emptyMap()
        );

        assertThat(zfsMap).isEmpty();
    }

    @Test
    public void zfsListAppliesLocalUserProperties() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool"),
            "rpool/vol0\t57344\t1073741824\tvolume\t8192\t-\t-\n"
        );
        expect(
            new String[]
            {
                "zfs", "get", "-r", "-o", "name,value", "-H", "-p", "-s", "local", "linstor:snapshot-of",
                "rpool/vol0"
            },
            "rpool/vol0\tsome-snap\n" +
                // properties of unknown datasets have to be ignored
                "rpool/unknown\tother-value\n"
        );

        Map<String, String> appliedProps = new HashMap<>();
        Map<String, BiConsumer<ZfsUtils.ZfsInfo, String>> settersByProp = Collections.singletonMap(
            "snapshot-of",
            (zfsInfo, value) -> appliedProps.put(zfsInfo.identifier, value)
        );

        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            Collections.singletonList("rpool"),
            DeviceProviderKind.ZFS_THIN,
            settersByProp
        );

        assertThat(zfsMap).containsOnlyKeys("rpool/vol0");
        assertThat(appliedProps).containsOnlyKeys("vol0");
        assertThat(appliedProps.get("vol0")).isEqualTo("some-snap");
    }

    @Test
    public void zfsListWithoutDatasetsRunsNoCommand() throws StorageException
    {
        // without a dataset argument "zfs list -r" / "zfs get -r" would walk every dataset of every pool on the host.
        // No command is expected here, so any call fails the test.
        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            Collections.emptyList(),
            DeviceProviderKind.ZFS_THIN,
            Collections.singletonMap("marked_for_deletion", (zfsInfo, value) -> fail("no property expected"))
        );

        assertThat(zfsMap).isEmpty();
    }

    @Test
    public void zfsListQueriesPropertiesOnlyForListedDatasets() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool/vol0", "rpool/vol1@s1"),
            "rpool/vol0\t57344\t1073741824\tvolume\t8192\t-\t-\n" +
                "rpool/vol0@CF_1\t4096\t1073741824\tsnapshot\t-\t-\trpool/clone0\n" +
                "rpool/vol1@s1\t4096\t1073741824\tsnapshot\t-\t-\t-\n"
        );
        // recursive on the listed volume (covers its snapshots) and on the snapshot whose volume was not listed;
        // never on the whole zpool
        expect(
            new String[]
            {
                "zfs", "get", "-r", "-o", "name,value", "-H", "-p", "-s", "local", "linstor:marked_for_deletion",
                "rpool/vol0", "rpool/vol1@s1"
            },
            "rpool/vol0@CF_1\ttrue\n"
        );

        Map<String, String> appliedProps = new HashMap<>();
        HashMap<String, ZfsUtils.ZfsInfo> zfsMap = ZfsUtils.getZfsList(
            extCmdFactory,
            new LinkedHashSet<>(Arrays.asList("rpool/vol0", "rpool/vol1@s1")),
            DeviceProviderKind.ZFS_THIN,
            Collections.singletonMap(
                "marked_for_deletion",
                (zfsInfo, value) -> appliedProps.put(zfsInfo.identifier, value)
            )
        );

        assertThat(zfsMap).containsOnlyKeys("rpool/vol0", "rpool/vol0@CF_1", "rpool/vol1@s1");
        assertThat(appliedProps).containsExactly(Map.entry("vol0@CF_1", "true"));
    }

    @Test
    public void zfsListToleratesDatasetsDestroyedBeforeThePropertyQuery() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool/vol0", "rpool/vol1"),
            "rpool/vol0\t57344\t1073741824\tvolume\t8192\t-\t-\n" +
                "rpool/vol1\t57344\t1073741824\tvolume\t8192\t-\t-\n"
        );
        String[] getCmd = {
            "zfs", "get", "-r", "-o", "name,value", "-H", "-p", "-s", "local", "linstor:marked_for_deletion",
            "rpool/vol0", "rpool/vol1"
        };
        // vol1 was destroyed between the list and the get
        extCmd.setExpectedBehavior(
            new Command(getCmd),
            new TestOutputData(getCmd, "rpool/vol0\ttrue\n", "cannot open 'rpool/vol1': dataset does not exist\n", 1)
        );

        Map<String, String> appliedProps = new HashMap<>();
        ZfsUtils.getZfsList(
            extCmdFactory,
            new LinkedHashSet<>(Arrays.asList("rpool/vol0", "rpool/vol1")),
            DeviceProviderKind.ZFS_THIN,
            Collections.singletonMap(
                "marked_for_deletion",
                (zfsInfo, value) -> appliedProps.put(zfsInfo.identifier, value)
            )
        );

        assertThat(appliedProps).containsExactly(Map.entry("vol0", "true"));
    }

    @Test
    public void zfsListFailsOnOtherPropertyQueryErrors()
    {
        expect(
            StringUtils.concat(ZFS_LIST_BASE_CMD, "rpool/vol0"),
            "rpool/vol0\t57344\t1073741824\tvolume\t8192\t-\t-\n"
        );
        String[] getCmd = {
            "zfs", "get", "-r", "-o", "name,value", "-H", "-p", "-s", "local", "linstor:marked_for_deletion",
            "rpool/vol0"
        };
        // must not be read as "no dataset is marked for deletion"
        extCmd.setExpectedBehavior(
            new Command(getCmd),
            new TestOutputData(getCmd, "", "cannot open 'rpool/vol0': pool I/O is currently suspended\n", 1)
        );

        assertThatThrownBy(
            () -> ZfsUtils.getZfsList(
                extCmdFactory,
                Collections.singletonList("rpool/vol0"),
                DeviceProviderKind.ZFS_THIN,
                Collections.singletonMap("marked_for_deletion", (zfsInfo, value) -> { })
            )
        ).isInstanceOf(StorageException.class);
    }

    @Test
    public void zfsListSplitsDatasetsIntoBatchesWithoutAnEmptyOne() throws StorageException
    {
        List<String> datasets = new ArrayList<>();
        for (int idx = 0; idx < Commands.ARGUMENT_LIMIT; idx++)
        {
            datasets.add(String.format("rpool/vol%04d", idx));
        }
        // exactly ARGUMENT_LIMIT datasets: one call, no second call without dataset arguments
        expect(StringUtils.concat(ZFS_LIST_BASE_CMD, datasets), "");

        assertThat(
            ZfsUtils.getZfsList(extCmdFactory, datasets, DeviceProviderKind.ZFS_THIN, Collections.emptyMap())
        ).isEmpty();
    }

    @Test
    public void thinZPoolsListParsesFilesystemsOnly() throws StorageException
    {
        expect(
            StringUtils.concat(ZFS_LIST_FILESYSTEMS_BASE_CMD, "rpool"),
            "rpool\t107374182400\tfilesystem\n" +
                "rpool/linstor\t53687091200\tfilesystem\n" +
                // lines with unparsable numbers have to be ignored
                "rpool/badfs\tnot-a-number\tfilesystem\n" +
                // non-filesystem lines have to be ignored
                "rpool/vol0\t1073741824\tvolume\n"
        );

        HashMap<String, ZfsUtils.ZfsInfo> thinPools = ZfsUtils.getThinZPoolsList(
            extCmd,
            Collections.singletonList("rpool")
        );

        assertThat(thinPools).containsOnlyKeys("rpool", "rpool/linstor");

        ZfsUtils.ZfsInfo nested = thinPools.get("rpool/linstor");
        assertThat(nested.poolName).isEqualTo("rpool");
        assertThat(nested.identifier).isEqualTo("linstor");
        assertThat(nested.typeStr).isEqualTo("filesystem");
        assertThat(nested.type).isEqualTo(ZfsVolumeType.FILESYSTEM);
        assertThat(nested.path).isEqualTo("/dev/zvol/rpool/linstor");
        // "available" is reported in bytes and converted to KiB
        assertThat(nested.usableSize).isEqualTo(52428800L);
        assertThat(nested.allocatedSize).isEqualTo(-1L);
        assertThat(nested.volBlockSize).isNull();

        assertThat(thinPools.get("rpool").usableSize).isEqualTo(104857600L);
    }

    @Test
    public void thinZPoolsListEmptyOutputResultsInEmptyMap() throws StorageException
    {
        expect(StringUtils.concat(ZFS_LIST_FILESYSTEMS_BASE_CMD, "rpool"), "");

        assertThat(ZfsUtils.getThinZPoolsList(extCmd, Collections.singletonList("rpool"))).isEmpty();
    }

    @Test
    public void zPoolTotalSizeUsesQuotaOrFallsBackToPoolSize() throws StorageException
    {
        Set<String> zPools = new LinkedHashSet<>(Arrays.asList("tank/ds1", "tank/ds2"));
        expect(
            new String[]
            {
                "zfs", "get", "quota", "-o", "name,value", "-Hp", "tank/ds1", "tank/ds2"
            },
            "tank/ds1\t0\n" +
                "tank/ds2\t10737418240\n"
        );
        expect(
            new String[]
            {
                "zpool", "get", "size", "-Hp", "tank"
            },
            "tank\tsize\t21474836480\t-\n"
        );

        Map<String, Long> totalSizes = ZfsUtils.getZPoolTotalSize(extCmd, zPools);

        assertThat(totalSizes).containsOnlyKeys("tank/ds1", "tank/ds2");
        // no quota set -> total size of the root zpool, converted from bytes to KiB
        assertThat(totalSizes.get("tank/ds1")).isEqualTo(20971520L);
        // quota set -> quota value, converted from bytes to KiB
        assertThat(totalSizes.get("tank/ds2")).isEqualTo(10485760L);
    }

    @Test
    public void zPoolTotalSizeThrowsOnUnparsableQuota()
    {
        expect(
            new String[]
            {
                "zfs", "get", "quota", "-o", "name,value", "-Hp", "tank/ds1"
            },
            "tank/ds1\tnot-a-number\n"
        );

        assertThatThrownBy(() -> ZfsUtils.getZPoolTotalSize(extCmd, Collections.singleton("tank/ds1")))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("quota size");
    }

    private void expect(String[] argv, String stdOut)
    {
        extCmd.setExpectedBehavior(new Command(argv), new TestOutputData(argv, stdOut, "", 0));
    }
}
