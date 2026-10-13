package com.linbit.linstor.layer.storage.lvm.utils;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

/**
 * Tests the parsing of "lvs" / "vgs" / "pvdisplay" output done by {@link LvmUtils} by feeding canned
 * command output through a {@link TestExtCmd}.
 */
public class LvmUtilsExtCmdTest
{
    private static final String LVS_COLUMNS =
        "lv_name,lv_path,lv_size,vg_name,pool_lv,data_percent,lv_attr,metadata_percent,chunk_size,stripes,origin," +
            "kernel_discards";
    private static final String VGS_THICK_COLUMNS = "vg_name,vg_extent_size,vg_size,vg_free";
    private static final String VGS_THIN_COLUMNS =
        "vg_name,vg_extent_size,vg_size,vg_free,lv_name,lv_size,data_percent";
    private static final String IGNORE_DRBD_CONFIG =
        "devices { ignore_suspended_devices=1 filter=[\"r|^/dev/drbd.*|\"] }";

    private TestExtCmd extCmd;
    private ExtCmdFactory extCmdFactory;

    @Before
    public void setUp()
    {
        extCmd = new TestExtCmd(new EmptyErrorReporter());
        extCmdFactory = Mockito.mock(ExtCmdFactory.class);
        Mockito.when(extCmdFactory.create()).thenReturn(extCmd);
        LvmUtils.recacheNext();
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
    public void lvsInfoParsesThickThinPoolAndThinVolume() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Collections.singleton("scratch"));
        expect(pvDisplayCommand("scratch"), "  /dev/sdz1\n");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/sdz1");
        expect(
            lvsCommand(lvmConfig, vgSet),
            "  thick_00000;/dev/scratch/thick_00000;4194304.00;scratch;;;-wi-ao----;;0;1;;\n" +
                "  thinpool;/dev/scratch/thinpool;8388608.00;scratch;;20.51;twi-aotz--;10.42;64.00;1;;nopassdown\n" +
                "  thin_00000;/dev/scratch/thin_00000;1048576.00;scratch;thinpool;15,29;Vwi-aotz--;;0;2;snap1;\n" +
                "  WARNING: some lvm warning that does not use the delimiter\n" +
                "\n"
        );

        Map<String, Map<String, LvmUtils.LvsInfo>> lvsInfo = LvmUtils.getLvsInfo(extCmdFactory, vgSet);

        assertThat(lvsInfo).containsOnlyKeys("scratch");
        Map<String, LvmUtils.LvsInfo> byLvName = lvsInfo.get("scratch");
        assertThat(byLvName).containsOnlyKeys("thick_00000", "thinpool", "thin_00000");

        LvmUtils.LvsInfo thick = byLvName.get("thick_00000");
        assertThat(thick.volumeGroup).isEqualTo("scratch");
        assertThat(thick.thinPool).isNull();
        assertThat(thick.identifier).isEqualTo("thick_00000");
        assertThat(thick.path).isEqualTo("/dev/scratch/thick_00000");
        assertThat(thick.size).isEqualTo(4194304L);
        // empty data_percent has to fall back to the default of 100%
        assertThat(thick.dataPercent).isEqualTo(100f);
        assertThat(thick.attributes).isEqualTo("-wi-ao----");
        assertThat(thick.metaDataPercentStr).isEmpty();
        assertThat(thick.chunkSizeInKib).isEqualTo(0L);
        assertThat(thick.stripes).isEqualTo(1);
        // empty origin column has to be mapped to null
        assertThat(thick.origin).isNull();
        assertThat(thick.kernelDiscards).isNull();

        LvmUtils.LvsInfo thinPool = byLvName.get("thinpool");
        assertThat(thinPool.thinPool).isNull();
        assertThat(thinPool.size).isEqualTo(8388608L);
        assertThat(thinPool.dataPercent).isEqualTo(20.51f);
        assertThat(thinPool.attributes).isEqualTo("twi-aotz--");
        assertThat(thinPool.metaDataPercentStr).isEqualTo("10.42");
        assertThat(thinPool.chunkSizeInKib).isEqualTo(64L);
        assertThat(thinPool.kernelDiscards).isEqualTo("nopassdown");

        LvmUtils.LvsInfo thinVlm = byLvName.get("thin_00000");
        assertThat(thinVlm.thinPool).isEqualTo("thinpool");
        assertThat(thinVlm.size).isEqualTo(1048576L);
        // ',' has to be accepted as decimal separator (localized lvm output)
        assertThat(thinVlm.dataPercent).isEqualTo(15.29f);
        assertThat(thinVlm.stripes).isEqualTo(2);
        assertThat(thinVlm.origin).isEqualTo("snap1");
    }

    @Test
    public void lvsInfoParsesMultipleVolumeGroupsAndThinPoolNotation() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Arrays.asList("vg_a", "vg_b/thin1"));
        expect(pvDisplayCommand("vg_a"), "  /dev/sda2\n");
        // "vg_b" (stripped thin pool part) has no physical volumes
        expect(pvDisplayCommand("vg_b"), "");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/sda2");
        expect(
            lvsCommand(lvmConfig, vgSet),
            "  lv_one;/dev/vg_a/lv_one;2097152.00;vg_a;;;-wi-a-----;;0;1;;\n" +
                "  lv_two;/dev/vg_b/lv_two;307200.00;vg_b;thin1;0.00;Vwi-a-tz--;;0;1;;\n"
        );

        Map<String, Map<String, LvmUtils.LvsInfo>> lvsInfo = LvmUtils.getLvsInfo(extCmdFactory, vgSet);

        assertThat(lvsInfo).containsOnlyKeys("vg_a", "vg_b");
        assertThat(lvsInfo.get("vg_a")).containsOnlyKeys("lv_one");
        assertThat(lvsInfo.get("vg_a").get("lv_one").size).isEqualTo(2097152L);
        assertThat(lvsInfo.get("vg_b")).containsOnlyKeys("lv_two");

        LvmUtils.LvsInfo lvTwo = lvsInfo.get("vg_b").get("lv_two");
        assertThat(lvTwo.thinPool).isEqualTo("thin1");
        assertThat(lvTwo.size).isEqualTo(307200L);
        assertThat(lvTwo.dataPercent).isEqualTo(0f);
    }

    @Test
    public void lvsInfoEmptyOutputResultsInEmptyMap() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Collections.singleton("empty_vg"));
        expect(pvDisplayCommand("empty_vg"), "  /dev/sdc1\n");
        expect(lvsCommand(LvmUtils.getLvmFilterByPhysicalVolumes("/dev/sdc1"), vgSet), "\n");

        assertThat(LvmUtils.getLvsInfo(extCmdFactory, vgSet)).isEmpty();
    }

    @Test
    public void lvsInfoThrowsOnUnparsableSize()
    {
        expect(
            lvsCommand("", Collections.emptySet()),
            "  lv0;/dev/vg/lv0;not-a-number;vg;;;-wi-ao----;;0;1;;\n"
        );

        assertThatThrownBy(() -> LvmUtils.getLvsInfo(extCmdFactory, Collections.emptySet()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Unable to parse logical volume size");
    }

    @Test
    public void lvsInfoThrowsOnUnparsableDataPercent()
    {
        expect(
            lvsCommand("", Collections.emptySet()),
            "  lv0;/dev/vg/lv0;1024.00;vg;pool;abc;Vwi-aotz--;;0;1;;\n"
        );

        assertThatThrownBy(() -> LvmUtils.getLvsInfo(extCmdFactory, Collections.emptySet()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Unable to parse data_percent of thin lv");
    }

    @Test
    public void lvsInfoThrowsOnUnparsableChunkSize()
    {
        expect(
            lvsCommand("", Collections.emptySet()),
            "  lv0;/dev/vg/lv0;1024.00;vg;;;-wi-ao----;;bad;1;;\n"
        );

        assertThatThrownBy(() -> LvmUtils.getLvsInfo(extCmdFactory, Collections.emptySet()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Unable to parse logical chunk size");
    }

    @Test
    public void lvsInfoThrowsOnUnparsableStripes()
    {
        expect(
            lvsCommand("", Collections.emptySet()),
            "  lv0;/dev/vg/lv0;1024.00;vg;;;-wi-ao----;;0;x;;\n"
        );

        assertThatThrownBy(() -> LvmUtils.getLvsInfo(extCmdFactory, Collections.emptySet()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Unable to parse stripes");
    }

    @Test
    public void vgsInfoThickParsesMultipleVolumeGroups() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Arrays.asList("data_vg", "other_vg"));
        expect(pvDisplayCommand("data_vg"), "  /dev/nvme0n1p3\n");
        expect(pvDisplayCommand("other_vg"), "");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/nvme0n1p3");
        expect(
            vgsCommand(false, lvmConfig, vgSet),
            "  data_vg;4096.00;209715200.00;104857600.00\n" +
                "  other_vg;1024.00;52428800.00;0\n"
        );

        Map<String, LvmUtils.VgsInfo> vgsInfo = LvmUtils.getVgsInfo(extCmdFactory, vgSet, false);

        assertThat(vgsInfo).containsOnlyKeys("data_vg", "other_vg");

        LvmUtils.VgsInfo dataVg = vgsInfo.get("data_vg");
        assertThat(dataVg.vgName).isEqualTo("data_vg");
        assertThat(dataVg.vgExtentSize).isEqualTo(4096L);
        assertThat(dataVg.vgSize).isEqualTo(209715200L);
        assertThat(dataVg.vgFree).isEqualTo(104857600L);
        assertThat(dataVg.lvName).isNull();
        assertThat(dataVg.lvSize).isNull();
        assertThat(dataVg.dataPercent).isNull();

        LvmUtils.VgsInfo otherVg = vgsInfo.get("other_vg");
        assertThat(otherVg.vgExtentSize).isEqualTo(1024L);
        assertThat(otherVg.vgFree).isEqualTo(0L);
    }

    @Test
    public void vgsInfoThinParsesOneRowPerThinPool() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Collections.singleton("thin_vg"));
        expect(pvDisplayCommand("thin_vg"), "  /dev/sdd1\n");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/sdd1");
        expect(
            vgsCommand(true, lvmConfig, vgSet),
            "  thin_vg;4096.00;104857600.00;52428800.00;thinpool;40960000.00;12.5\n" +
                "  thin_vg;4096.00;104857600.00;52428800.00;pool2;8192000.00;0,00\n"
        );

        Map<String, LvmUtils.VgsInfo> vgsInfo = LvmUtils.getVgsInfo(extCmdFactory, vgSet, true);

        // one entry per "<vg>/<lv>" plus one entry for the plain vg name
        assertThat(vgsInfo).containsOnlyKeys("thin_vg", "thin_vg/thinpool", "thin_vg/pool2");

        LvmUtils.VgsInfo thinPool = vgsInfo.get("thin_vg/thinpool");
        assertThat(thinPool.vgName).isEqualTo("thin_vg");
        assertThat(thinPool.vgExtentSize).isEqualTo(4096L);
        assertThat(thinPool.vgSize).isEqualTo(104857600L);
        assertThat(thinPool.vgFree).isEqualTo(52428800L);
        assertThat(thinPool.lvName).isEqualTo("thinpool");
        assertThat(thinPool.lvSize).isEqualTo(40960000L);
        assertThat(thinPool.dataPercent).isEqualTo(12.5f);

        LvmUtils.VgsInfo pool2 = vgsInfo.get("thin_vg/pool2");
        assertThat(pool2.lvName).isEqualTo("pool2");
        assertThat(pool2.lvSize).isEqualTo(8192000L);
        // ',' has to be accepted as decimal separator (localized lvm output)
        assertThat(pool2.dataPercent).isEqualTo(0f);

        // the plain vg entry is overwritten by every row, the last one wins
        assertThat(vgsInfo.get("thin_vg").lvName).isEqualTo("pool2");
    }

    @Test
    public void vgsInfoThinEmptyDataPercentDefaultsToHundred() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Collections.singleton("dflt_thin_vg"));
        expect(pvDisplayCommand("dflt_thin_vg"), "  /dev/sde1\n");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/sde1");
        expect(
            vgsCommand(true, lvmConfig, vgSet),
            "  dflt_thin_vg;4096.00;104857600.00;104857600.00;thinpool;40960000.00;\n"
        );

        Map<String, LvmUtils.VgsInfo> vgsInfo = LvmUtils.getVgsInfo(extCmdFactory, vgSet, true);

        assertThat(vgsInfo).containsOnlyKeys("dflt_thin_vg", "dflt_thin_vg/thinpool");
        LvmUtils.VgsInfo vgInfo = vgsInfo.get("dflt_thin_vg/thinpool");
        assertThat(vgInfo.vgExtentSize).isEqualTo(4096L);
        assertThat(vgInfo.lvName).isEqualTo("thinpool");
        assertThat(vgInfo.lvSize).isEqualTo(40960000L);
        // empty data_percent has to fall back to the default of 100%
        assertThat(vgInfo.dataPercent).isEqualTo(100f);
    }

    @Test
    public void vgsInfoRetriesWhenRequestedVgIsMissing() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Collections.singleton("absent_vg"));
        // no physical volumes -> empty lvm config, both commands are executed twice (recache + retry)
        expect(pvDisplayCommand("absent_vg"), "");
        expect(vgsCommand(false, "", vgSet), "");

        assertThat(LvmUtils.getVgsInfo(extCmdFactory, vgSet, false)).isEmpty();
    }

    @Test
    public void vgsInfoThickThrowsOnUnparsableVgSize()
    {
        expect(
            vgsCommand(false, "", Collections.emptySet()),
            "  bad_vg;4096.00;garbage;100.00\n"
        );

        assertThatThrownBy(() -> LvmUtils.getVgsInfo(extCmdFactory, Collections.emptySet(), false))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("vg_size")
            .hasMessageContaining("bad_vg");
    }

    @Test
    public void vgsInfoThinThrowsOnUnparsableDataPercent()
    {
        expect(
            vgsCommand(true, "", Collections.emptySet()),
            "  bad_vg;4096.00;102400.00;51200.00;pool;1024.00;pct\n"
        );

        assertThatThrownBy(() -> LvmUtils.getVgsInfo(extCmdFactory, Collections.emptySet(), true))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("data_percent")
            .hasMessageContaining("bad_vg");
    }

    @Test
    public void thinFreeSizeIsComputedFromLvSizeAndDataPercent() throws StorageException
    {
        Set<String> vgSet = new LinkedHashSet<>(Collections.singleton("free_vg"));
        expect(pvDisplayCommand("free_vg"), "  /dev/sdf1\n");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/sdf1");
        expect(
            vgsCommand(true, lvmConfig, vgSet),
            "  free_vg;4096.00;104857600.00;52428800.00;thinpool;40960000.00;25.00\n" +
                "  free_vg;4096.00;104857600.00;52428800.00;pool2;8388608.00;12,5\n"
        );

        Map<String, Map<String, Long>> freeSizes = LvmUtils.getThinFreeSize(extCmdFactory, vgSet);

        assertThat(freeSizes).containsOnlyKeys("free_vg");
        Map<String, Long> byThinPool = freeSizes.get("free_vg");
        assertThat(byThinPool).containsOnlyKeys("thinpool", "pool2");
        // 40960000k * (1 - 25%) == 30720000k
        assertThat(byThinPool.get("thinpool")).isEqualTo(30720000L);
        // 8388608k * (1 - 12.5%) == 7340032k
        assertThat(byThinPool.get("pool2")).isEqualTo(7340032L);
    }

    private void expect(String[] argv, String stdOut)
    {
        extCmd.setExpectedBehavior(new Command(argv), new TestOutputData(argv, stdOut, "", 0));
    }

    private String[] pvDisplayCommand(String volumeGroup)
    {
        return new String[]
        {
            "pvdisplay",
            "--config", IGNORE_DRBD_CONFIG,
            "--columns",
            "-o", "pv_name",
            "-S", "vg_name=" + volumeGroup,
            "--noheadings",
            "--nosuffix"
        };
    }

    private String[] lvsCommand(String lvmConfig, Set<String> volumeGroups)
    {
        List<String> cmd = new ArrayList<>();
        cmd.add("lvs");
        if (!lvmConfig.isEmpty())
        {
            cmd.add("--config");
            cmd.add(lvmConfig);
        }
        Collections.addAll(
            cmd,
            "-o", LVS_COLUMNS,
            "--separator", ";",
            "--noheadings",
            "--units", "k",
            "--nosuffix"
        );
        cmd.addAll(volumeGroups);
        return cmd.toArray(new String[0]);
    }

    private String[] vgsCommand(boolean thin, String lvmConfig, Set<String> volumeGroups)
    {
        List<String> cmd = new ArrayList<>();
        cmd.add("vgs");
        if (!lvmConfig.isEmpty())
        {
            cmd.add("--config");
            cmd.add(lvmConfig);
        }
        Collections.addAll(
            cmd,
            "-o", thin ? VGS_THIN_COLUMNS : VGS_THICK_COLUMNS,
            "--separator", ";",
            "--units", "k",
            "--noheadings",
            "--nosuffix"
        );
        cmd.addAll(volumeGroups);
        return cmd.toArray(new String[0]);
    }
}
