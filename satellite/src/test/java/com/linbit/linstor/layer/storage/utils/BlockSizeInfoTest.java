package com.linbit.linstor.layer.storage.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Tests the sysfs lookup of {@link BlockSizeInfo} against a fake sysfs tree, mimicking the kernel's layout:
 * <code>/sys/class/block/&lt;dev&gt;</code> symlinks into <code>/sys/devices/...</code>, where only whole disks
 * have a <code>queue</code> directory and partitions are subdirectories of their disk.
 */
public class BlockSizeInfoTest
{
    private static final String DISC_GRAN = "discard_granularity";
    private static final String PHY_BLK_SIZE = "physical_block_size";
    private static final long DFLT = 7;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private Path sysClassBlock;
    private Path devDir;

    @Before
    public void setUp() throws IOException
    {
        Path root = tmp.getRoot().toPath();
        Path devices = Files.createDirectories(root.resolve("sys/devices"));
        sysClassBlock = Files.createDirectories(root.resolve("sys/class/block"));
        devDir = Files.createDirectories(root.resolve("dev"));

        Path vdc = devices.resolve("pci0/virtio2/block/vdc");
        writeFile(vdc.resolve("queue/" + DISC_GRAN), "512\n");
        writeFile(vdc.resolve("queue/" + PHY_BLK_SIZE), "4096\n");
        writeFile(vdc.resolve("vdc1/partition"), "1\n");
        addBlockDevice("vdc", vdc);
        addBlockDevice("vdc1", vdc.resolve("vdc1"));

        Path dm0 = devices.resolve("virtual/block/dm-0");
        writeFile(dm0.resolve("queue/" + DISC_GRAN), "65536\n");
        addBlockDevice("dm-0", dm0);
        Files.createDirectories(devDir.resolve("vg"));
        Files.createSymbolicLink(devDir.resolve("vg/lv"), Path.of("../dm-0"));

        // a device without queue directory that is not a partition must not borrow its parent's queue
        Files.createDirectories(vdc.resolve("holder"));
        addBlockDevice("holder", vdc.resolve("holder"));
    }

    private void addBlockDevice(String name, Path sysDevDir) throws IOException
    {
        Files.createSymbolicLink(sysClassBlock.resolve(name), sysClassBlock.relativize(sysDevDir));
        Files.createFile(devDir.resolve(name));
    }

    private static void writeFile(Path path, String content) throws IOException
    {
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }

    private long getSize(String devPath, String queueId)
    {
        return BlockSizeInfo.getSize(sysClassBlock, devDir.resolve(devPath), queueId, DFLT, 0, Long.MAX_VALUE);
    }

    @Test
    public void wholeDisk()
    {
        assertEquals(512, getSize("vdc", DISC_GRAN));
        assertEquals(4096, getSize("vdc", PHY_BLK_SIZE));
    }

    @Test
    public void partitionUsesQueueOfDisk()
    {
        assertEquals(512, getSize("vdc1", DISC_GRAN));
        assertEquals(4096, getSize("vdc1", PHY_BLK_SIZE));
    }

    @Test
    public void symlinkedDevice()
    {
        assertEquals(65536, getSize("vg/lv", DISC_GRAN));
    }

    @Test
    public void missingValueReturnsDefault()
    {
        assertEquals(DFLT, getSize("dm-0", PHY_BLK_SIZE));
    }

    @Test
    public void unknownDeviceReturnsDefault() throws IOException
    {
        Files.createFile(devDir.resolve("nosuchdev"));
        assertEquals(DFLT, getSize("nosuchdev", DISC_GRAN));
    }

    @Test
    public void nonPartitionWithoutQueueReturnsDefault()
    {
        assertEquals(DFLT, getSize("holder", DISC_GRAN));
    }

    @Test
    public void valueIsBounded()
    {
        assertEquals(1024, BlockSizeInfo.getSize(sysClassBlock, devDir.resolve("vdc"), PHY_BLK_SIZE, DFLT, 0, 1024));
    }
}
