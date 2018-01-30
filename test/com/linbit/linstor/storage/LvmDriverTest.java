package com.linbit.linstor.storage;

import static com.linbit.extproc.utils.TestExtCmd.Command;
import static com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import static com.linbit.linstor.storage.LvmDriver.LVM_CREATE_DEFAULT;
import static com.linbit.linstor.storage.LvmDriver.LVM_LVS_DEFAULT;
import static com.linbit.linstor.storage.LvmDriver.LVM_REMOVE_DEFAULT;
import static com.linbit.linstor.storage.LvmDriver.LVM_VGS_DEFAULT;
import static com.linbit.linstor.storage.LvmDriver.LVM_VOLUME_GROUP_DEFAULT;
import static com.linbit.linstor.storage.StorageConstants.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.linbit.drbd.md.MetaData;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    LvmDriver.class,
    ExtCmd.class
})
public class LvmDriverTest extends StorageTestUtils
{
    protected static final String EXT_COMMAND_SEPARATOR = ";";
    protected static final long DEFAULT_EXTENT_SIZE = 4_096;

    public LvmDriverTest() throws Exception
    {
        super(new StorageTestUtils.DriverFactory()
        {
            @Override
            public StorageDriver createDriver() throws StorageException
            {
                return new LvmDriver();
            }
        });
    }

    public LvmDriverTest(DriverFactory driverFactory) throws Exception
    {
        super(driverFactory);
    }

    @Test
    public void testConfigVolumeGroup() throws StorageException
    {
        final HashMap<String,String> config = new HashMap<>();

        String volumeGroup = "otherName";
        config.put(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
        driver.setConfiguration(config);

        ec.clearBehaviors();
        volumeGroup = "_specialName";
        config.put(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
        driver.setConfiguration(config);

        ec.clearBehaviors();
        volumeGroup = "special-Name";
        config.put(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
        driver.setConfiguration(config);
    }

    @Test(expected = StorageException.class)
    public void testConfigVolumeGroupValidNotExistent() throws StorageException
    {
        String volumeGroup = "valid";
        Map<String, String> config = createMap(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup, false);
        driver.setConfiguration(config);
    }

    @Test(expected = StorageException.class)
    public void testConfigVolumeGroupEmpty() throws StorageException
    {
        driver.setConfiguration(createMap(CONFIG_LVM_VOLUME_GROUP_KEY, ""));
    }

    @Test(expected = StorageException.class)
    public void testConfigVolumeGroupWhitespacesOnly() throws StorageException
    {
        driver.setConfiguration(createMap(CONFIG_LVM_VOLUME_GROUP_KEY, "  "));
    }

    @Test
    public void testConfigCommand() throws StorageException, IOException
    {
        expectException(createMap(CONFIG_LVM_CHANGE_COMMAND_KEY, "notLvmChange"));
        expectException(createMap(CONFIG_LVM_CREATE_COMMAND_KEY, "notLvmCreate"));
        expectException(createMap(CONFIG_LVM_LVS_COMMAND_KEY, "notLvs"));
        expectException(createMap(CONFIG_LVM_REMOVE_COMMAND_KEY, "notLvmRemove"));
        expectException(createMap(CONFIG_LVM_VGS_COMMAND_KEY, "notVgs"));

        String vgsCommand = "otherVgs";
        File tmpFile = tempFolder.newFile(vgsCommand);
        tmpFile.setExecutable(true);
        driver.setConfiguration(createMap(CONFIG_LVM_VGS_COMMAND_KEY, tmpFile.getAbsolutePath()));

        String volumeGroup = "newVolumeGroup";
        expectCheckVolumeGroup(tmpFile.getAbsolutePath(),volumeGroup);
        driver.setConfiguration(createMap(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup));
    }

    @Test
    public void testConfigToleranceFactor() throws StorageException
    {
        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "2.4"));
        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "0"));
        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "-1"));
        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "NaN"));

        driver.setConfiguration(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY,"4"));

        final String volumeIdentifier = "identifier";

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, volumeIdentifier, "118784.00k");// 102400 + 4096 * 4
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, "4096.00k");

        driver.checkVolume(volumeIdentifier, 102400);

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, volumeIdentifier, "118785.00k"); // +1 as before

        try
        {
            driver.checkVolume(volumeIdentifier, 102400);
            fail("volume size should be higher than tolerated");
        }
        catch (StorageException storExc)
        {
            // expected
        }
    }

    @Test
    public void testStartVolume() throws StorageException
    {
        final String identifier = "identifier";
        driver.startVolume(identifier); // should not trigger anything
    }

    @Test
    public void testStartUnknownVolume() throws StorageException
    {
        final String unknownIdentifier = "unknown";
        driver.startVolume(unknownIdentifier); // should not trigger anything
    }

    @Test
    public void testStopVolume() throws StorageException
    {
        final String identifier = "identifier";
        driver.stopVolume(identifier); // should not trigger anything
    }

    @Test
    public void testStopUnknownVolume() throws StorageException
    {
        final String unknownIdentifier = "unknown";
        driver.stopVolume(unknownIdentifier); // should not trigger anything
    }

    @Test
    public void testCreateVolumeDelayed() throws Exception
    {
        final long volumeSize = 102400;
        final String identifier = "testVolume";
        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, Long.toString(volumeSize)+".00k");
        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, identifier, LVM_VOLUME_GROUP_DEFAULT, false);
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, DEFAULT_EXTENT_SIZE);

        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.CREATE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        final FileEntry testEntry = new FileEntry(
            Paths.get(expectedFilePath),
            Event.CREATE,
            emptyFileObserver);

        Thread thread = new Thread(
            new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.sleep(2000); // give the driver some time to execute the .createVolume command
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    testFileEntryGroup.fileEvent(testEntry);
                }
            }
        );
        thread.start();
        driver.createVolume(identifier, volumeSize);
    }

    @Test
    public void testCreateVolumeInstant() throws Exception
    {
        final long volumeSize = 102400;
        final String identifier = "testVolume";

        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, identifier, LVM_VOLUME_GROUP_DEFAULT, false);
        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, DEFAULT_EXTENT_SIZE);

        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.CREATE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        final FileEntry testEntry = new FileEntry(
            Paths.get(expectedFilePath),
            Event.CREATE,
            emptyFileObserver);

        testFileEntryGroup.fileEvent(testEntry);
        driver.createVolume(identifier, volumeSize);
    }

    @Test(expected = StorageException.class)
    public void testCreateVolumeTimeout() throws Exception
    {
        final long volumeSize = 102400;
        final String identifier = "testVolume";

        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, identifier, LVM_VOLUME_GROUP_DEFAULT, false);
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, DEFAULT_EXTENT_SIZE);

        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.CREATE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        // do not fire file event
        driver.createVolume(identifier, volumeSize);
    }


    @Test(expected = StorageException.class)
    public void testCreateExistingVolume() throws Exception
    {
        final long volumeSize = 102400;
        final String volumeName = "testVolume";

        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, volumeName, LVM_VOLUME_GROUP_DEFAULT, true);
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, DEFAULT_EXTENT_SIZE);

        driver.createVolume(volumeName, volumeSize);
    }



    @Test
    public void testDeleteVolume() throws Exception
    {
        final String identifier = "testVolume";

        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, identifier, LVM_VOLUME_GROUP_DEFAULT, true);

        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.DELETE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        final FileEntry testEntry = new FileEntry(
            Paths.get(expectedFilePath),
            Event.DELETE,
            emptyFileObserver);

        testFileEntryGroup.fileEvent(testEntry);
        driver.deleteVolume(identifier);
    }

    @Test(expected = StorageException.class)
    public void testDeleteVolumeTimeout() throws Exception
    {
        final String identifier = "testVolume";

        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, identifier, LVM_VOLUME_GROUP_DEFAULT, true);

        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.DELETE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        // do not fire file event
        driver.deleteVolume(identifier);
    }

    @Test(expected = StorageException.class)
    public void testDeleteVolumeNonExisting() throws Exception
    {
        final String volumeName = "testVolume";

        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, volumeName, LVM_VOLUME_GROUP_DEFAULT, false);

        driver.deleteVolume(volumeName);
    }

    @Test
    public void testCheckVolume() throws StorageException
    {
        final String identifier = "testVolume";
        final long size = 102_400;

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, size);
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, DEFAULT_EXTENT_SIZE);

        driver.checkVolume(identifier, size);
    }

    @Test(expected = StorageException.class)
    public void testCheckVolumeSizeTooLarge() throws StorageException
    {
        final String identifier = "testVolume";
        final long size = MetaData.DRBD_MAX_kiB + 1;

        driver.checkVolume(identifier, size);
    }

    @Test(expected = StorageException.class)
    public void testCheckVolumeTooSmall() throws StorageException
    {
        final String identifier = "testVolume";
        final long size = 102_400;

        expectLvsInfoBehavior(
            LVM_LVS_DEFAULT,
            LVM_VOLUME_GROUP_DEFAULT,
            identifier,
            size - 10); // user wanted at least 100 MB, but we give him a little bit less
        // should trigger exception

        driver.checkVolume(identifier, size);
    }

    @Test
    public void testVolumePath() throws StorageException
    {
        final String identifier = "testVolume";

        final String path = driver.getVolumePath(identifier);
        assertEquals("/dev/" +
            LVM_VOLUME_GROUP_DEFAULT + "/" +
            identifier,
            path);
    }

    @Test
    public void testSize() throws StorageException
    {
        final String identifier = "testVolume";
        final long volumeSize = 102_400;

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);

        final long size = driver.getSize(identifier);
        assertEquals(volumeSize, size);
    }

    @Test
    public void testFreeSize() throws StorageException
    {
        final long size = 1 * 1024 * 1024 * 1024;
        expectVgsFreeSizeCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, size);

        assertEquals(size, driver.getFreeSize());
    }

    @Test(expected = StorageException.class)
    public void testSizeUnknownVolume() throws StorageException
    {
        final String identifier = "testVolume";
        final long volumeSize = 102_400;

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);

        driver.getSize("otherVolume");
    }

    @Test
    public void testTraits() throws StorageException
    {
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, DEFAULT_EXTENT_SIZE);
        Map<String, String> traits = driver.getTraits();

        final String traitProv = traits.get(DriverTraits.KEY_PROV);
        assertEquals(DriverTraits.PROV_FAT, traitProv);

        final String size = traits.get(DriverTraits.KEY_ALLOC_UNIT);
        assertEquals("4096", size);
    }

    @Test
    public void testConfigurationKeys()
    {
        final HashSet<String> keys = new HashSet<>(driver.getConfigurationKeys());

        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY));
        assertTrue(keys.remove(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY));

        assertTrue(keys.isEmpty());
    }

    protected void expectStartVolumeCommand(
        final String lvmChangeDefault,
        final String lvmVolumeGroupDefault,
        final String identifier,
        final boolean volumeExists)
    {
        final Command command = new Command(
            lvmChangeDefault,
            "-ay",
            "-kn", "-K",
            lvmVolumeGroupDefault + "/" + identifier);

        final OutputData outData;
        if (volumeExists)
        {
            outData = new TestOutputData("", "", 0);
        }
        else
        {
            outData = new TestOutputData(
                "",
                "One or more specified logical volume(s, retCode) not found.",
                5);
        }
        ec.setExpectedBehavior(command, outData);
    }

    protected void expectStopVolumeCommand(
        final String lvmChangeDefault,
        final String lvmVolumeGroupDefault,
        final String identifier,
        final boolean volumeExists)
    {
        final Command command = new Command(
            lvmChangeDefault,
            "-an",
            lvmVolumeGroupDefault + "/" + identifier);

        final OutputData outData;
        if (volumeExists)
        {
            outData = new TestOutputData("", "", 0);
        }
        else
        {
            outData = new TestOutputData(
                "",
                "One or more specified logical volume(s, retCode) not found.",
                5);
        }
        ec.setExpectedBehavior(command, outData);
    }

    protected void expectLvmDeleteVolumeBehavior(
        final String lvmRemoveCommand,
        final String identifier,
        final String volumeGroup,
        final boolean volumeExists)
    {
        final Command cmd = new Command(
            lvmRemoveCommand,
            "-f",
            volumeGroup + File.separator + identifier);

        final OutputData outData;
        if (volumeExists)
        {
            outData = new TestOutputData(
                "Logical volume \"" + identifier + "\" successfully removed",
                "",
                0);
        }
        else
        {
            outData = new TestOutputData(
                "",
                "One or more specified logical volume(s) not found.",
                5);
        }
        ec.setExpectedBehavior(cmd, outData);
    }

    protected void expectLvmCreateVolumeBehavior(
        final String lvmCreateCommand,
        final long volumeSize,
        final String identifier,
        final String volumeGroup,
        final boolean volumeExists)
    {
        final Command cmd = new Command(
            lvmCreateCommand,
            "--size", volumeSize + "k",
            "-n", identifier,
            volumeGroup
            );

        final OutputData outData;
        if (volumeExists)
        {
            outData = new TestOutputData(
                "",
                "Logical volume \""+identifier+"\" already exists in volume group \""+volumeGroup+"\"",
                5);
        }
        else
        {
            outData = new TestOutputData(
                "Logical volume \"identifier\" created",
                "",
                0);
        }

        ec.setExpectedBehavior(cmd, outData);
    }

    protected void expectCheckVolumeGroup(final String vgsCommand, final String volumeGroup)
    {
        expectCheckVolumeGroup(vgsCommand, volumeGroup, true);
    }

    protected void expectCheckVolumeGroup(final String vgsCommand, final String volumeGroup, final boolean success)
    {
        Command command = new Command(
            vgsCommand,
            "-o", "vg_name",
            "--noheadings"
            );
        OutputData outData;
        if (success)
        {
            outData = new TestOutputData(
                "   " + volumeGroup,
                "",
                0);
        }
        else
        {
            outData = new TestOutputData(
                "",
                "  Volume groug \""+volumeGroup+"\" not found",
                5);
        }
        ec.setExpectedBehavior(command, outData);
    }

    protected void expectLvsInfoBehavior(
        final String lvsCommand,
        final String volumeGroup,
        final String identifier,
        final long volumeSize)
    {
        expectLvsInfoBehavior(lvsCommand, volumeGroup, identifier, Long.toString(volumeSize)+".00k");
    }

    protected void expectLvsInfoBehavior(
        final String lvsCommand,
        final String volumeGroup,
        final String identifier,
        final String volumeSize)
    {
        Command command = new Command(
            lvsCommand,
            "-o", "lv_name,lv_path,lv_size",
            "--separator", EXT_COMMAND_SEPARATOR,
            "--noheadings",
            "--units", "k",
            volumeGroup);
        StringBuilder sb = new StringBuilder();
        OutputData outData = new TestOutputData(
            sb
            .append(identifier).append(EXT_COMMAND_SEPARATOR)
            .append("/dev/").append(LVM_VOLUME_GROUP_DEFAULT).append("/").append(identifier).append(EXT_COMMAND_SEPARATOR)
            .append(volumeSize).toString(),
            "",
            0);

        ec.setExpectedBehavior(command, outData);

    }

    protected void expectVgsExtentCommand(final String vgsCommand, final String volumeGroup, final long extentSize)
    {
        expectVgsExtentCommand(vgsCommand, volumeGroup, Long.toString(extentSize) + ".00");
    }

    protected void expectVgsExtentCommand(
        final String vgsCommand,
        final String volumeGroup,
        final String extentSize)
    {
        expectVgsPropCommand(vgsCommand, "vg_extent_size", volumeGroup, extentSize);
    }

    protected void expectVgsFreeSizeCommand(final String vgsCommand, final String volumeGroup, final long freeSize)
    {
        expectVgsPropCommand(vgsCommand, "vg_free", volumeGroup, Long.toString(freeSize) + ".00");
    }

    protected void expectVgsPropCommand(
        final String vgsCommand,
        final String property,
        final String volumeGroup,
        final String extentSize)
    {
        Command command = new Command(
            vgsCommand,
            volumeGroup,
            "-o", property,
            "--units", "k",
            "--noheadings",
            "--nosuffix");
        OutputData outData = new TestOutputData("   "+extentSize, "", 0);
        ec.setExpectedBehavior(command, outData);
    }
}
