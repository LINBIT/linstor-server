package com.linbit.drbdmanage.storage;

import static com.linbit.drbdmanage.storage.LvmConstants.*;
import static com.linbit.drbdmanage.storage.LvmThinDriver.LVM_VGS_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmThinDriver.LVM_LVS_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmThinDriver.LVM_VOLUME_GROUP_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmThinDriver.LVM_CHANGE_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmThinDriver.LVM_CREATE_DEFAULT;
import static com.linbit.drbdmanage.storage.LvmThinDriver.LVM_REMOVE_DEFAULT;
import static com.linbit.extproc.utils.TestExtCmd.Command;
import static com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.linbit.drbd.md.MetaData;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.fsevent.EntryGroupObserver;
import com.linbit.fsevent.FileObserver;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LvmThinDriver.class)
public class LvmThinDriverTest
{
    private static final String EXT_COMMAND_SEPARATOR = ",";

    private final FileObserver emptyFileObserver;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private LvmThinDriver driver;
    private TestExtCmd ec;


    public LvmThinDriverTest()
    {
        emptyFileObserver = new FileObserver()
        {
            @Override
            public void fileEvent(FileEntry watchEntry)
            {
            }
        };
    }

    @Before
    public void setUp() throws Exception
    {
        ec = new TestExtCmd();
        driver = new LvmThinDriver(ec);
    }

    @After
    public void tearDown() throws Exception
    {
        StringBuilder sb = new StringBuilder();
        HashSet<Command> uncalledCommands = ec.getUncalledCommands();
        if (!uncalledCommands.isEmpty())
        {
            for (Command cmd : uncalledCommands)
            {
                sb.append(cmd).append("\n");
            }
            sb.setLength(sb.length()-1);
            fail("Not all expected commands were called: \n"+sb.toString());
        }
    }

    @Test
    public void testConfigVolumeGroup() throws StorageException
    {
        final HashMap<String,String> config = new HashMap<>();

        String volumeGroup = "otherName";
        config.put(CONFIG_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
        driver.setConfiguration(config);

        ec.clearBehaviors();
        volumeGroup = "_specialName";
        config.put(CONFIG_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
        driver.setConfiguration(config);

        ec.clearBehaviors();
        volumeGroup = "special-Name";
        config.put(CONFIG_VOLUME_GROUP_KEY, volumeGroup);
        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
        driver.setConfiguration(config);
    }

    @Test(expected = StorageException.class)
    public void testConfigVolumeGroupEmpty() throws StorageException
    {
        driver.setConfiguration(createMap(CONFIG_VOLUME_GROUP_KEY, ""));
    }

    @Test(expected = StorageException.class)
    public void testConfigVolumeGroupWhitespacesOnly() throws StorageException
    {
        driver.setConfiguration(createMap(CONFIG_VOLUME_GROUP_KEY, "  "));
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
        tmpFile.delete();
        Files.copy(findCommand(LVM_VGS_DEFAULT), tmpFile.toPath());
        tmpFile.setExecutable(true);
        driver.setConfiguration(createMap(CONFIG_LVM_VGS_COMMAND_KEY, tmpFile.getAbsolutePath()));

        String volumeGroup = "newVolumeGroup";
        expectCheckVolumeGroup(tmpFile.getAbsolutePath(),volumeGroup);
        driver.setConfiguration(createMap(CONFIG_VOLUME_GROUP_KEY, volumeGroup));
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
        expectStartVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, true);
        driver.startVolume(identifier);
    }

    @Test(expected = StorageException.class)
    public void testStartUnknownVolume() throws StorageException
    {
        final String unknownIdentifier = "unknown";
        expectStartVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, unknownIdentifier, false);
        driver.startVolume(unknownIdentifier);
    }

    @Test
    public void testStopVolume() throws StorageException
    {
        final String identifier = "identifier";
        expectStopVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, true);
        driver.stopVolume(identifier);
    }

    @Test(expected = StorageException.class)
    public void testStopUnknownVolume() throws StorageException
    {
        final String unknownIdentifier = "unknown";
        expectStopVolumeCommand(LVM_CHANGE_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, unknownIdentifier, false);
        driver.stopVolume(unknownIdentifier);
    }

    @Test
    public void testCreateVolumeDelayed() throws Exception
    {
        final long volumeSize = 102400;
        final String volumeName = "testVolume";
        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, volumeName, Long.toString(volumeSize)+".00k");
        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, volumeName, LVM_VOLUME_GROUP_DEFAULT, false);

        final String expectedFilePath = "/dev/"+LVM_VOLUME_GROUP_DEFAULT + "/"+volumeName;
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
                        Thread.sleep(100); // give the driver some time to execute the .createVolume command
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
        driver.createVolume(volumeName, volumeSize);
    }

    @Test
    public void testCreateVolumeInstant() throws Exception
    {
        final long volumeSize = 102400;
        final String volumeName = "testVolume";

        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, volumeName, LVM_VOLUME_GROUP_DEFAULT, false);
        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, volumeName, volumeSize);

        final String expectedFilePath = "/dev/"+LVM_VOLUME_GROUP_DEFAULT + "/"+volumeName;
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.CREATE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        final FileEntry testEntry = new FileEntry(
            Paths.get(expectedFilePath),
            Event.CREATE,
            emptyFileObserver);

        testFileEntryGroup.fileEvent(testEntry);
        driver.createVolume(volumeName, volumeSize);
    }

    @Test(expected = StorageException.class)
    public void testCreateVolumeTimeout() throws Exception
    {
        final long volumeSize = 102400;
        final String volumeName = "testVolume";

        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, volumeName, LVM_VOLUME_GROUP_DEFAULT, false);

        final String expectedFilePath = "/dev/"+LVM_VOLUME_GROUP_DEFAULT + "/"+volumeName;
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.CREATE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        // do not fire file event
        driver.createVolume(volumeName, volumeSize);
    }


    @Test(expected = StorageException.class)
    public void testCreateExistingVolume() throws Exception
    {
        final long volumeSize = 102400;
        final String volumeName = "testVolume";

        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, volumeName, LVM_VOLUME_GROUP_DEFAULT, true);
        driver.createVolume(volumeName, volumeSize);
    }



    @Test
    public void testDeleteVolume() throws Exception
    {
        final String volumeName = "testVolume";

        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, volumeName, LVM_VOLUME_GROUP_DEFAULT, true);

        final String expectedFilePath = "/dev/"+LVM_VOLUME_GROUP_DEFAULT + "/"+volumeName;
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.DELETE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        final FileEntry testEntry = new FileEntry(
            Paths.get(expectedFilePath),
            Event.DELETE,
            emptyFileObserver);

        testFileEntryGroup.fileEvent(testEntry);
        driver.deleteVolume(volumeName);
    }

    @Test(expected = StorageException.class)
    public void testDeleteVolumeTimeout() throws Exception
    {
        final String volumeName = "testVolume";

        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, volumeName, LVM_VOLUME_GROUP_DEFAULT, true);

        final String expectedFilePath = "/dev/"+LVM_VOLUME_GROUP_DEFAULT + "/"+volumeName;
        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);

        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(expectedFilePath, Event.DELETE, testFileEntryGroup);
        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);

        // do not fire file event
        driver.deleteVolume(volumeName);
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
        final long extentSize = 4_096;

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, size);
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, extentSize);

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
        final long volumeSize = 102_400;

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);

        final String path = driver.getVolumePath(identifier);
        assertEquals(
            File.separator +
            "dev" + File.separator+
            LVM_VOLUME_GROUP_DEFAULT + File.separator +
            identifier,
            path);
    }

    @Test(expected = StorageException.class)
    public void testVolumePathUnknownVolume() throws StorageException
    {
        final String identifier = "testVolume";
        final long volumeSize = 102_400;

        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);

        driver.getVolumePath("otherVolume");
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
        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, 4096);
        Map<String, String> traits = driver.getTraits();

        final String traitProv = traits.get(DriverTraits.KEY_PROV);
        assertEquals(DriverTraits.PROV_THIN, traitProv);

        final String size = traits.get(DriverTraits.KEY_ALLOC_UNIT);
        assertEquals("4096", size);
    }

    @Test
    public void testConfigurationKeys()
    {
        final HashSet<String> keys = new HashSet<>(driver.getConfigurationKeys());

        assertTrue(keys.remove(LvmConstants.CONFIG_LVM_CREATE_COMMAND_KEY));
        assertTrue(keys.remove(LvmConstants.CONFIG_LVM_REMOVE_COMMAND_KEY));
        assertTrue(keys.remove(LvmConstants.CONFIG_LVM_CHANGE_COMMAND_KEY));
        assertTrue(keys.remove(LvmConstants.CONFIG_LVM_LVS_COMMAND_KEY));
        assertTrue(keys.remove(LvmConstants.CONFIG_LVM_VGS_COMMAND_KEY));
        assertTrue(keys.remove(LvmConstants.CONFIG_VOLUME_GROUP_KEY));
        assertTrue(keys.remove(LvmConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY));

        assertTrue(keys.isEmpty());
    }

    private void expectStartVolumeCommand(
        final String lvmChangeDefault,
        final String lvmVolumeGroupDefault,
        final String identifier,
        final boolean volumeExists)
    {
        final Command command = new Command(
            LVM_CHANGE_DEFAULT,
            "-ay",
            LVM_VOLUME_GROUP_DEFAULT + "/" + identifier);

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

    private void expectStopVolumeCommand(
        final String lvmChangeDefault,
        final String lvmVolumeGroupDefault,
        final String identifier,
        final boolean volumeExists)
    {
        final Command command = new Command(
            LVM_CHANGE_DEFAULT,
            "-an",
            LVM_VOLUME_GROUP_DEFAULT + "/" + identifier);

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

    private void expectLvmDeleteVolumeBehavior(
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
                "Logical volume \"" + identifier + "\" successuflly removed",
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

    private void expectLvmCreateVolumeBehavior(
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

    private FileEntryGroupBuilder getTestFileEntryGroupBuilder(final String expectedFilePath, final Event expectedEvent, final FileEntryGroup testFileEntryGroup)
    {
        return new FileEntryGroupBuilder()
        {
            @Override
            public void newEntry(String filePath, Event event)
            {
                assertEquals(expectedFilePath, filePath);
                assertEquals(expectedEvent, event);
            }

            @Override
            public FileEntryGroup create(FileSystemWatch watch, EntryGroupObserver observer) throws IOException
            {
                return testFileEntryGroup;
            }
        };
    }

    private void expectCheckVolumeGroup(final String vgsCommand, final String volumeGroup)
    {
        Command command = new Command(
            vgsCommand,
            "-o", "vg_name",
            "--noheadings"
            );
        OutputData outData = new TestOutputData(
            "   " + volumeGroup,
            "",
            0);
        ec.setExpectedBehavior(command, outData);
    }

    private void expectLvsInfoBehavior(
        final String lvsCommand,
        final String volumeGroup,
        final String identifier,
        final long volumeSize)
    {
        expectLvsInfoBehavior(lvsCommand, volumeGroup, identifier, Long.toString(volumeSize)+".00k");
    }

    private void expectLvsInfoBehavior(
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

    private void expectVgsExtentCommand(final String vgsCommand, final String volumeGroup, final long extentSize)
    {
        expectVgsExtentCommand(vgsCommand, volumeGroup, Long.toString(extentSize) + ".00k");
    }

    private void expectVgsExtentCommand(
        final String vgsCommand,
        final String volumeGroup,
        final String extentSize)
    {
        Command command = new Command(
            vgsCommand,
            volumeGroup,
            "-o", "vg_extent_size",
            "--units", "k",
            "--noheadings");
        OutputData outData = new TestOutputData(extentSize, "", 0);
        ec.setExpectedBehavior(command, outData);
    }


    private void expectException(Map<String, String> config)
    {
        try
        {
            driver.setConfiguration(config);
            fail(StorageException.class.getName() + " expected");
        }
        catch (StorageException e)
        {
            // expected
        }
    }

    private Map<String, String> createMap(String... strings)
    {
        HashMap<String, String> map = new HashMap<>();
        int idx = 0;
        while (idx < strings.length)
        {
            map.put(strings[idx], strings[idx+1]);
            idx += 2;
        }
        return map;
    }

    private Path findCommand(String command)
    {
        Path[] pathFolders = getPathFolders();
        Path path = null;

        if (pathFolders != null)
        {
            for (Path folder : pathFolders)
            {
                Path commandPath = folder.resolve(command);
                if (Files.exists(commandPath) && Files.isExecutable(commandPath))
                {
                    path = commandPath;
                    break;
                }
            }
        }
        return path;
    }


    private Path[] getPathFolders()
    {
        String path = System.getenv("PATH");
        if (path == null)
        {
            path = System.getenv("path");
        }
        if (path == null)
        {
            path = System.getenv("Path");
        }
        Path[] folders = null;
        if (path != null)
        {
            String[] split = path.split(File.pathSeparator);

            folders = new Path[split.length];
            for (int i = 0; i < split.length; i++)
            {
                folders[i] = Paths.get(split[i]);
            }
        }
        return folders;
    }

    private <T> T getInstance(Class<T> clazz, Object... parameters) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
        Class<?>[] parameterClasses = new Class[parameters.length];
        for (int i = 0; i < parameters.length; i++)
        {
            parameterClasses[i] = parameters[i].getClass();
        }
        Constructor<T> constructor = clazz.getDeclaredConstructor(parameterClasses);
        boolean accessible = constructor.isAccessible();
        constructor.setAccessible(true);
        T ret = constructor.newInstance(parameters);
        constructor.setAccessible(accessible);
        return ret;
    }
}
