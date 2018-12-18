package com.linbit.linstor.storage;

//import static com.linbit.linstor.storage.LvmDriver.LVM_VOLUME_GROUP_DEFAULT;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
//import java.io.File;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.InvocationTargetException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//
//import com.linbit.linstor.timer.CoreTimer;
//import com.linbit.linstor.timer.CoreTimerImpl;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.rules.TemporaryFolder;
//import org.mockito.Mockito;
//import org.powermock.api.mockito.PowerMockito;
//import com.linbit.extproc.ExtCmd;
//import com.linbit.extproc.ExtCmd.OutputData;
//import com.linbit.extproc.utils.TestExtCmd;
//import com.linbit.extproc.utils.TestExtCmd.Command;
//import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
//import com.linbit.fsevent.EntryGroupObserver;
//import com.linbit.fsevent.FileObserver;
//import com.linbit.fsevent.FileSystemWatch;
//import com.linbit.fsevent.FileSystemWatch.Event;
//import com.linbit.fsevent.FileSystemWatch.FileEntry;
//import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
//import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;
//import com.linbit.linstor.core.StltConfigAccessor;
//import com.linbit.linstor.logging.ErrorReporter;
//import com.linbit.linstor.testutils.EmptyErrorReporter;
//
//public class StorageTestUtils
//{
//    public static final String LVM_EXT_CMD_SEPARATOR = ";";
//    private static final int START_VOL_NOT_EXISTS_ERROR_CODE = 5;
//    private static final int STOP_VOL_NOT_EXISTS_ERROR_CODE = 5;
//    private static final int DEL_VOL_EXISTS_ERROR_CODE = 5;
//    private static final int CREATE_VOL_EXISTS_ERROR_CODE = 5;
//    private static final int CHECK_VLM_GROUP_ERROR_CODE = 5;
//
//    protected final FileObserver emptyFileObserver;
//
//    @Rule
//    public TemporaryFolder tempFolder = new TemporaryFolder();
//
//    protected StorageDriver driver;
//    protected TestExtCmd ec;
//
//    private StorageDriverKind driverKind;
//
//
//    public StorageTestUtils(StorageDriverKind driverKindRef)
//        throws Exception
//    {
//        driverKind = driverKindRef;
//        emptyFileObserver = new FileObserver()
//        {
//            @Override
//            public void fileEvent(FileEntry watchEntry)
//            {
//            }
//        };
//        ec = new TestExtCmd();
//        PowerMockito
//            .whenNew(ExtCmd.class)
//            .withAnyArguments()
//            .thenReturn(ec);
//    }
//
//    @Before
//    public void setUp() throws Exception
//    {
//        ec.clearBehaviors();
//
//        ErrorReporter errRep = new EmptyErrorReporter();
//        CoreTimer timer = new CoreTimerImpl();
//        StltConfigAccessor mockedStltCfgAccessor = Mockito.mock(StltConfigAccessor.class);
//        Mockito.when(mockedStltCfgAccessor.useDmStats()).thenReturn(false);
//
//
//        driver = driverKind.makeStorageDriver(
//            errRep,
//            Mockito.mock(FileSystemWatch.class),
//            timer,
//            mockedStltCfgAccessor
//        );
//    }
//
//    @After
//    public void tearDown() throws Exception
//    {
//        StringBuilder sb = new StringBuilder();
//        HashSet<Command> uncalledCommands = ec.getUncalledCommands();
//        if (!uncalledCommands.isEmpty())
//        {
//            for (Command cmd : uncalledCommands)
//            {
//                sb.append(cmd).append("\n");
//            }
//            sb.setLength(sb.length() - 1);
//            fail("Not all expected commands were called: \n" + sb.toString());
//        }
//    }
//
//    protected void expectException(Map<String, String> config)
//    {
//        try
//        {
//            driver.setConfiguration("TestStorPool", config, Collections.emptyMap(), Collections.emptyMap());
//            fail(StorageException.class.getName() + " expected");
//        }
//        catch (StorageException exc)
//        {
//            // expected
//        }
//    }
//
//    protected static Map<String, String> createMap(String... strings)
//    {
//        HashMap<String, String> map = new HashMap<>();
//        int idx = 0;
//        while (idx < strings.length)
//        {
//            map.put(strings[idx], strings[idx + 1]);
//            idx += 2;
//        }
//        return map;
//    }
//
//    protected static Path findCommand(String command)
//    {
//        Path[] pathFolders = getPathFolders();
//        Path path = null;
//
//        if (pathFolders != null)
//        {
//            for (Path folder : pathFolders)
//            {
//                Path commandPath = folder.resolve(command);
//                if (Files.exists(commandPath) && Files.isExecutable(commandPath))
//                {
//                    path = commandPath;
//                    break;
//                }
//            }
//        }
//        return path;
//    }
//
//    protected static Path[] getPathFolders()
//    {
//        String path = System.getenv("PATH");
//        if (path == null)
//        {
//            path = System.getenv("path");
//        }
//        if (path == null)
//        {
//            path = System.getenv("Path");
//        }
//        Path[] folders = null;
//        if (path != null)
//        {
//            String[] split = path.split(File.pathSeparator);
//
//            folders = new Path[split.length];
//            for (int idx = 0; idx < split.length; idx++)
//            {
//                folders[idx] = Paths.get(split[idx]);
//            }
//        }
//        return folders;
//    }
//
//    protected static <T> T getInstance(Class<T> clazz, Object... parameters)
//        throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException,
//        IllegalArgumentException, InvocationTargetException
//    {
//        Class<?>[] parameterClasses = new Class[parameters.length];
//        for (int idx = 0; idx < parameters.length; idx++)
//        {
//            parameterClasses[idx] = parameters[idx].getClass();
//        }
//        Constructor<T> constructor = clazz.getDeclaredConstructor(parameterClasses);
//        boolean accessible = constructor.isAccessible();
//        constructor.setAccessible(true);
//        T ret = constructor.newInstance(parameters);
//        constructor.setAccessible(accessible);
//        return ret;
//    }
//
//    protected static FileEntryGroupBuilder getTestFileEntryGroupBuilder(
//        final String expectedFilePath,
//        final Event expectedEvent,
//        final FileEntryGroup testFileEntryGroup
//    )
//    {
//        return new FileEntryGroupBuilder()
//        {
//            @Override
//            public void newEntry(String filePath, Event event)
//            {
//                assertEquals(expectedFilePath, filePath);
//                assertEquals(expectedEvent, event);
//            }
//
//            @Override
//            public FileEntryGroup create(EntryGroupObserver observer)
//            {
//                return testFileEntryGroup;
//            }
//        };
//    }
//
//
//    protected void expectStartVolumeCommand(
//        final String lvmChangeDefault,
//        final String lvmVolumeGroupDefault,
//        final String identifier,
//        final boolean volumeExists)
//    {
//        final Command command = new Command(
//            lvmChangeDefault,
//            "-ay",
//            "-K",
//            lvmVolumeGroupDefault + "/" + identifier);
//
//        final OutputData outData;
//        if (volumeExists)
//        {
//            outData = new TestOutputData(command.getRawCommand(), "", "", 0);
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "One or more specified logical volume(s, retCode) not found.",
//                START_VOL_NOT_EXISTS_ERROR_CODE
//            );
//        }
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectStopVolumeCommand(
//        final String lvmChangeDefault,
//        final String lvmVolumeGroupDefault,
//        final String identifier,
//        final boolean volumeExists)
//    {
//        final Command command = new Command(
//            lvmChangeDefault,
//            "-an",
//            lvmVolumeGroupDefault + "/" + identifier);
//
//        final OutputData outData;
//        if (volumeExists)
//        {
//            outData = new TestOutputData(command.getRawCommand(), "", "", 0);
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "One or more specified logical volume(s, retCode) not found.",
//                STOP_VOL_NOT_EXISTS_ERROR_CODE
//            );
//        }
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectLvmDeleteVolumeBehavior(
//        final String lvmRemoveCommand,
//        final String identifier,
//        final String volumeGroup,
//        final boolean volumeExists)
//    {
//        final Command cmd = new Command(
//            lvmRemoveCommand,
//            "-f",
//            volumeGroup + File.separator + identifier);
//
//        final OutputData outData;
//        if (volumeExists)
//        {
//            outData = new TestOutputData(
//                cmd.getRawCommand(),
//                "Logical volume \"" + identifier + "\" successfully removed",
//                "",
//                0);
//        }
//        else
//        {
//            outData = new TestOutputData(
//                cmd.getRawCommand(),
//                "",
//                "One or more specified logical volume(s) not found.",
//                DEL_VOL_EXISTS_ERROR_CODE
//            );
//        }
//        ec.setExpectedBehavior(cmd, outData);
//    }
//
//    protected void expectLvmCreateVolumeBehavior(
//        final String lvmCreateCommand,
//        final long volumeSize,
//        final String identifier,
//        final String volumeGroup,
//        final boolean volumeExists)
//    {
//        final Command cmd = new Command(
//            lvmCreateCommand,
//            "--size", volumeSize + "k",
//            "-n", identifier,
//            "-y",
//            volumeGroup
//            );
//
//        final OutputData outData;
//        if (volumeExists)
//        {
//            outData = new TestOutputData(
//                cmd.getRawCommand(),
//                "",
//                "Logical volume \"" + identifier + "\" already exists in volume group \"" + volumeGroup + "\"",
//                CREATE_VOL_EXISTS_ERROR_CODE
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                cmd.getRawCommand(),
//                "Logical volume \"identifier\" created",
//                "",
//                0);
//        }
//
//        ec.setExpectedBehavior(cmd, outData);
//    }
//
//    protected void expectCheckVolumeGroup(final String vgsCommand, final String volumeGroup)
//    {
//        expectCheckVolumeGroup(vgsCommand, volumeGroup, true);
//    }
//
//    protected void expectCheckVolumeGroup(final String vgsCommand, final String volumeGroup, final boolean success)
//    {
//        Command command = new Command(
//            vgsCommand,
//            "-o", "vg_name",
//            "--noheadings"
//            );
//        OutputData outData;
//        if (success)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "   " + volumeGroup,
//                "",
//                0);
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "  Volume groug \"" + volumeGroup + "\" not found",
//                CHECK_VLM_GROUP_ERROR_CODE
//            );
//        }
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectLvsInfoBehavior(
//        final String lvsCommand,
//        final String volumeGroup,
//        final String identifier,
//        final long volumeSize)
//    {
//        expectLvsInfoBehavior(lvsCommand, volumeGroup, identifier, Long.toString(volumeSize) + ".00");
//    }
//
//    protected void expectLvsInfoBehavior(
//        final String lvsCommand,
//        final String volumeGroup,
//        final String identifier,
//        final String volumeSize
//    )
//    {
//        expectLvsInfoBehavior(lvsCommand, volumeGroup, identifier, volumeSize, true);
//    }
//
//    protected void expectLvsInfoBehavior(
//        final String lvsCommand,
//        final String volumeGroup,
//        final String identifier,
//        final String volumeSize,
//        final boolean exists
//    )
//    {
//        Command command = new Command(
//            lvsCommand,
//            "-o", "lv_name,lv_path,lv_size",
//            "--separator", LVM_EXT_CMD_SEPARATOR,
//            "--noheadings",
//            "--units", "k",
//            "--nosuffix",
//            volumeGroup);
//        StringBuilder sb = new StringBuilder();
//        OutputData outData;
//        if (exists)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                sb
//                .append(identifier)
//                .append(LVM_EXT_CMD_SEPARATOR)
//                .append("/dev/").append(LVM_VOLUME_GROUP_DEFAULT).append("/").append(identifier)
//                .append(LVM_EXT_CMD_SEPARATOR)
//                .append(volumeSize).toString(),
//                "",
//                0
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(command.getRawCommand(), "", "", 0);
//        }
//
//        ec.setExpectedBehavior(command, outData);
//
//    }
//
//    protected void expectVgsExtentCommand(final String vgsCommand, final String volumeGroup, final long extentSize)
//    {
//        expectVgsExtentCommand(vgsCommand, volumeGroup, Long.toString(extentSize) + ".00");
//    }
//
//    protected void expectVgsExtentCommand(
//        final String vgsCommand,
//        final String volumeGroup,
//        final String extentSize)
//    {
//        expectVgsPropCommand(vgsCommand, "vg_extent_size", volumeGroup, extentSize);
//    }
//
//    protected void expectVgsTotalSpaceCommand(final String vgsCommand, final String volumeGroup, final long totalSpace)
//    {
//        expectVgsPropCommand(vgsCommand, "vg_size", volumeGroup, Long.toString(totalSpace) + ".00");
//    }
//
//    protected void expectVgsFreeSpaceCommand(final String vgsCommand, final String volumeGroup, final long freeSpace)
//    {
//        expectVgsPropCommand(vgsCommand, "vg_free", volumeGroup, Long.toString(freeSpace) + ".00");
//    }
//
//    protected void expectVgsPropCommand(
//        final String vgsCommand,
//        final String property,
//        final String volumeGroup,
//        final String extentSize)
//    {
//        Command command = new Command(
//            vgsCommand,
//            volumeGroup,
//            "-o", property,
//            "--units", "k",
//            "--noheadings",
//            "--nosuffix");
//        OutputData outData = new TestOutputData(command.getRawCommand(), "   " + extentSize, "", 0);
//        ec.setExpectedBehavior(command, outData);
//    }
//}
