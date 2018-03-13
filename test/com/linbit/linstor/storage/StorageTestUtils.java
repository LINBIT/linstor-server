package com.linbit.linstor.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.fsevent.EntryGroupObserver;
import com.linbit.fsevent.FileObserver;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.testutils.EmptyErrorReporter;

public class StorageTestUtils
{
    protected final FileObserver emptyFileObserver;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    protected StorageDriver driver;
    protected TestExtCmd ec;

    private StorageDriverKind driverKind;

    public StorageTestUtils(StorageDriverKind driverKindRef) throws Exception
    {
        driverKind = driverKindRef;
        emptyFileObserver = new FileObserver()
        {
            @Override
            public void fileEvent(FileEntry watchEntry)
            {
            }
        };
        ec = new TestExtCmd();
        PowerMockito
            .whenNew(ExtCmd.class)
            .withAnyArguments()
            .thenReturn(ec);
    }

    @Before
    public void setUp() throws Exception
    {
        ec.clearBehaviors();

        ErrorReporter errRep = new EmptyErrorReporter();
        CoreTimer timer = new CoreTimerImpl();

        driver = driverKind.makeStorageDriver(
            errRep,
            Mockito.mock(FileSystemWatch.class),
            timer
        );
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
            sb.setLength(sb.length() - 1);
            fail("Not all expected commands were called: \n" + sb.toString());
        }
    }

    protected void expectException(Map<String, String> config)
    {
        try
        {
            driver.setConfiguration(config);
            fail(StorageException.class.getName() + " expected");
        }
        catch (StorageException exc)
        {
            // expected
        }
    }

    protected static Map<String, String> createMap(String... strings)
    {
        HashMap<String, String> map = new HashMap<>();
        int idx = 0;
        while (idx < strings.length)
        {
            map.put(strings[idx], strings[idx + 1]);
            idx += 2;
        }
        return map;
    }

    protected static Path findCommand(String command)
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

    protected static Path[] getPathFolders()
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
            for (int idx = 0; idx < split.length; idx++)
            {
                folders[idx] = Paths.get(split[idx]);
            }
        }
        return folders;
    }

    protected static <T> T getInstance(Class<T> clazz, Object... parameters)
        throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException,
        IllegalArgumentException, InvocationTargetException
    {
        Class<?>[] parameterClasses = new Class[parameters.length];
        for (int idx = 0; idx < parameters.length; idx++)
        {
            parameterClasses[idx] = parameters[idx].getClass();
        }
        Constructor<T> constructor = clazz.getDeclaredConstructor(parameterClasses);
        boolean accessible = constructor.isAccessible();
        constructor.setAccessible(true);
        T ret = constructor.newInstance(parameters);
        constructor.setAccessible(accessible);
        return ret;
    }

    protected static FileEntryGroupBuilder getTestFileEntryGroupBuilder(
        final String expectedFilePath,
        final Event expectedEvent,
        final FileEntryGroup testFileEntryGroup
    )
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
            public FileEntryGroup create(EntryGroupObserver observer)
            {
                return testFileEntryGroup;
            }
        };
    }
}
