package com.linbit.drbdmanage.storage;

import java.io.IOException;
import java.util.ArrayList;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.ErrorReporter;
import com.linbit.drbdmanage.debug.DebugErrorReporter;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;
import com.linbit.timer.Timer;

/**
 * Although this class is in the test folder, this class is
 *
 * NOT A JUNIT TEST.
 *
 * The {@link LvmDriverTest}, {@link LvmThinDriverTest} and {@link ZfsDriverTest}
 * are JUnit tests, which simulates every external IO. However, this class will
 * not simulate anything, thus really executing all commands on the underlying system.
 * Because of this, we need to ensure the order of the "tests", which
 * is not possible with JUnit.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public abstract class NoSimDriverTest
{
    protected boolean log = true;

    protected StorageDriver driver;
    protected ExtCmd extCommand;

    protected ArrayList<String> testIdentifiers = new ArrayList<>();

    protected boolean inCleanup = false;

    public NoSimDriverTest(StorageDriver driver) throws IOException, StorageException
    {
        this.driver = driver;
        CoreServices coreSvc = new TestCoreServices();
        extCommand = new ExtCmd(coreSvc.getTimer());
        driver.initialize(coreSvc);
    }

    protected void runTest() throws StorageException, MaxSizeException, MinSizeException, ChildProcessTimeoutException, IOException
    {
        initialize();

        String identifier = getUnusedIdentifier();

        long size = 100*1024;
        createVolume(identifier, size, "creating volume [%s] with size [%d]... ");

        try
        {
            createVolume(identifier, size, "trying to create existing volume [%s]");
            fail("Creating an existing volume [%s] should have thrown an exception");
        }
        catch (StorageException expected)
        {
            // expected
            log(" done%n");
        }

        checkVolume(identifier, size, "checking volume [%s], with size [%d]...");
        checkVolume(identifier, size - 1, "checking volume [%s], with size [%d] (should be tolerated)...");
        try
        {
            checkVolume(identifier, size / 2, "test checking volume [%s], with size [%d] (too low)...");
            fail("checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            // expected
            log(" done%n");
        }
        try
        {
            checkVolume(identifier, size + 1, "test checking volume [%s], with size [%d] (too high)...");
            fail("checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            // expected
            log(" done%n");
        }

        String identifier2 = getUnusedIdentifier();
        long unalignedSize = 99*1024 + 1;// should be aligned up to 100 * 1024, aka [size]
        createVolume(identifier2, unalignedSize, "creating volume [%s] with unaligned size [%d]");
        checkVolume(identifier2, unalignedSize, "checking volume [%s], with unaligned size [%d]...");
        checkVolume(identifier2, size, "checking volume [%s], with aligned size [%d]...");

        checkSize(identifier, size, "getting size of volume [%s]");

        checkPath(identifier, "getting path of volume [%s]...");

        if (isVolumeStartStopSupportedImpl())
        {
            stopVolume(identifier, "stopping volume [%s]...");
            startVolume(identifier, "starting volume [%s]...");
        }

        deleteVolume(identifier, "deleting volume [%s]...");

        try
        {
            deleteVolume(identifier, "test deleting already deleted volume [%s]...");
        }
        catch(StorageException expected)
        {
            // expected
            log(" done%n");
        }
        log("%n%nall tests done - no errors found %n%n%n");
    }

    protected final void failIf(boolean condition, String format, Object...args) throws ChildProcessTimeoutException, IOException
    {
        if(condition)
        {
            fail(format, args);
        }
    }

    protected final void fail(String format, Object... args) throws ChildProcessTimeoutException, IOException
    {
        if (!inCleanup) // prevent endless recursion
        {
            cleanUpImpl();
        }
        throw new TestFailedException(String.format(format, args));
    }

    protected OutputData callChecked(String... command) throws ChildProcessTimeoutException, IOException
    {
        OutputData exec = extCommand.exec(command);
        if (exec.exitCode != 0)
        {
            StringBuilder sb = new StringBuilder();
            for (String cmd : command)
            {
                sb.append(cmd).append(" ");
            }
            System.err.println("Could not execute: " + sb.toString());
            System.err.println(new String(exec.stderrData));
            fail("Failed to execute command [%s]", sb.toString());
        }
        return exec;
    }

    protected void log(String format, Object...args)
    {
        if (log)
        {
            System.out.format(format, args);
        }
    }

    private void initialize() throws ChildProcessTimeoutException, IOException
    {
        log("initializing... %n");
        initializeImpl();
        log("initialized %n");
    }

    private String getUnusedIdentifier() throws ChildProcessTimeoutException, IOException
    {
        log("requesting unused identifier... ");
        String identifier = getUnusedIdentifierImpl();
        this.testIdentifiers.add(identifier); // used for cleanup
        log("using identifier: [%s] %n", identifier);
        return identifier;
    }

    private void createVolume(String identifier, long size, String format) throws MaxSizeException, MinSizeException, StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier, size);
        driver.createVolume(identifier, size);
        failIf(!volumeExistsImpl(identifier), "Failed to create volume [%s]", identifier);
        log(" done %n");
    }

    private void checkVolume(String identifier, long size, String format) throws StorageException
    {
        log(format, identifier, size);
        driver.checkVolume(identifier, size);
        log(" done %n");
    }

    private void checkSize(String identifier, long expectedSize, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        long driverSize = driver.getSize(identifier);
        failIf(driverSize != expectedSize, "expected size [%d] but was [%d]", expectedSize, driverSize);
        log(" done %n");
    }

    private void checkPath(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        String volumePath = driver.getVolumePath(identifier);
        String expectedVolumePath = getVolumePathImpl(identifier);
        failIf(!expectedVolumePath.equals(volumePath), "unexpected volume path: [%s], expected: [%s]", volumePath, expectedVolumePath);
        log(" done %n");
    }

    private void stopVolume(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.stopVolume(identifier);
        failIf(isVolumeStartedImpl(identifier), "volume [%s] failed to stop", identifier);
        log(" done %n");
    }

    private void startVolume(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.startVolume(identifier);
        failIf(!isVolumeStartedImpl(identifier), "volume [%s] failed to start", identifier);
        log(" done %n");
    }

    private void deleteVolume(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.deleteVolume(identifier);
        failIf(volumeExistsImpl(identifier), "Failed to delete volume [%s]", identifier);
        log(" done %n");
    }

    protected abstract void initializeImpl() throws ChildProcessTimeoutException, IOException;

    protected abstract String getUnusedIdentifierImpl() throws ChildProcessTimeoutException, IOException;

    protected abstract boolean volumeExistsImpl(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract String getVolumePathImpl(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract boolean isVolumeStartStopSupportedImpl();

    protected abstract boolean isVolumeStartedImpl(String identifier);

    protected abstract void cleanUpImpl() throws ChildProcessTimeoutException, IOException;

    private static class TestCoreServices implements CoreServices
    {
        private final GenericTimer<String, Action<String>> timerEventSvc ;
        private final FileSystemWatch fsEventSvc;
        private DebugErrorReporter errorReporter;

        public TestCoreServices() throws IOException
        {
            timerEventSvc = new GenericTimer<>();
            fsEventSvc = new FileSystemWatch();
            errorReporter = new DebugErrorReporter(System.err);
        }

        @Override
        public ErrorReporter getErrorReporter()
        {
            return errorReporter;
        }

        @Override
        public Timer<String, Action<String>> getTimer()
        {
            return timerEventSvc;
        }

        @Override
        public FileSystemWatch getFsWatch()
        {
            return fsEventSvc;
        }
    }

    protected static class TestFailedException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        public TestFailedException()
        {
            super();
        }

        public TestFailedException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public TestFailedException(String message)
        {
            super(message);
        }

        public TestFailedException(Throwable cause)
        {
            super(cause);
        }
    }
}
