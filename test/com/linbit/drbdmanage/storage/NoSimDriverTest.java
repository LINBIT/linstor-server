package com.linbit.drbdmanage.storage;

import java.io.IOException;

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

    protected String testIdentifier = null;

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
        log("initializing... %n");
        initialize();


        log("requesting unused identifier... ");
        String identifier = getUnusedIdentifier();
        this.testIdentifier = identifier; // used for cleanup
        log("using identifier: [%s] %n", identifier);

        int size = 100*1024;
        log("creating volume [%s] with size [%d]... ", identifier, size);
        driver.createVolume(identifier, size);
        failIf(!volumeExists(identifier), "Failed to create volume [%s]", identifier);
        log(" done %n");

        try
        {
            log("checking is volume exists...");
            driver.createVolume(identifier, size);
            fail("Creating an existing volume [%s] should have thrown an exception", identifier);
        }
        catch (StorageException expected)
        {
            log(" done %n");
        }

        log("checking volume [%s], with size [%d]...", identifier, size);
        driver.checkVolume(identifier, size);
        log(" done %n");
        log("checking volume [%s], with size [%d]...", identifier, size - 1);
        driver.checkVolume(identifier, size - 1); // should be in tolerance range
        log(" done %n");
        try
        {
            log("test checking volume [%s], with size [%d] (too low)...", identifier, size / 2);
            driver.checkVolume(identifier, size / 2);
            fail("checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            log(" done %n");
        }
        try
        {
            log("test checking volume [%s], with size [%d] (too high)...", identifier, size + 1);
            driver.checkVolume(identifier, size + 1);
            fail("checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            log(" done %n");
        }


        log("getting size of volume [%s]... ", identifier);
        long driverSize = driver.getSize(identifier);
        failIf(driverSize != size, "Expected size [%d] but was [%d]", size, driverSize);
        log(" done %n");

        log("getting path of volume [%s]...", identifier);
        String volumePath = driver.getVolumePath(identifier);
        String expectedVolumePath = getVolumePath(identifier);
        failIf(!expectedVolumePath.equals(volumePath), "Unexpected volume path: [%s], expected: [%s]", volumePath, expectedVolumePath);
        log(" done %n");

        if (isVolumeStartStopSupported())
        {
            log("stopping volume [%s]...", identifier);
            driver.stopVolume(identifier);
            failIf(isVolumeStarted(identifier), "Volume [%s] failed to stop", identifier);
            log(" done %n");

            log("starting volume [%s]...", identifier);
            driver.startVolume(identifier);
            failIf(!isVolumeStarted(identifier), "Volume [%s] failed to start", identifier);
            log(" done %n");
        }

        log("deleting volume [%s]...", identifier);
        driver.deleteVolume(identifier);
        failIf(volumeExists(identifier), "Failed to delete volume [%s]", identifier);
        log(" done %n");

        try
        {
            log("test deleting already deleted volume [%s]...", identifier);
            driver.deleteVolume(identifier);
            fail("Deleting non existent volume [%s] should have failed", identifier);
        }
        catch(StorageException expected)
        {
            log(" done %n");
        }
        log("all tests done - no errors found %n%n%n");
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
            cleanUp();
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

    protected abstract void initialize() throws ChildProcessTimeoutException, IOException;

    protected abstract String getUnusedIdentifier() throws ChildProcessTimeoutException, IOException;

    protected abstract boolean volumeExists(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract String getVolumePath(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract boolean isVolumeStartStopSupported();

    protected abstract boolean isVolumeStarted(String identifier);

    protected abstract void cleanUp() throws ChildProcessTimeoutException, IOException;

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
