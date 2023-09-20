package com.linbit.extproc;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GenericExecutorTest
{
    GenericTimer<String, Action<String>> intrTimer;
    ErrorReporter errLog;

    @Before
    public void setUp()
    {
        intrTimer = new GenericTimer<>();
        intrTimer.start();
        errLog = new StderrErrorReporter("LINSTOR-UNITTESTS");
    }

    @Test
    public void testBasicExecute() throws StorageException
    {
        ExtCmd extCmd = new ExtCmd(intrTimer, errLog);
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]{
                "test-support/exitcode", "0"
            },
            "Failed execute exit command",
            "Failed to execute exit command",
            Commands.NO_RETRY
        );
    }

    @Test
    public void testIgnoreExitCodeExecute() throws StorageException
    {
        ExtCmd extCmd = new ExtCmd(intrTimer, errLog);
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]{
                "test-support/exitcode", "2"
            },
            "Failed execute exit command",
            "Failed to execute exit command",
            Commands.NO_RETRY,
            Collections.singletonList(2)
        );
        Assert.assertEquals(0, outputData.stdoutData.length);

        Commands.genericExecutor(
            extCmd,
            new String[]{
                "test-support/exitcode", "0"
            },
            "Failed execute exit command",
            "Failed to execute exit command",
            Commands.NO_RETRY,
            Collections.singletonList(2)
        );
    }

    @Test(expected = StorageException.class)
    public void testFailonExitCodeExecute() throws StorageException
    {
        ExtCmd extCmd = new ExtCmd(intrTimer, errLog);
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]{
                "test-support/exitcode", "3"
            },
            "Failed execute exit command",
            "Failed to execute exit command",
            Commands.NO_RETRY,
            Collections.singletonList(2)
        );
    }
}
