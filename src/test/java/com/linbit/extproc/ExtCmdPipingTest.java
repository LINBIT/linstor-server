package com.linbit.extproc;

import static org.junit.Assert.fail;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test external command execution and piping the output of external commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ExtCmdPipingTest
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

    @After
    public void tearDown()
    {
        intrTimer.shutdown(false);
    }

    /**
     * Tests synchronous execution of a command with data received from stdout
     */
    @Test
    public void execTest() throws Exception
    {
        final int expectedRc = 42;
        final int expectedLen = 12345;
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ExtCmd.OutputData output = ec.exec(
            "test-support/TestOutput",
            Integer.toString(expectedRc),
            Integer.toString(expectedLen),
            "stdout",
            "exit"
        );

        if (output.stdoutData.length != expectedLen)
        {
            fail(String.format("Unexpected stdoutData length %d", output.stdoutData.length));
        }
        if (output.exitCode != expectedRc)
        {
            fail(String.format("Unexpected exit code %d", output.exitCode));
        }
    }

    /**
     * Tests synchronous execution of a command with data received from stderr
     */
    @Test
    public void execStderrTest() throws Exception
    {
        final int expectedRc = 21;
        final int expectedLen = 76543;
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ExtCmd.OutputData output = ec.exec(
            "test-support/TestOutput",
            Integer.toString(expectedRc),
            Integer.toString(expectedLen),
            "stderr",
            "exit"
        );

        if (output.stderrData.length != expectedLen)
        {
            fail(String.format("Unexpected stderrData length %d", output.stderrData.length));
        }
        if (output.exitCode != expectedRc)
        {
            fail(String.format("Unexpected exit code %d", output.exitCode));
        }
    }

    /**
     * Tests asynchronous execution of a command with data received from stdout
     */
    @Test
    public void asyncExecTest() throws Exception
    {
        final int expectedRc = 23;
        final int expectedLen = 3456;
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ec.asyncExec(
            "test-support/TestOutput",
            Integer.toString(expectedRc),
            Integer.toString(expectedLen),
            "stdout",
            "exit"
        );

        // TODO: run something else while the external process is working

        ExtCmd.OutputData output = ec.syncProcess();
        if (output.stdoutData.length != expectedLen)
        {
            fail(String.format("Unexpected stdoutData length %d", output.stdoutData.length));
        }
        if (output.exitCode != expectedRc)
        {
            fail(String.format("Unexpected exit code %d", output.exitCode));
        }
    }

    /**
     * Tests synchronous execution of a command that returns too much data on stdout
     */
    @Test(expected = java.io.IOException.class)
    public void execTooMuchDataTest() throws Exception
    {
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ExtCmd.OutputData output = ec.exec("test-support/TestOutput", "42", "6350750", "stdout", "exit");

        if (output.stdoutData.length > OutputReceiver.MAX_DATA_SIZE)
        {
            fail(
                String.format(
                    "OutputReceiver data size limitation does not work, read %d bytes",
                    output.stdoutData.length
                )
            );
        }
    }

    /**
     * Tests asynchronous execution of a command that returns too much data on stdout
     */
    @Test(expected = java.io.IOException.class)
    public void asyncExecTooMuchDataTest() throws Exception
    {
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ec.asyncExec("test-support/TestOutput", "42", "6350750", "stdout", "exit");
        ExtCmd.OutputData output = ec.syncProcess();

        if (output.stdoutData.length > OutputReceiver.MAX_DATA_SIZE)
        {
            fail(
                String.format(
                    "OutputReceiver data size limitation does not work, read %d bytes",
                    output.stdoutData.length
                )
            );
        }
    }

    /**
     * Tests synchronous execution of a command that times out
     */
    @SuppressWarnings("checkstyle:magicnumber")
    @Test(expected = ChildProcessTimeoutException.class)
    public void execHangingProcessTest() throws Exception
    {
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ec.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 3000);
        ec.setTimeout(ChildProcessHandler.TimeoutType.TERM, 1000);
        ExtCmd.OutputData output = ec.exec("test-support/TestOutput", "42", "640046", "stdout", "hang");
    }

    /**
     * Tests asynchronous execution of a command that times out
     */
    @SuppressWarnings("checkstyle:magicnumber")
    @Test(expected = ChildProcessTimeoutException.class)
    public void asyncExecHangingProcessTest() throws Exception
    {
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        ec.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 3000);
        ec.setTimeout(ChildProcessHandler.TimeoutType.TERM, 1000);
        ec.asyncExec("test-support/TestOutput", "42", "640046", "stdout", "hang");

        // TODO: run something else while the external process is working

        ExtCmd.OutputData output = ec.syncProcess();
    }
}
