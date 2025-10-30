package com.linbit.extproc;

import static org.junit.Assert.fail;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the ClientProcessHandler class
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */

@SuppressWarnings("checkstyle:magicnumber")
public class ClientProcessHandlerTest
{
    public static final String TEST_PROGRAM = "test-support/TestProcess";

    GenericTimer<String, Action<String>> intrTimer;

    @Before
    public void setUp()
    {
        intrTimer = new GenericTimer<>();
        intrTimer.start();
    }

    @After
    public void tearDown()
    {
        intrTimer.shutdown(false);
    }

    @Test
    public void testExitingProc() throws ChildProcessTimeoutException, IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "17",
                "250",
                "sigterm"
            }
        );
        Process childProc = pBuilder.start();
        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        int exitCode = cph.waitFor();
        if (exitCode != 17)
        {
            fail("Test program exit code != 0");
        }
    }

    @Test(expected = ChildProcessTimeoutException.class)
    public void testTimingOutProc() throws ChildProcessTimeoutException, IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "13",
                "15000",
                "sigterm"
            }
        );
        Process childProc = pBuilder.start();

        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 250L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        cph.waitFor();
    }

    @Test
    public void testWaitForDestroy() throws ChildProcessTimeoutException, IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "13",
                "15000",
                "sigterm"
            }
        );
        Process childProc = pBuilder.start();

        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 250L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        cph.setAutoTerm(false);
        cph.setAutoKill(false);
        boolean timeout = false;
        try
        {
            cph.waitFor();
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            timeout = true;
        }
        if (!timeout)
        {
            fail("Expected ChildProcessTimeoutException not thrown");
        }

        cph.waitForDestroy();
    }

    @Test(expected = ChildProcessTimeoutException.class)
    public void testWaitForDestroyTimeout() throws ChildProcessTimeoutException, IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "13",
                "15000",
                "never"
            }
        );
        Process childProc = pBuilder.start();

        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 250L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        cph.setAutoTerm(false);
        cph.setAutoKill(false);
        boolean timeout = false;
        try
        {
            cph.waitFor();
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            timeout = true;
        }
        if (!timeout)
        {
            fail("Expected ChildProcessTimeoutException not thrown");
        }

        cph.waitForDestroy();
    }

    @Test
    public void testWaitForDestroyForcibly() throws IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "13",
                "15000",
                "never"
            }
        );
        Process childProc = pBuilder.start();

        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 250L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        cph.setAutoTerm(false);
        cph.setAutoKill(false);
        boolean timeout = false;
        try
        {
            cph.waitFor();
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            timeout = true;
        }
        if (!timeout)
        {
            fail("Expected ChildProcessTimeoutException not thrown");
        }

        boolean killed = cph.waitForDestroyForcibly();
        if (!killed)
        {
            fail("Child process not killed as expected");
        }
    }

    @Test
    public void testAutoTerm() throws IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "13",
                "15000",
                "sigterm"
            }
        );
        Process childProc = pBuilder.start();

        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 250L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        cph.setAutoKill(false);
        try
        {
            cph.waitFor();
            fail("Process did not time out");
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            if (!timeoutExc.isTerminated())
            {
                fail("Terminating the child process using SIGTERM failed");
            }
        }
    }

    @Test
    public void testAutoKill() throws IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder(
            new String[]
            {
                TEST_PROGRAM,
                "13",
                "15000",
                "never"
            }
        );
        Process childProc = pBuilder.start();

        ChildProcessHandler cph = new ChildProcessHandler(childProc, intrTimer);
        cph.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 250L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.TERM, 500L);
        cph.setTimeout(ChildProcessHandler.TimeoutType.KILL, 500L);
        try
        {
            cph.waitFor();
            fail("Process did not time out");
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            if (!timeoutExc.isTerminated())
            {
                fail("Terminating the child process using SIGKILL failed");
            }
        }
    }
}
