package com.linbit.extproc;

import static org.junit.Assert.fail;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;

import java.io.IOException;

import org.junit.Test;

public class ExtCmdConcurrentTest implements Runnable
{
    public static final int CONCURRENT_COUNT = 20;

    public static final int OUTPUT_LENGTH = 14153;
    public static final int EXIT_CODE = 120;
    public static final int TESTOUTPUT_DELAY = 1500;

    GenericTimer<String, Action<String>> intrTimer;
    ErrorReporter errLog;

    public ExtCmdConcurrentTest()
    {
        intrTimer = new GenericTimer<>();
        intrTimer.start();
        errLog = new StderrErrorReporter("LINSTOR-UNITTESTS");
    }

    @Test
    public void concurrentTest() throws Exception
    {
        Thread[] ecThr = new Thread[CONCURRENT_COUNT];
        for (int thrIdx = 0; thrIdx < CONCURRENT_COUNT; ++thrIdx)
        {
            ecThr[thrIdx] = new Thread(new ExtCmdConcurrentTest());
        }

        for (int thrIdx = 0; thrIdx < CONCURRENT_COUNT; ++thrIdx)
        {
            ecThr[thrIdx].start();
        }

        for (int thrIdx = 0; thrIdx < CONCURRENT_COUNT; ++thrIdx)
        {
            ecThr[thrIdx].join();
        }

        intrTimer.shutdown(false);
    }

    @Override
    public void run()
    {
        if (intrTimer == null)
        {
            fail("WTF?");
        }
        ExtCmd ec = new ExtCmd(intrTimer, errLog);
        OutputData output;
        try
        {
            output = ec.exec(
                "test-support/TestOutput",
                Integer.toString(EXIT_CODE), Integer.toString(OUTPUT_LENGTH),
                "stdout", "exit",
                Integer.toString(TESTOUTPUT_DELAY)
            );
            if (output.stdoutData.length != OUTPUT_LENGTH)
            {
                fail(String.format("Unexpected stdoutData length %d", output.stdoutData.length));
            }
            if (output.exitCode != EXIT_CODE)
            {
                fail(String.format("Unexpected exit code %d", output.exitCode));
            }
        }
        catch (IOException ioExc)
        {
            fail("Test generated an IOException");
            ioExc.printStackTrace(System.err);
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            fail("Test generated a ChildProcessTimeoutException");
            timeoutExc.printStackTrace(System.err);
        }
    }
}
