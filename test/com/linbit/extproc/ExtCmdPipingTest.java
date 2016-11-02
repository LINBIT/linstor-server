package com.linbit.extproc;

import com.linbit.timer.GenericTimer;
import com.linbit.timer.Action;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExtCmdPipingTest
{
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
        intrTimer.shutdown();
    }

    @Test
    public void execTest() throws Exception
    {
        ExtCmd ec = new ExtCmd(intrTimer);
        ExtCmd.OutputData output = ec.exec("test-support/TestOutput", "0", "12345", "stdout", "exit");
    }

    @Test
    public void asyncExecTest() throws Exception
    {
        ExtCmd ec = new ExtCmd(intrTimer);
        ec.asyncExec("test-support/TestOutput", "0", "3456", "stdout", "exit");

        // TODO: run something else while the external process is working

        ExtCmd.OutputData output = ec.syncProcess();
    }
}
