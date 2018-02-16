package com.linbit.linstor.debug;

import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.security.AccessContext;

/**
 * Throws a test exception to test exception handling and reporting
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdTestErrorLog extends BaseControllerDebugCmd
{
    public CmdTestErrorLog()
    {
        super(
            new String[]
            {
                "TstErrLog"
            },
            "Test error log",
            "Throws an exception for test purposes",
            null,
            null
        );
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception
    {
        debugOut.println("Throwing exception for error logging test");
        throw new TestException(
            TestException.class.getName() + " instance thrown by the " +
            CmdTestErrorLog.class.getName() + " class for testing purposes",
            new TestException(
                "Nested " + TestException.class.getName() + " instance",
                null
            )
        );
    }

    public static class TestException extends Exception
    {
        TestException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}


