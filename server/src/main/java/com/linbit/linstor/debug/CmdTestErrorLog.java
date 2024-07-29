package com.linbit.linstor.debug;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;

import java.io.PrintStream;
import java.util.Map;

/**
 * Throws a test exception to test exception handling and reporting
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdTestErrorLog extends BaseDebugCmd
{
    @Inject
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
                "dummy description",
                "dummy cause",
                "dummy correction",
                "dummy details",
                1L,
                null
            )
        );
    }

    public static class TestException extends LinStorException
    {
        private static final long serialVersionUID = 362631327796557839L;

        TestException(String message, @Nullable Throwable cause)
        {
            super(message, cause);
        }

        public TestException(
            String messageRef,
            @Nullable String descriptionTextRef,
            @Nullable String causeTextRef,
            @Nullable String correctionTextRef,
            @Nullable String detailsTextRef,
            @Nullable Long numericCodeRef,
            @Nullable Throwable causeRef
        )
        {
            super(
                messageRef,
                descriptionTextRef,
                causeTextRef,
                correctionTextRef,
                detailsTextRef,
                numericCodeRef,
                causeRef
            );
            // TODO Auto-generated constructor stub
        }
    }
}


