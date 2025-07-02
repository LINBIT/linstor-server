package com.linbit.extproc;

import java.io.IOException;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.LinStorException;
import com.linbit.utils.ShellUtils;

public class ExtCmdFailedException extends LinStorException
{
    private static final long serialVersionUID = 5779506237459279868L;

    private static final String EXCEPTION_DESCR_FORMAT = "Execution of the external command '%s' failed.";
    private static final String EXCEPTION_DETAILS_FORMAT = "The full command line executed was:\n%s";
    private static final String EXCEPTION_STDOUT_DATA = "The external command sent the following output data:";
    private static final String EXCEPTION_STDERR_DATA = "The external command sent the following error information:";

    public ExtCmdFailedException(String[] command, ChildProcessTimeoutException cause)
    {
        super(
            String.format("The external command '%s' did not complete within the timeout", command[0]),
            String.format(EXCEPTION_DESCR_FORMAT, command[0]),
            "The external command did not complete within the timeout.\n" +
            "Possible causes include:\n" +
            "- The system load may be too high to ensure completion of external commands in a timely manner.\n" +
            "- The program implementing the external command may not be operating properly.\n" +
            "- The operating system may have entered an erroneous state.",
            "Check whether the external program and the operating system are still operating properly.\n" +
            "Check whether the system's load is within normal parameters.\n",
            String.format(EXCEPTION_DETAILS_FORMAT, ShellUtils.joinShellQuote(command)),
            cause
        );
    }

    public ExtCmdFailedException(String[] command, IOException cause)
    {
        super(
            String.format("Data exchange with the external command '%s' failed", command[0]),
            String.format(EXCEPTION_DESCR_FORMAT, command[0]),
            "Data exchange with the external command failed before the execution completed, or " +
            "the amount of data sent by the external command exceeded the size limit.",
            "Check whether the external program is operating properly and produces meaningful output.",
            String.format(EXCEPTION_DETAILS_FORMAT, ShellUtils.joinShellQuote(command)),
            cause
        );
    }

    public ExtCmdFailedException(String[] command, OutputData outputData)
    {
        super(
            String.format("The external command '%s' exited with error code %d\n", command[0], outputData.exitCode),
            String.format(EXCEPTION_DESCR_FORMAT, command[0]),
            String.format("The external command exited with error code %d.", outputData.exitCode),
            "- Check whether the external program is operating properly.\n" +
            "- Check whether the command line is correct.\n" +
            "  Contact a system administrator or a developer if the command line is no longer valid\n" +
            "  for the installed version of the external program.",
            String.format(
                EXCEPTION_DETAILS_FORMAT +
                "\n\n",
                ShellUtils.joinShellQuote(command)
            ) +
            EXCEPTION_STDOUT_DATA + "\n" + new String(outputData.stdoutData) + "\n\n" +
            EXCEPTION_STDERR_DATA + "\n" + new String(outputData.stderrData) + "\n"
        );
    }
}
