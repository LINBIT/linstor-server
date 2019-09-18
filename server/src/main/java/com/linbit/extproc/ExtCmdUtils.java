package com.linbit.extproc;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.StringUtils;

public class ExtCmdUtils
{
    public static final int DEFAULT_RET_CODE_OK = 0;

    @FunctionalInterface
    public interface ExceptionFactory<EXC extends Exception>
    {
        EXC createException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText
        );
    }

    public static <EXC extends Exception> void checkExitCode(
        OutputData output,
        ExceptionFactory<EXC> excFactory,
        String format,
        Object... args
    )
        throws EXC
    {
        checkExitCode(output, DEFAULT_RET_CODE_OK, excFactory, format, args);
    }

    /**
     * Simple check that throws a exception returned by the given {@link ExceptionFactory}
     * if the exit code is not equal to the {@code expectedRetCode}.
     *
     * @param output
     *            The {@link OutputData} which contains the exit code
     * @param command
     *            The <code>String[]</code> that was called (used in the
     *            exception message)
     * @param expectedRetCode
     *            The expected return code - usually 0
     * @param excFactory
     *            The exception factory creating a new exception if the exitCode is unexpected
     * @param format
     *            An optional additional message which is printed before the default
     *            "command '%s' returned with exitcode %d. Error message: %s" message
     * @param args
     *            The arguments for the format parameter
     * @throws StorageException
     *            If the exitCode of output is not 0, a {@link StorageException} is thrown.
     */
    public static <EXC extends Exception> void checkExitCode(
        OutputData output,
        int expectedRetCode,
        ExceptionFactory<EXC> excFactory,
        String format,
        Object... args
    )
        throws EXC
    {
        if (output.exitCode != expectedRetCode)
        {
            throw excFactory.createException(
                format != null && format.length() > 0 ?
                    String.format(format, args) :
                    "External command failed",
                null,
                null,
                null,
                String.format(
                    "Command '%s' returned with exitcode %d. %n%n" +
                        "Standard out: %n" +
                        "%s" +
                        "%n%n" +
                        "Error message: %n" +
                        "%s" +
                        "%n",
                    StringUtils.join(" ", output.executedCommand),
                    output.exitCode,
                    new String(output.stdoutData),
                    new String(output.stderrData)
                )
            );
        }
    }

    private ExtCmdUtils()
    {
    }
}
