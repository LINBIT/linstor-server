package com.linbit.extproc;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.ShellUtils;

import java.util.Collections;
import java.util.List;

public class ExtCmdUtils
{
    public static final int DEFAULT_RET_CODE_OK = 0;

    @FunctionalInterface
    public interface ExceptionFactory<EXC extends Exception>
    {
        EXC createException(
            String message,
            @Nullable String descriptionText,
            @Nullable String causeText,
            @Nullable String correctionText,
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
        checkExitCode(output, Collections.singletonList(DEFAULT_RET_CODE_OK), excFactory, format, args);
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
     * @param expectedRetCodes
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
        List<Integer> expectedRetCodes,
        ExceptionFactory<EXC> excFactory,
        String format,
        Object... args
    )
        throws EXC
    {
        if (!expectedRetCodes.contains(output.exitCode))
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
                    ShellUtils.joinShellQuote(output.executedCommand),
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
