package com.linbit.linstor.storage.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.Platform;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.ShellUtils;
import com.linbit.utils.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Commands
{
    public interface RetryHandler
    {
        /**
         * If skip returns true, the failed executed external command
         * is completely ignored. No exception is thrown.
         *
         * @param outData
         */
        boolean skip(OutputData outData);

        /**
         * If retry returns true the command will be executed again.
         *
         * @param outputData
         */
        boolean retry(OutputData outputData);
    }

    public static final RetryHandler NO_RETRY = new NoRetryHandler();
    public static final RetryHandler SKIP_EXIT_CODE_CHECK = new SkipExitCodeRetryHandler();
    public static final int ARGUMENT_LIMIT = 1000;

    public static OutputData genericExecutor(
        ExtCmd extCmd,
        String[] command,
        @Nullable String failMsgExitCode,
        @Nullable String failMsgExc
    )
        throws StorageException
    {
        return genericExecutor(extCmd, command, failMsgExitCode, failMsgExc, NO_RETRY);
    }

    public static OutputData genericExecutor(
        ExtCmd extCmd,
        String[] command,
        @Nullable String failMsgExitCode,
        @Nullable String failMsgExc,
        RetryHandler retryHandler
    )
        throws StorageException
    {
        return genericExecutor(extCmd, command, failMsgExitCode, failMsgExc, retryHandler, Collections.emptyList());
    }

    public static OutputData genericExecutor(
        ExtCmd extCmd,
        String[] command,
        @Nullable String failMsgExitCode,
        @Nullable String failMsgExc,
        RetryHandler retryHandler,
        List<Integer> allowExitCodes
    )
        throws StorageException
    {
        OutputData outData;
        try
        {
            outData = extCmd.exec(command);

            boolean skipExitCodeCheck = false;
            while (!(outData.exitCode == ExtCmdUtils.DEFAULT_RET_CODE_OK || allowExitCodes.contains(outData.exitCode)))
            {
                if (retryHandler.skip(outData))
                {
                    skipExitCodeCheck = true;
                    break;
                }
                if (retryHandler.retry(outData))
                {
                    outData = extCmd.exec(command);
                }
                else
                {
                    break;
                }
            }

            if (!skipExitCodeCheck)
            {
                List<Integer> ignoreCodes = new ArrayList<>(allowExitCodes);
                ignoreCodes.add(ExtCmdUtils.DEFAULT_RET_CODE_OK);
                ExtCmdUtils.checkExitCode(
                    outData,
                    ignoreCodes,
                    StorageException::new,
                    failMsgExitCode
                );
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                failMsgExc,
                null,
                (exc instanceof IOException) ?
                    "External command threw an IOException" :
                    "External command timed out",
                null,
                String.format("External command: %s", ShellUtils.joinShellQuote(command)),
                exc
            );
        }

        return outData;
    }

    /**
     * Uses the varArgs collection to do looped execution of the given command and concat all output into a single
     * OutputData object. This only works well if the output doesn't have any header or footer data.
     * @param extCmd
     * @param command
     * @param varArgs
     * @param failMsgExitCode
     * @param failMsgExc
     * @param allowExitCodes
     * @return Combined OutputData object of each execution.
     * @throws StorageException
     */
    public static OutputData genericExecutorLimiter(
        ExtCmd extCmd,
        String[] command,
        Collection<String> varArgs,
        @Nullable String failMsgExitCode,
        @Nullable String failMsgExc,
        List<Integer> allowExitCodes
    )
        throws StorageException
    {
        int loops = (varArgs.size() / ARGUMENT_LIMIT) + 1;
        ByteArrayOutputStream stdOutData = new ByteArrayOutputStream();
        ByteArrayOutputStream stdErrData = new ByteArrayOutputStream();
        for (long i = 0; i < loops; ++i)
        {
            String[] curCmd = StringUtils.concat(
                command,
                varArgs.stream().skip(ARGUMENT_LIMIT * i).limit(ARGUMENT_LIMIT).collect(Collectors.toList()));
            OutputData data = genericExecutor(extCmd, curCmd, failMsgExitCode, failMsgExc, NO_RETRY, allowExitCodes);

            try
            {
                stdOutData.write(data.stdoutData);
                stdErrData.write(data.stderrData);
            }
            catch (IOException ioExc)
            {
                throw new StorageException("Unable to concat extCmd output data: " + ioExc.getMessage());
            }
        }
        return new OutputData(command, stdOutData.toByteArray(), stdErrData.toByteArray(), 0);
    }

    public static OutputData wipeFs(
        ExtCmd extCmd,
        Collection<String> devicePaths
    )
        throws StorageException
    {
        OutputData outputData = null;

        if (Platform.isLinux())
        {
            outputData = genericExecutor(
                extCmd,
                StringUtils.concat(
                    new String[]
                    {
                        "wipefs", "-a", "-f"
                    },
                    devicePaths
                ),
                "Failed to wipeFs of " + String.join(", ", devicePaths),
                "Failed to wipeFs of " + String.join(", ", devicePaths)
            );
        }
        else if (Platform.isWindows())
        {
            outputData = new OutputData(
                    new String[] {},
                    new byte[] {},
                    new byte[] {},
                    0
            );
        }
        else
        {
            throw new ImplementationError("Platform is neither Linux nor Windows, please add support for it to LINSTOR");
        }
        return outputData;
    }

    public static long getDeviceSizeInSectors(
        ExtCmd extCmd,
        String devicePath
    )
        throws StorageException
    {
        OutputData output = genericExecutor(
            extCmd,
            new String[]
            {
                "blockdev",
                "--getsz",
                devicePath
            },
            "Failed to get device size of " + devicePath,
            "Failed to get device size of " + devicePath
        );
        String outRaw = new String(output.stdoutData);
        return Long.parseLong(outRaw.trim());
    }

    public static long getBlockSizeInKib(
        ExtCmd extCmd,
        String devicePath
    )
        throws StorageException
    {
        OutputData output = genericExecutor(
            extCmd,
            new String[] {"blockdev", "--getsize64", devicePath},
            "Failed to get block size of " + devicePath,
            "Failed to get block size of " + devicePath
        );
        String outRaw = new String(output.stdoutData);
        return SizeConv.convert(
            Long.parseLong(outRaw.trim()),
            SizeUnit.UNIT_B,
            SizeUnit.UNIT_KiB
        );
    }

    public static class NoRetryHandler implements RetryHandler
    {
        @Override
        public boolean retry(OutputData outputData)
        {
            return false;
        }

        @Override
        public boolean skip(OutputData outData)
        {
            return false;
        }
    }

    public static class SkipExitCodeRetryHandler implements RetryHandler
    {
        @Override
        public boolean retry(OutputData outputData)
        {
            return false;
        }

        @Override
        public boolean skip(OutputData outData)
        {
            return true;
        }
    }

    private Commands()
    {
    }
}
