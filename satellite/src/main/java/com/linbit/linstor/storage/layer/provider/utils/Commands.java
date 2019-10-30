package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.SpdkUtils;
import com.linbit.utils.StringUtils;

import java.io.IOException;
import java.util.Arrays;

import static com.linbit.linstor.storage.utils.SpdkUtils.SPDK_PATH_PREFIX;

public class Commands
{
    public interface RetryHandler
    {
        boolean retry(OutputData outputData);

        boolean skip(OutputData outData);
    }

    public static final RetryHandler NO_RETRY = new NoRetryHandler();
    public static final RetryHandler SKIP_EXIT_CODE_CHECK = new SkipExitCodeRetryHandler();

    public static OutputData genericExecutor(
        ExtCmd extCmd,
        String[] command,
        String failMsgExitCode,
        String failMsgExc
    )
        throws StorageException
    {
        return genericExecutor(extCmd, command, failMsgExitCode, failMsgExc, NO_RETRY);
    }

    public static OutputData genericExecutor(
        ExtCmd extCmd,
        String[] command,
        String failMsgExitCode,
        String failMsgExc,
        RetryHandler retryHandler
    )
        throws StorageException
    {
        OutputData outData;
        try
        {
            outData = extCmd.exec(command);

            boolean skipExitCodeCheck = false;
            while (outData.exitCode != ExtCmdUtils.DEFAULT_RET_CODE_OK)
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
                ExtCmdUtils.checkExitCode(
                    outData,
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
                String.format("External command: %s", StringUtils.join(" ", command)),
                exc
            );
        }

        return outData;
    }

    public static OutputData wipeFs(
        ExtCmd extCmd,
        String devicePath
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                "wipefs", "-a", "-f",
                devicePath
            },
            "Failed to wipeFs of " + devicePath,
            "Failed to wipeFs of " + devicePath
        );
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
        long sizeKiB;
        if (devicePath.startsWith(SPDK_PATH_PREFIX))
        {
            sizeKiB = SpdkUtils.getBlockSizeByName(extCmd, devicePath.split(SPDK_PATH_PREFIX)[1]);
        }
        else
        {
            OutputData output = genericExecutor(
                extCmd,
                new String[]
                {
                    "blockdev",
                    "--getsize64",
                    devicePath
                },
                "Failed to get block size of " + devicePath,
                "Failed to get block size of " + devicePath
            );
            String outRaw = new String(output.stdoutData);
            sizeKiB = SizeConv.convert(
                Long.parseLong(outRaw.trim()),
                SizeUnit.UNIT_B,
                SizeUnit.UNIT_KiB
            );
        }
        return sizeKiB;
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
