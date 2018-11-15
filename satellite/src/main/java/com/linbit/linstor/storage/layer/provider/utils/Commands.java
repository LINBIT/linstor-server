package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.StringUtils;

import java.io.IOException;

public class Commands
{
    private static final int KIB = 1024;

    public static OutputData genericExecutor(
        ExtCmd extCmd,
        String[] command,
        String failMsgExitCode,
        String failMsgExc
    )
        throws StorageException
    {
        OutputData outData;
        try
        {
            outData = extCmd.exec(command);

            ExtCmdUtils.checkExitCode(
                outData,
                command,
                StorageException::new,
                failMsgExitCode
            );
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
                    "wipefs",
                    devicePath
                },
            "Failed to wipeFs of " + devicePath,
            "Failed to wipeFs of " + devicePath
        );
    }

    public static long getBlockSizeInKib(
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
                    "--getsize64",
                    devicePath
                },
            "Failed to get block size of " + devicePath,
            "Failed to get block size of " + devicePath
        );
        String outRaw = new String(output.stdoutData);
        return Long.parseLong(outRaw.trim()) / KIB;
    }

}
