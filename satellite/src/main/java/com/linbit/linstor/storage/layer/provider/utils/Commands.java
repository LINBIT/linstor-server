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

}
