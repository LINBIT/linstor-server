package com.linbit.linstor.layer.storage.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.ShellUtils;

import java.io.IOException;

public class DmStatCommands
{
    private static final String DMSTATS_COMMAND = "dmstats";

    public static String[] getDmstatCreateCommand(String devPath)
    {
        return new String[]
        {
            DMSTATS_COMMAND, "create", devPath
        };
    }

    public static String[] getDmstatDeleteCommand(String devPath)
    {
        return new String[]
        {
            DMSTATS_COMMAND, "delete", devPath, "--allregions"
        };
    }

    public static void create(ExtCmd extCmd, String devPath) throws StorageException
    {
        genericExecutor(
            extCmd,
            getDmstatCreateCommand(devPath),
            "Failed to call dmstat create " + devPath,
            "Failed to call dmstat create"
        );
    }

    public static void delete(ExtCmd extCmd, String devPath) throws StorageException
    {
        genericExecutor(
            extCmd,
            getDmstatDeleteCommand(devPath),
            "Failed to call dmstat delete " + devPath,
            "Failed to call dmstat delete"
        );
    }

    private static void genericExecutor(
        ExtCmd extCmd,
        String[] command,
        String failMsgExitCode,
        String failMsgExc
    )
        throws StorageException
    {
        OutputData output;
        try
        {
            output = extCmd.exec(command);

            ExtCmdUtils.checkExitCode(
                output,
                StorageException::new,
                failMsgExitCode
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                failMsgExc,
                null,
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", ShellUtils.joinShellQuote(command)),
                exc
            );
        }

    }

    private DmStatCommands()
    {
    }

}
