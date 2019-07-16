package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import static com.linbit.linstor.storage.layer.provider.utils.Commands.genericExecutor;

import java.nio.file.Path;

public class LosetupCommands
{
    public static final int LOSETUP_LIST_DEV_NAME_IDX = 0;
    public static final int LOSETUP_LIST_BACK_FILE_IDX = 1;

    public static OutputData attach(ExtCmd extCmd, Path backingFile)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "losetup",
                "-f",
                "--show",
                backingFile.toString()
            },
            "Failed to attach loop back device",
            "Failed to attach loop back device for backing file '" + backingFile + "'"
        );
    }

    public static OutputData list(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "losetup",
                "-l",
                "-O", "NAME,BACK-FILE"
            },
            "Failed to list loop back devices",
            "Failed to list loop back devices"
        );
    }

    public static OutputData detach(ExtCmd extCmd, String devPath)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "losetup",
                "-d", devPath
            },
            "Failed to detach loop back device",
            "Failed to detach loop back device '" + devPath + "'"
        );
    }
}
