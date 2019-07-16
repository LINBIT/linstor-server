package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import static com.linbit.linstor.storage.layer.provider.utils.Commands.genericExecutor;

import java.io.IOException;
import java.nio.file.Path;

import com.google.common.io.Files;

public class FileCommands
{
    public static OutputData createFat(
        ExtCmd extCmd,
        Path vlmPath,
        long size
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "fallocate",
                "-x",
                "-l", size + "KiB",
                vlmPath.toString()
            },
            "Failed to create file volume",
            "Failed to create new file volume '" + vlmPath + "' with size " + size + "kb"
        );
    }

    public static OutputData createThin(
        ExtCmd extCmd,
        Path vlmPath,
        long size
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "truncate",
                "-s", size + "KiB",
                vlmPath.toString()
            },
            "Failed to create file volume",
            "Failed to create new file volume '" + vlmPath + "' with size " + size + "kb"
        );
    }


    public static void rename(
        Path storageDirectoryRef,
        String oldIdRef,
        String newIdRef
    )
        throws StorageException
    {
        try
        {
            Files.move(
                storageDirectoryRef.resolve(oldIdRef).toFile(),
                storageDirectoryRef.resolve(newIdRef).toFile()
            );
        }
        catch (IOException exc)
        {
            throw new StorageException("Unable to rename file volume from '" + oldIdRef + "' to '" + newIdRef +
                "' within directory '" + storageDirectoryRef + "'",
                exc
            );
        }
    }

    public static void delete(Path storageDirectoryRef, String newIdRef)
    {
        storageDirectoryRef.resolve(newIdRef).toFile().delete();
    }
}
