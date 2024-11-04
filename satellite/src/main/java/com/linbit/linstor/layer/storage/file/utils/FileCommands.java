package com.linbit.linstor.layer.storage.file.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;

import static com.linbit.linstor.storage.utils.Commands.genericExecutor;

import java.io.IOException;
import java.nio.file.Path;

import com.google.common.io.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    // ignoring return values because whether there actually was a file to delete is unimportant
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static void delete(Path storageDirectoryRef, String newIdRef)
    {
        storageDirectoryRef.resolve(newIdRef).toFile().delete();
    }

    public static OutputData createSnapshot(ExtCmd extCmd, Path fromPath, Path toPath) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "cp",
                "--reflink=always",
                fromPath.toString(),
                toPath.toString()
            },
            "Failed to create snapshot of file volume",
            "Failed to create snapshot '" + toPath + "' of file volume '" + fromPath + "'"
        );
    }

    public static OutputData copy(ExtCmd extCmd, Path fromPath, Path toPath) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "cp",
                fromPath.toString(),
                toPath.toString()
            },
            "Failed to copy file",
            "Failed to copy '" + fromPath + "' to '" + toPath + "'"
        );
    }

    public static OutputData getTotalCapacity(ExtCmd extCmd, Path storageDirectory)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "df",
                "-B", "1", // in bytes
                "--output=size",
                storageDirectory.toString()
            },
            "Failed to fetch pool capacity",
            "Failed to fetch capacity of storage pool '" + storageDirectory + "'"
        );
    }

    public static OutputData getFreeSpace(ExtCmd extCmd, Path storageDirectory)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "df",
                "-B", "1", // in bytes
                "--output=avail",
                storageDirectory.toString()
            },
            "Failed to fetch pool free space",
            "Failed to fetch free space of storage pool '" + storageDirectory + "'"
        );
    }

    public static OutputData getSourceDevice(ExtCmd extCmd, Path storageDirectoryRef)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "df",
                "--output=source",
                storageDirectoryRef.toString()
            },
            null,
            null
        );
    }

    public static OutputData getAllocatedThinSize(ExtCmd extCmd, String storagePathRef) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                "stat",
                "-c",
                "%B %b", // size of one block in bytes, number of allocated blocks
                storagePathRef
            },
            "Failed to fetch allocated size of " + storagePathRef,
            "Failed to fetch allocated size of " + storagePathRef
        );
    }

    private FileCommands()
    {
    }
}
