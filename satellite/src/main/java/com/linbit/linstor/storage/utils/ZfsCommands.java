package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.storage.layer.provider.utils.Commands.genericExecutor;

import java.io.File;
import java.util.Set;

public class ZfsCommands
{
    public static OutputData list(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "list",
                "-H",   // no headers, single tab instead of spaces
                "-p",   // sizes in bytes
                "-o", "name,used,type", // columns: name, allocated space
            },
            "Failed to list zfs volumes",
            "Failed to query 'zfs' info"
        );
    }

    public static OutputData getExtentSize(ExtCmd extCmd, String zpool, String identifier) throws StorageException
    {
        String fullQualifiedId = zpool + File.separator + identifier;
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "get", "volblocksize",
                "-o", "value",
                "-Hp",
                fullQualifiedId
            },
            "Failed to query zfs extent size",
            "Failed to query extent size of zfs volume " + fullQualifiedId
        );
    }

    public static OutputData create(ExtCmd extCmd, String zpool, String identifier, long size, boolean thin)
        throws StorageException
    {
        String fullQualifiedId = zpool + File.separator + identifier;
        String[] command;
        if (thin)
        {
            command = new String[] {
                "zfs",
                "create",
                "-s",
                "-V", size + "KB",
                fullQualifiedId
            };
        }
        else
        {
            command = new String[] {
                "zfs",
                "create",
                // "-s",
                "-V", size + "KB",
                fullQualifiedId
            };
        }
        return genericExecutor(
            extCmd,
            command,
            "Failed to create zfsvolume",
            "Failed to create new zfs volume '" + fullQualifiedId + "' with size " + size + "kb"
        );
    }

    public static OutputData delete(ExtCmd extCmd, String zpool, String identifier)
        throws StorageException
    {
        String fullQualifiedId = zpool + File.separator + identifier;
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "destroy",
                fullQualifiedId
            },
            "Failed to delete zfs volume",
            "Failed to delete zfs volume '" + fullQualifiedId + "'"
        );
    }

    public static OutputData resize(ExtCmd extCmd, String zpool, String identifier, long size)
        throws StorageException
    {
        String fullQualifiedId = zpool + File.separator + identifier;
        return genericExecutor(
            extCmd,
            new String[]
            {
                "zfs",
                "set", "volsize=" + size + "KB",
                fullQualifiedId
            },
            "Failed to resize zfs volume",
            "Failed to resize zfs volume '" + fullQualifiedId + "' to size " + size
        );
    }

    public static OutputData rename(ExtCmd extCmd, String zpool, String currentId, String newId)
        throws StorageException
    {
        String fullQualifiedCurrentId = zpool + File.separator + currentId;
        String fullQualifiedNewId = zpool + File.separator + newId;
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "rename",
                fullQualifiedCurrentId,
                fullQualifiedNewId
            },
            "Failed to rename zfs volume from '" + fullQualifiedCurrentId + "' to '" + fullQualifiedNewId + "'",
            "Failed to rename zfs volume from '" + fullQualifiedCurrentId + "' to '" + fullQualifiedNewId + "'"
        );
    }

    public static OutputData getZPoolTotalSize(ExtCmd extCmd, Set<String> zpools)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            StringUtils.concat(
                new String[]
                {
                    "zpool",
                    "get",
                    "size",
                    "-Hp"
                },
                zpools
            ),
            "Failed to query total size of zpool(s) " + zpools,
            "Failed to query total size of zpool(s) " + zpools
        );
    }

    public static OutputData getZPoolFreeSize(ExtCmd extCmd, Set<String> zPools)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            StringUtils.concat(
                new String[]
                {
                    "zfs",
                    "get",
                    "available",
                    "-o", "name,value",
                    "-Hp"
                },
                zPools
            ),
            "Failed to query free size of zPool(s) " + zPools,
            "Failed to query free size of zPools(s) " + zPools
        );
    }
}
