package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.linstor.storage.layer.provider.utils.RetryIfDeviceBusy;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.storage.layer.provider.utils.Commands.genericExecutor;

import java.io.File;
import java.util.List;
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
                "-o", "name,used,volsize,type", // columns: name, allocated space, available space, type
                "-t", "volume,snapshot"
            },
            "Failed to list zfs volumes",
            "Failed to query 'zfs' info"
        );
    }

    public static OutputData getExtentSize(ExtCmd extCmd, String zpool, String identifier) throws StorageException
    {
        String fullQualifiedId;
        if (identifier == null || identifier.trim().isEmpty())
        {
            fullQualifiedId = zpool;
        }
        else
        {
            fullQualifiedId = zpool + File.separator + identifier;
        }
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
            "Failed to delete zfs volume '" + fullQualifiedId + "'",
            new RetryIfDeviceBusy()
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


    public static OutputData createSnapshot(ExtCmd extCmd, String zPool, String srcIdentifier, String snapName)
        throws StorageException
    {
        String fullQualifiedId = zPool + File.separator + srcIdentifier + "@" + snapName;
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "snapshot",
                fullQualifiedId
            },
            "Failed to create snapshot '" + fullQualifiedId + "'",
            "Failed to create snapshot '" + fullQualifiedId + "'"
        );
    }

    public static OutputData restoreSnapshot(
        ExtCmd extCmd,
        String zPool,
        String sourceLvName,
        String sourceSnapName,
        String targetLvName
    )
        throws StorageException
    {
        String fullQualifiedSourceSnapId =
            zPool + File.separator +
            sourceLvName + "@" +
            sourceSnapName;
        String fullQualifiedTargetLvId =
            zPool + File.separator +
            targetLvName;
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "clone",
                fullQualifiedSourceSnapId,
                fullQualifiedTargetLvId
            },
            "Failed to restore snapshot '" + fullQualifiedSourceSnapId + "' into '" + fullQualifiedTargetLvId + "'",
            "Failed to restore snapshot '" + fullQualifiedSourceSnapId + "' into '" + fullQualifiedTargetLvId + "'"
        );
    }

    public static OutputData rollback(ExtCmd extCmd, String zPool, String vlmId, String snapName)
        throws StorageException
    {
        String fullQualifiedSnapSource = zPool + File.separator + vlmId + "@" + snapName;
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "rollback",
                fullQualifiedSnapSource
            },
            "Failed to rollback to snapshot '" + fullQualifiedSnapSource + "'",
            "Failed to rollback to snapshot '" + fullQualifiedSnapSource + "'"
        );
    }

    public static OutputData listZpools(ExtCmd extCmd)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                "zpool",
                "list",
                "-o", "name",
                "-H"
            },
            "Failed to query list of zpools",
            "Failed to query list of zpools"
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

    public static OutputData listThinPools(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "list",
                "-H",   // no headers, single tab instead of spaces
                "-p",   // sizes in bytes
                "-o", "name,available,type", // columns: name, available space, type
                "-t", "filesystem"
            },
            "Failed to list zfs filesystem types",
            "Failed to query 'zfs' info"
        );
    }

    public static OutputData createZPool(
        ExtCmd extCmd,
        final List<String> devicePaths,
        final RaidLevel raidLevel,  // ignore for now as we only support JBOD yet
        final String zpoolName
    )
        throws StorageException
    {
        final String failMsg = "Failed to create zpool: " + zpoolName;
        return genericExecutor(
            extCmd,
            StringUtils.concat(
                new String[] {
                    "zpool",
                    "create",
                    "-f", // force otherwise zpool will cry about possible partition on device
                    zpoolName
                },
                devicePaths),
            failMsg,
            failMsg
        );
    }

    public static OutputData getPhysicalDevices(ExtCmd extCmdRef, String zPoolRef) throws StorageException
    {
        final String failMsg = "Failed to query physical devices for zpool: " + zPoolRef;
        return genericExecutor(
            extCmdRef,
            new String[]
            {
                "zpool",
                "list",
                "-v", // verbose, in order to include physical devices
                "-P", // Display full paths for vdevs instead of only the last component of the path
                "-H", // Scripted mode. Do not display headers, and separate fields by a single tab instead of arbitrary
                      // space.
                "-o", "name", // only "name" column
                zPoolRef
            },
            failMsg,
            failMsg
        );
    }

    private ZfsCommands()
    {
    }
}
