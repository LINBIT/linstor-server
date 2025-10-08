package com.linbit.linstor.layer.storage.zfs.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.layer.storage.utils.RetryIfDeviceBusy;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.linstor.storage.utils.Commands.RetryHandler;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.storage.utils.Commands.genericExecutor;
import static com.linbit.linstor.storage.utils.Commands.genericExecutorLimiter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ZfsCommands
{
    private static final String ZFS_USER_PROP_LINSTOR_PREFIX = "linstor:";

    public static OutputData list(ExtCmd extCmd, Collection<String> datasets) throws StorageException
    {
        return genericExecutorLimiter(
            extCmd,
            new String[] {
                "zfs",
                "list",
                "-r",   // recursive
                "-H",   // no headers, single tab instead of spaces
                "-p",   // sizes in bytes
                // columns: name, referred space, available space, type, extent size, origin, clones
                "-o", "name,refer,volsize,type,volblocksize,origin,clones",
                "-t", "volume,snapshot"
            },
            datasets,
            "Failed to list zfs volumes",
            "Failed to query 'zfs' info",
            Collections.singletonList(1)
        );
    }

    public static OutputData getExtentSize(ExtCmd extCmd, String zpool, String identifier) throws StorageException
    {
        String fullQualifiedId;
        if (identifier.trim().isEmpty())
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

    public static OutputData create(
        ExtCmd extCmd,
        String zpool,
        String identifier,
        long size,
        boolean thin,
        String... additionalParameters
    )
        throws StorageException
    {
        String fullQualifiedId = zpool + File.separator + identifier;
        ArrayList<String> cmdList = new ArrayList<>();

        cmdList.add("zfs");
        cmdList.add("create");
        if (thin)
        {
            cmdList.add("-s");
        }
        cmdList.add("-V");
        cmdList.add(size + "KB");
        cmdList.addAll(Arrays.asList(additionalParameters));
        cmdList.add(fullQualifiedId);

        return genericExecutor(
            extCmd,
            cmdList.toArray(new String[0]),
            "Failed to create zfsvolume",
            "Failed to create new zfs volume '" + fullQualifiedId + "' with size " + size + "kb"
        );
    }

    public static OutputData delete(ExtCmd extCmd, String zpool, String identifier, ZfsVolumeType type)
        throws StorageException
    {
        return delete(extCmd, zpool + File.separator + identifier, type, new RetryIfDeviceBusy());
    }

    public static OutputData delete(
        ExtCmd extCmd,
        String zpool,
        String identifier,
        ZfsVolumeType type,
        RetryHandler retryHandlerRef
    )
        throws StorageException
    {
        return delete(extCmd, zpool + File.separator + identifier, type, retryHandlerRef);
    }

    public static OutputData delete(ExtCmd extCmd, String fullQualifiedIdentifier, ZfsVolumeType type)
        throws StorageException
    {
        return delete(extCmd, fullQualifiedIdentifier, type, new RetryIfDeviceBusy());
    }

    public static OutputData delete(
        ExtCmd extCmd,
        String fullQualifiedIdentifier,
        ZfsVolumeType type,
        RetryHandler retryHandlerRef
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "destroy",
                fullQualifiedIdentifier
            },
            "Failed to delete zfs " + type.descr,
            "Failed to delete zfs " + type.descr + " '" + fullQualifiedIdentifier + "'",
            retryHandlerRef
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


    public static OutputData createSnapshot(
        ExtCmd extCmd,
        String zPool,
        String srcIdentifier,
        String snapName,
        String... additionalParameters
    )
        throws StorageException
    {
        return createSnapshotFullName(extCmd, zPool, srcIdentifier + "@" + snapName, additionalParameters);
    }

    public static OutputData createSnapshotFullName(
        ExtCmd extCmd,
        String zPool,
        String fullSnapName,
        String... additionalParameters
    )
        throws StorageException
    {

        String fullQualifiedId = zPool + File.separator + fullSnapName;

        ArrayList<String> cmdList = new ArrayList<>();
        cmdList.add("zfs");
        cmdList.add("snapshot");
        cmdList.addAll(Arrays.asList(additionalParameters));
        cmdList.add(fullQualifiedId);

        return genericExecutor(
            extCmd,
            cmdList.toArray(new String[0]),
            "Failed to create snapshot '" + fullQualifiedId + "'",
            "Failed to create snapshot '" + fullQualifiedId + "'"
        );
    }

    public static OutputData restoreSnapshot(
        ExtCmd extCmd,
        String zPool,
        String sourceSnapshotName,
        String targetLvName
    )
        throws StorageException
    {
        String fullQualifiedSourceSnapId =
            zPool + File.separator + sourceSnapshotName;
        String fullQualifiedTargetLvId = zPool + File.separator + targetLvName;
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

    public static OutputData getQuotaSize(ExtCmd extCmd, Set<String> zPools)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            StringUtils.concat(
                new String[]
                    {
                        "zfs",
                        "get",
                        "quota",
                        "-o", "name,value",
                        "-Hp"
                    },
                zPools
            ),
            "Failed to query quota size of zPool(s) " + zPools,
            "Failed to query quota size of zPools(s) " + zPools
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

    public static OutputData listThinPools(ExtCmd extCmd, Collection<String> datasets) throws StorageException
    {
        return genericExecutor(
            extCmd,
            StringUtils.concat(
                new String[] {
                    "zfs",
                    "list",
                    "-r",   // recursive
                    "-H",   // no headers, single tab instead of spaces
                    "-p",   // sizes in bytes
                    // columns: name, available space, type
                    "-o", "name,available,type",
                    "-t", "filesystem"
                },
                datasets
            ),
            "Failed to list zfs filesystem types",
            "Failed to query 'zfs' info"
        );
    }

    public static OutputData createZPool(
        ExtCmd extCmd,
        final List<String> devicePaths,
        final List<String> createArgs,
        final RaidLevel raidLevel,  // ignore for now as we only support JBOD yet
        final String zpoolName
    )
        throws StorageException
    {
        final String failMsg = "Failed to create zpool: " + zpoolName;

        String[] command = StringUtils.concat(
            new String[] {
                "zpool",
                "create",
                "-f", // force otherwise zpool will cry about possible partition on device
                "-m", "none", // do not mount the default zpool dataset
            },
            createArgs
        );
        command = StringUtils.concat(command, zpoolName);
        command = StringUtils.concat(command, devicePaths);

        return genericExecutor(extCmd, command, failMsg, failMsg);
    }

    public static OutputData deleteZPool(
        ExtCmd extCmd,
        final String zpoolName
    ) throws StorageException
    {
        final String failMsg = "Failed to destroy zpool: " + zpoolName;
        return genericExecutor(
            extCmd,
            new String[] {
                "zpool",
                "destroy",
                zpoolName
            },
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

    public static OutputData setUserProperty(ExtCmd extCmd, String zPool, String zfsId, String name, String value)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                "zfs",
                "set",
                ZFS_USER_PROP_LINSTOR_PREFIX + name + "=" + value,
                zPool + "/" + zfsId
            },
            "Failed to set user property",
            "Failed to set user property"
        );
    }

    public static OutputData hideUnhideSnapshotDevice(
        ExtCmd extCmd,
        final String zPool,
        final String vlmId,
        boolean hide
    ) throws StorageException
    {
        final String fullPath = String.format("%s/%s", zPool, vlmId);
        final String failMsg = String.format(
            "Failed to hide/unhide snapshot: %s", fullPath);
        return genericExecutor(
            extCmd,
            new String[] {
                "zfs",
                "set",
                "snapdev=" + (hide ? "hidden" : "visible"),
                fullPath
            },
            failMsg,
            failMsg
        );
    }

    public static OutputData getUserProperty(ExtCmd extCmdRef, String zfsProp, Collection<String> dataSets)
        throws StorageException
    {
        return genericExecutor(
            extCmdRef,
            StringUtils.concat(
                new String[]
                {
                    "zfs",
                    "get",
                    "-r", // not just the prop from the datasets, but all of their volumes. automatically includes
                    // snapshots
                    "-o", "name,value",
                    "-H", // no heading
                    "-p", // parsable (exact) numbers
                    "-s", "local", // ZFS also has some property-inheritance. we only want to know if the property is
                    // set directly on the ZFS snapshot/volume, not if a snapshot inherits the property from its volume
                    ZFS_USER_PROP_LINSTOR_PREFIX + zfsProp
                },
                dataSets
            ),
            "Failed to get user property '" + ZFS_USER_PROP_LINSTOR_PREFIX + zfsProp + "'",
            "Failed to get user property '" + ZFS_USER_PROP_LINSTOR_PREFIX + zfsProp + "'"
        );
    }

    public enum ZfsVolumeType
    {
        VOLUME("volume"), SNAPSHOT("snapshot"), FILESYSTEM("filesystem");

        private final String descr;

        ZfsVolumeType(String descrRef)
        {
            descr = descrRef;
        }

        public String getDescr()
        {
            return descr;
        }

        public static @Nullable ZfsVolumeType parseNullable(String strRef)
        {
            @Nullable ZfsVolumeType ret = null;
            for (ZfsVolumeType type : values())
            {
                if (type.descr.equalsIgnoreCase(strRef))
                {
                    ret = type;
                    break;
                }
            }
            return ret;
        }

        public static ZfsVolumeType parseOrThrow(String strRef) throws StorageException
        {
            @Nullable ZfsVolumeType ret = parseNullable(strRef);
            if (ret == null)
            {
                throw new StorageException("Failed to parse '" + strRef + "'");
            }
            return ret;
        }
    }

    private ZfsCommands()
    {
    }
}
