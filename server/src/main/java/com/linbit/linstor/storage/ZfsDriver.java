package com.linbit.linstor.storage;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.utils.Crypt;
import com.linbit.linstor.timer.CoreTimer;

public class ZfsDriver extends AbsStorageDriver
{
    public static final String ZFS_POOL_DEFAULT = "linstorpool";
    public static final String ZFS_COMMAND_DEFAULT = "zfs";
    public static final String ZFS_POOL_COMMAND_DEFAULT = "zpool";

    protected String zfsCommand = ZFS_COMMAND_DEFAULT;
    protected String zpoolCommand = ZFS_POOL_COMMAND_DEFAULT;

    protected String pool = ZFS_POOL_DEFAULT;

    public static final long ZFS_VOLBLOCKSIZE = 8; // 8K

    // zfs allows sub datasets to be specified and therefore '/' is needed
    public static final byte[] VALID_INNER_CHARS = {'_', '-', '/'};

    public ZfsDriver(
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StorageDriverKind storageDriverKind,
        StltConfigAccessor stltCfgAccessor,
        Crypt crypt
    )
    {
        super(errorReporter, fileSystemWatch, timer, storageDriverKind, stltCfgAccessor, crypt);
    }

    @Override
    public Map<String, String> getTraits(final String identifier) throws StorageException
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(ApiConsts.KEY_STOR_POOL_ALLOCATION_UNIT, String.valueOf(getExtentSize(identifier)));

        return traits;
    }

    @Override
    protected String getExpectedVolumePath(String identifier)
    {
        return File.separator + "dev" +
            File.separator + "zvol" +
            File.separator + pool +
            File.separator + identifier;
    }

    @Override
    protected boolean storageVolumeExists(String identifier, VolumeType volumeType) throws StorageException
    {
        boolean exists;

        final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
        final String[] command = new String[]
            {
                zfsCommand,
                "list",
                "-Hp",
                "-t", volumeType == VolumeType.VOLUME ? "volume" : "snapshot", // specify type
                pool + File.separator + identifier
            };

        try
        {
            OutputData outputData = extCommand.exec(command);

            exists = outputData.exitCode == 0;
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to check if volume exists",
                String.format("Failed to check if volume '%s' exists.", identifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format(
                    "External command: %s",
                    glue(
                        ZfsVolumeInfo.getZfsVolumeInfoCommand(
                            zfsCommand,
                            pool,
                            identifier
                        ),
                        " "
                    )
                ),
                exc
            );
        }

        return exists;
    }

    @Override
    protected VolumeInfo getVolumeInfo(String identifier, boolean failIfNull) throws StorageException
    {
        final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
        VolumeInfo vlmInfo;
        try
        {
            vlmInfo = ZfsVolumeInfo.getInfo(extCommand, zfsCommand, pool, identifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to get volume information",
                String.format("Failed to get information for volume: %s", identifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format(
                    "External command: %s",
                    glue(
                        ZfsVolumeInfo.getZfsVolumeInfoCommand(
                            zfsCommand,
                            pool,
                            identifier
                        ),
                        " "
                    )
                ),
                exc
            );
        }
        return vlmInfo;
    }

    @Override
    protected long getExtentSize(String identifier) throws StorageException
    {
        long extentSize = ZFS_VOLBLOCKSIZE;
        if (storageVolumeExists(identifier, VolumeType.VOLUME))
        {
            final String[] command = new String[]
                {
                    zfsCommand,
                    "get", "volblocksize",
                    "-o", "value",
                    "-Hp",
                    pool + '/' + identifier
                };

            try
            {
                final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
                OutputData outputData = extCommand.exec(command);

                checkExitCode(outputData, command);

                String strBlockSize = new String(outputData.stdoutData);
                extentSize = Long.parseLong(strBlockSize.trim()) >> 10; // we have to return extent size in KiB
            } catch (ChildProcessTimeoutException | IOException exc)
            {
                throw new StorageException(
                    "Failed to get the extent size (zfs 'volblocksize')",
                    String.format("Failed to get the extent size for volume: %s", pool),
                    (exc instanceof ChildProcessTimeoutException) ?
                        "External command timed out" :
                        "External command threw an IOException",
                    null,
                    String.format("External command: %s", glue(command, " ")),
                    exc
                );
            }
        }
        return extentSize;
    }

    @Override
    protected void checkConfiguration(
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
        throws StorageException
    {
        checkCommand(storPoolNamespace, StorageConstants.CONFIG_ZFS_COMMAND_KEY);
        checkPool(storPoolNamespace);
        checkToleranceFactor(storPoolNamespace);
    }


    @Override
    protected void applyConfiguration(
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
    {
        zfsCommand = getZfsCommandFromConfig(storPoolNamespace);
        pool = getPoolFromConfig(storPoolNamespace);
        sizeAlignmentToleranceFactor = uncheckedGetAsInt(
            storPoolNamespace, StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY, sizeAlignmentToleranceFactor
        );
    }

    @Override
    protected String[] getCreateCommand(String identifier, long size)
    {
        return new String[]
        {
            zfsCommand,
            "create",
            "-V", size + "KB",
            pool + File.separator + identifier
        };
    }

    @Override
    protected String[] getResizeCommand(String identifier, long size)
    {
        return new String[]
        {
            zfsCommand, //zfs set volsize=new_size tank/name_of_the_zvol
            "set",
            "volsize=" + size + "KB",
            pool + File.separator + identifier
        };
    }

    @Override
    protected String[] getDeleteCommand(String identifier)
    {
        return new String[]
        {
            zfsCommand,
            "destroy",
            "-f",  // force
            "-r",  // also delete snapshots of this volume
            pool + File.separator + identifier
        };
    }

    @Override
    protected String[] getCreateSnapshotCommand(String identifier, String snapshotName)
    {
        final String zfsSnapName = getQualifiedSnapshotPath(identifier, snapshotName);
        final String[] command = new String[]
        {
            zfsCommand,
            "snapshot",
            zfsSnapName
        };
        return command;
    }

    @Override
    protected String[] getRestoreSnapshotCommand(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        boolean isEncrypted
    )
        throws StorageException
    {
        return new String[]
        {
            zfsCommand,
            "clone",
            getQualifiedSnapshotPath(sourceIdentifier, snapshotName),
            pool + File.separator + targetIdentifier
        };
    }


    @Override
    protected String[] getDeleteSnapshotCommand(String identifier, String snapshotName)
    {
        return new String[]
        {
            zfsCommand,
            "destroy",
            getQualifiedSnapshotPath(identifier, snapshotName)
        };
    }

    @Override
    public boolean snapshotExists(String volumeIdentifier, String snapshotName)
        throws StorageException
    {
        return storageVolumeExists(getSnapshotIdentifier(volumeIdentifier, snapshotName), VolumeType.SNAPSHOT);
    }

    protected void checkPool(Map<String, String> config) throws StorageException
    {
        String newPool = getPoolFromConfig(config).trim();
        try
        {
            Checks.nameCheck(
                newPool,
                1,
                Integer.MAX_VALUE,
                VALID_CHARS,
                VALID_INNER_CHARS
            );
        }
        catch (InvalidNameException ine)
        {
            throw new StorageException(
                "Invalid configuration",
                null,
                String.format("Invalid pool name: %s", newPool),
                "Specify a valid and existing pool name",
                null
            );
        }

        final String[] poolCheckCommand = new String[]
            {
                getZfsCommandFromConfig(config),
                "list",
                "-H", // no headers
                "-o", "name", // name column only
                newPool
            };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData output = extCommand.exec(poolCheckCommand);
            if (output.exitCode != 0)
            {
                throw new StorageException(
                    "Invalid configuration",
                    "Unknown pool",
                    String.format("pool [%s] not found.", newPool),
                    "Specify a valid and existing pool name or create the desired pool manually",
                    null
                );
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to verify pool name",
                null,
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(poolCheckCommand, " ")),
                exc
            );
        }
    }

    private String getQualifiedSnapshotPath(String identifier, String snapshotName)
    {
        return pool + File.separator + getSnapshotIdentifier(identifier, snapshotName);
    }

    @Override
    protected String getSnapshotIdentifier(String identifier, String snapshotName)
    {
        return identifier + "@" + snapshotName;
    }

    private String getRootPool()
    {
        String rootPool = pool;
        int subPoolSep = pool.indexOf(File.separator);
        if (subPoolSep >= 0)
        {
            rootPool = pool.substring(0, subPoolSep);
        }
        return rootPool;
    }

    @Override
    public long getTotalSpace() throws StorageException
    {
        final String rootPool = getRootPool();
        final String[] command = new String[]
            {
                zpoolCommand, "get", "size", "-Hp", rootPool
            };

        long totalSpace;
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            OutputData outputData = extCommand.exec(command);

            checkExitCode(outputData, command);

            String strZPoolFree = new String(outputData.stdoutData);
            String[] cmdSplit = strZPoolFree.split("\t"); // [poolname, property, value, source]
            String strTotalSpace = cmdSplit[2]; // value
            totalSpace = Long.parseLong(strTotalSpace.trim()) >> 10; // we have to return free size in KiB
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to get the total size (zfs 'size')",
                String.format("Failed to get the total size for pool: %s", pool),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }
        return totalSpace;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public long getFreeSpace() throws StorageException
    {
        final String[] command = new String[]
        {
            zfsCommand, "get", "available", "-o", "value", "-Hp", pool
        };

        long freeSpace;
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            OutputData outputData = extCommand.exec(command);

            checkExitCode(outputData, command);

            String strFreeSpace = new String(outputData.stdoutData);
            freeSpace = Long.parseLong(strFreeSpace.trim()) >> 10; // we have to return free size in KiB
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to get the free size (zfs 'available')",
                String.format("Failed to get the free size for pool: %s", pool),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }
        return freeSpace;
    }

    private String getPoolFromConfig(Map<String, String> config)
    {
        return getAsString(config, StorageConstants.CONFIG_ZFS_POOL_KEY, pool);
    }

    private String getZfsCommandFromConfig(Map<String, String> config)
    {
        return getAsString(config, StorageConstants.CONFIG_ZFS_COMMAND_KEY, zfsCommand);
    }
}
