package com.linbit.linstor.storage;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;

public class ZfsDriver extends AbsStorageDriver
{
    public static final String ZFS_POOL_DEFAULT = "linstorpool";
    public static final String ZFS_COMMAND_DEFAULT = "zfs";

    protected String zfsCommand = ZFS_COMMAND_DEFAULT;

    protected String pool = ZFS_POOL_DEFAULT;

    public ZfsDriver()
    {
    }

    @Override
    public Map<String, String> getTraits() throws StorageException
    {
        final HashMap<String, String> traits = new HashMap<>();
        traits.put(DriverTraits.KEY_PROV, DriverTraits.PROV_FAT);
        traits.put(DriverTraits.KEY_ALLOC_UNIT, String.valueOf(getExtentSize()));

        return traits;
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        HashSet<String> keys = new HashSet<>();
        keys.add(StorageConstants.CONFIG_ZFS_POOL_KEY);
        keys.add(StorageConstants.CONFIG_ZFS_COMMAND_KEY);
        keys.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);

        //TODO zfs offers a lot of editable properties, for example recordsize..

        return keys;
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
    protected VolumeInfo getVolumeInfo(String identifier) throws StorageException
    {
        final ExtCmd extCommand = new ExtCmd(coreSvcs.getTimer(), coreSvcs.getErrorReporter());
        try
        {
            return ZfsVolumeInfo.getInfo(extCommand, zfsCommand, pool, identifier);
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
    }

    @Override
    protected long getExtentSize() throws StorageException
    {
        final String[] command = new String[]
        {
            zfsCommand,
            "get", "recordsize", //TODO check if recordsize really is the extent size
            "-o", "value",
            "-Hp",
            pool
        };

        try
        {
            final ExtCmd extCommand = new ExtCmd(coreSvcs.getTimer(), coreSvcs.getErrorReporter());
            OutputData outputData = extCommand.exec(command);

            checkExitCode(outputData, command);

            String strBlockSize = new String(outputData.stdoutData);
            return Long.parseLong(strBlockSize.trim()) >> 10; // we have to return extent size in KiB
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to get the extent size (zfs 'recordsize')",
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

    @Override
    protected void checkConfiguration(Map<String, String> config) throws StorageException
    {
        checkCommand(config, StorageConstants.CONFIG_ZFS_COMMAND_KEY);
        checkPool(config);
        checkToleranceFactor(config);
    }


    @Override
    protected void applyConfiguration(Map<String, String> config)
    {
        zfsCommand = getAsString(config, StorageConstants.CONFIG_ZFS_COMMAND_KEY, zfsCommand);
        pool = getAsString(config, StorageConstants.CONFIG_ZFS_POOL_KEY, pool);
        sizeAlignmentToleranceFactor = uncheckedGetAsInt(config, StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY, sizeAlignmentToleranceFactor);
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
    public boolean isSnapshotSupported()
    {
        return true;
    }

    @Override
    public void createSnapshot(String identifier, String snapshotName) throws StorageException
    {
        super.createSnapshot(identifier, snapshotName);
//        super.restoreSnapshot(getQualifiedSnapshotPath(identifier, snapshotName),, snapshotName);
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
    protected String[] getRestoreSnapshotCommand(String sourceIdentifier, String snapshotName, String targetIdentifier) throws StorageException
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

    protected void checkPool(Map<String, String> config) throws StorageException
    {
        String newPool = config.get(StorageConstants.CONFIG_ZFS_POOL_KEY);
        if (newPool != null)
        {
            newPool = newPool.trim();
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
                    zfsCommand,
                    "list",
                    "-H", // no headers
                    "-o", "name", // name column only
                    newPool
                };
            try
            {
                final ExtCmd extCommand = new ExtCmd(coreSvcs.getTimer(), coreSvcs.getErrorReporter());
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
    }

    private String getQualifiedSnapshotPath(String identifier, String snapshotName)
    {
        return pool + File.separator + identifier + "@" + snapshotName;
    }
}
