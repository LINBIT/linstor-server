package com.linbit.drbdmanage.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.InvalidNameException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;

public class ZfsDriver extends AbsStorageDriver
{
    public static final String ZFS_POOL_DEFAULT = "drbdpool";
    public static final String ZFS_COMMAND_DEFAULT = "zfs";

    protected String zfsCommand = ZFS_COMMAND_DEFAULT;

    protected String pool = ZFS_POOL_DEFAULT;

    public ZfsDriver()
    {
    }

    /* default */ ZfsDriver(ExtCmd ec)
    {
        extCommand = ec;
    }

    @Override
    public String createVolume(String identifier, long size) throws StorageException, MaxSizeException, MinSizeException
    {
        final long extent = getExtentSize();
        if (size % extent != 0)
        {
            // TODO: log that we are aligning the size
            size = ((size / extent) + 1) * extent; // rounding up needed for zfs
        }
        return super.createVolume(identifier, size);
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
        return "/dev/zvol/"+pool+"/"+identifier;
    }

    @Override
    protected VolumeInfo getVolumeInfo(String identifier) throws StorageException
    {
        try
        {
            return ZfsVolumeInfo.getInfo(extCommand, zfsCommand, pool, identifier);
        }
        catch (ChildProcessTimeoutException | IOException e)
        {
            throw new StorageException(
                String.format("Could not get Volume info for pool [%s] and identifier [%s]",
                    pool,
                    identifier
                ),
                e
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
            OutputData outputData = extCommand.exec(command);

            checkExitCode(outputData, command);

            String strBlockSize = new String(outputData.stdoutData);
            return Long.parseLong(strBlockSize.trim()) >> 10; // we have to return extent size in KiB
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                String.format("Could not determine block size. Command: %s",
                    glue(command, " ")
                ),
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
        return new String[] {
            zfsCommand,
            "create",
            "-V", size+"KB",
            pool+"/"+identifier
        };
    }

    @Override
    protected String[] getDeleteCommand(String identifier)
    {
        return new String[]{
            zfsCommand,
            "destroy", "-f",
            pool+"/"+identifier
        };
    }

    protected void checkPool(Map<String, String> config) throws StorageException
    {
        String value = config.get(StorageConstants.CONFIG_ZFS_POOL_KEY);
        if (value != null)
        {
            final String pool = value.trim();

            try
            {
                Checks.nameCheck(
                    pool,
                    1,
                    Integer.MAX_VALUE,
                    VALID_CHARS,
                    VALID_INNER_CHARS
                );
            }
            catch (InvalidNameException ine)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("Invalid volumeName [%s]", pool),
                    ine
                );
            }
            try
            {
                final String[] volumeGroupCheckCommand = new String[]
                {
                    zfsCommand,
                    "list",
                    "-H", // no headers
                    "-o", "name", // name column only
                    pool
                };

                final OutputData output = extCommand.exec(volumeGroupCheckCommand);
                if (output.exitCode != 0)
                {
                    // TODO: Detailed error reporting
                    throw new StorageException(
                        String.format("Zfs pool [%s] not found.", pool)
                    );
                }
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("Could not run zfs list %s", pool),
                    exc
                );
            }
        }
    }
}
