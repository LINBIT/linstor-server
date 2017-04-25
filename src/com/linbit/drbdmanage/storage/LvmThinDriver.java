package com.linbit.drbdmanage.storage;

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

/**
 * Logical Volume Manager - Storage driver for drbdmanageNG
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmThinDriver extends LvmDriver
{
    public static final String LVM_THIN_POOL_DEFAULT = "drbdthinpool";

    protected String baseVolumeGroup = LVM_VOLUME_GROUP_DEFAULT;
    protected String thinPoolName = LVM_THIN_POOL_DEFAULT;

    public LvmThinDriver()
    {
    }

    LvmThinDriver(final ExtCmd ec) throws StorageException
    {
        this.extCommand = ec;
    }

    @Override
    public void startVolume(final String identifier) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + "/" + identifier;
        try
        {
            final OutputData outputData = extCommand.exec(lvmChangeCommand, "-ay", qualifiedIdentifier);
            if (outputData.exitCode != 0)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("Volume [%s] could not be started.", qualifiedIdentifier)
                );
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Failed to start volume [%s]", qualifiedIdentifier), exc
            );
        }
    }

    @Override
    public void stopVolume(final String identifier) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + "/" + identifier;
        try
        {
            final OutputData outputData = extCommand.exec(lvmChangeCommand, "-an", qualifiedIdentifier);
            if (outputData.exitCode != 0)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("Volume [%s] could not be stopped.", qualifiedIdentifier)
                );
            }

        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Failed to stop volume [%s]", qualifiedIdentifier),
                exc
            );
        }
    }

    @Override
    public Map<String, String> getTraits() throws StorageException
    {
        long extentSize = getExtentSize();

        final HashMap<String, String> traits = new HashMap<>();
        traits.put(DriverTraits.KEY_PROV, DriverTraits.PROV_THIN);

        final String size = Long.toString(extentSize);
        traits.put(DriverTraits.KEY_ALLOC_UNIT, size);

        return traits;
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        final HashSet<String> keySet = new HashSet<>();

        keySet.add(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        keySet.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_THIN_POOL_KEY);

        return keySet;
    }

    @Override
    protected String[] getCreateCommand(String identifier, long size)
    {
        return new String[]
        {
            lvmCreateCommand,
            "--virtualsize", size + "k",
            "--thinpool", thinPoolName,
            "-n", identifier,
            volumeGroup
        };
    }

    @Override
    protected void checkConfiguration(Map<String, String> config) throws StorageException
    {
        super.checkConfiguration(config);
        checkThinPoolEntry(config);
    }

    @Override
    protected void applyConfiguration(Map<String, String> config)
    {
        super.applyConfiguration(config);
        thinPoolName = getAsString(config, StorageConstants.CONFIG_LVM_THIN_POOL_KEY, thinPoolName);
        baseVolumeGroup = getAsString(config, StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY, baseVolumeGroup);

        volumeGroup = baseVolumeGroup + "/" + thinPoolName;
    }

    private void checkThinPoolEntry(Map<String, String> config) throws StorageException
    {
        final String value = config.get(StorageConstants.CONFIG_LVM_THIN_POOL_KEY);
        if (value != null)
        {
            final String thinPoolName = value.trim();

            try
            {
                Checks.nameCheck(
                    thinPoolName,
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
                    String.format("Invalid thin pool name [%s]", thinPoolName),
                    ine
                );
            }
            try
            {
                final String[] volumeGroupCheckCommand = new String[]
                {
                    lvmVgsCommand,
                    "-o", "vg_name",
                    "--noheadings"
                };

                final OutputData output = extCommand.exec(volumeGroupCheckCommand);
                final String stdOut = new String(output.stdoutData);
                final String[] lines = stdOut.split("\n");
                boolean found = false;
                for (String line : lines)
                {
                    if (line.trim().equals(thinPoolName))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    // TODO: Detailed error reporting
                    throw new StorageException(
                        String.format("Volume group [%s] not found.", thinPoolName)
                    );
                }
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("Could not run [%s]", lvmVgsCommand),
                    exc
                );
            }
        }
    }
}
