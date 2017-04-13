package com.linbit.drbdmanage.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
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

    private String thinPoolName = LVM_THIN_POOL_DEFAULT;

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
    public void setConfiguration(final Map<String, String> config) throws StorageException
    {
        // first parse the commands as those may get called when parsing other configurations like volumeGroup

        final String configLvmCreateCommand = checkedCommandAndGet(config, LvmConstants.CONFIG_LVM_CREATE_COMMAND_KEY, lvmCreateCommand);
        final String configLvmRemoveCommand = checkedCommandAndGet(config, LvmConstants.CONFIG_LVM_REMOVE_COMMAND_KEY, lvmRemoveCommand);
        final String configLvmChangeCommand = checkedCommandAndGet(config, LvmConstants.CONFIG_LVM_CHANGE_COMMAND_KEY, lvmChangeCommand);
        final String configLvmLvsCommand = checkedCommandAndGet(config, LvmConstants.CONFIG_LVM_LVS_COMMAND_KEY, lvmLvsCommand);
        final String configLvmVgsCommand = checkedCommandAndGet(config, LvmConstants.CONFIG_LVM_VGS_COMMAND_KEY, lvmVgsCommand);
        final String configVolumeGroup = checkVolumeGroupEntry(config, volumeGroup);
        final String configThinPool = checkThinPoolEntry(config, thinPoolName);
        final int configToleranceFactor = checkedGetAsInt(config, LvmConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY,sizeAlignmentToleranceFactor);
        try
        {
            Checks.rangeCheck(configToleranceFactor, 1, Integer.MAX_VALUE);
        }
        catch (ValueOutOfRangeException valueOORangeExc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format(
                    "Tolerance factor has to be in range of 1 - %d, but was %d",
                    Integer.MAX_VALUE, configToleranceFactor
                ),
                valueOORangeExc
            );
        }

        // if no exception was thrown until now, apply all config.
        // this way we do not have to rollback partial config entries

        lvmCreateCommand = configLvmCreateCommand;
        lvmRemoveCommand = configLvmRemoveCommand;
        lvmChangeCommand = configLvmChangeCommand;
        lvmLvsCommand = configLvmLvsCommand;
        lvmVgsCommand = configLvmVgsCommand;

        volumeGroup = configVolumeGroup;
        thinPoolName = configThinPool;
        sizeAlignmentToleranceFactor = configToleranceFactor;
    }


    private String checkThinPoolEntry(Map<String, String> config, String defaultReturn) throws StorageException
    {
        final String value = config.get(LvmConstants.CONFIG_THIN_POOL_KEY);
        final String ret;
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
                ret = value;
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
        else
        {
            ret = defaultReturn;
        }
        return ret;
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        final HashSet<String> keySet = new HashSet<>();

        keySet.add(LvmConstants.CONFIG_LVM_CREATE_COMMAND_KEY);
        keySet.add(LvmConstants.CONFIG_LVM_REMOVE_COMMAND_KEY);
        keySet.add(LvmConstants.CONFIG_LVM_CHANGE_COMMAND_KEY);
        keySet.add(LvmConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        keySet.add(LvmConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        keySet.add(LvmConstants.CONFIG_VOLUME_GROUP_KEY);
        keySet.add(LvmConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);
        keySet.add(LvmConstants.CONFIG_THIN_POOL_KEY);

        return keySet;
    }

    @Override
    protected String[] getLvcreateCommand(String identifier, long size)
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
}
