package com.linbit.drbdmanage.storage;

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

/**
 * Logical Volume Manager - Storage driver for drbdmanageNG
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmThinDriver extends LvmDriver
{
    public static final String LVM_THIN_POOL_DEFAULT = "drbdthinpool";

    public static final String LVM_CONVERT_DEFAULT = "lvconvert";

    protected String lvmConvertCommand = LVM_CONVERT_DEFAULT;

    protected String baseVolumeGroup = LVM_VOLUME_GROUP_DEFAULT;
    protected String thinPoolName = LVM_THIN_POOL_DEFAULT;

    public LvmThinDriver()
    {
    }

    LvmThinDriver(final ExtCmd ec)
    {
        this.extCommand = ec;
    }

    @Override
    public void startVolume(final String identifier) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmChangeCommand,
            "-ay",              // activate volume
            // this should usually be enough
            "-kn", "-K",        // these parameters are needed to set a
            // snapshot to active and enabled
            qualifiedIdentifier
        };
        try
        {
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to start volume [%s]. ", qualifiedIdentifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to start volume",
                String.format("Failed to start volume [%s]", qualifiedIdentifier),
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
    public void stopVolume(final String identifier) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmChangeCommand,
            "-an",
            qualifiedIdentifier
        };
        try
        {
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to stop volume [%s]. ", qualifiedIdentifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to stop volume",
                String.format("Failed to stop volume [%s]", qualifiedIdentifier),
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
    public Map<String, String> getTraits() throws StorageException
    {
        final long extentSize = getExtentSize();

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
        keySet.add(StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY);
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
            "--virtualsize", size + "k", // -V
            "--thinpool", thinPoolName,  // -T
            "--name", identifier,        // -n
            volumeGroup
        };
    }

    @Override
    protected void checkConfiguration(Map<String, String> config) throws StorageException
    {
        super.checkConfiguration(config);
        checkCommand(config, StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY);
        checkThinPoolEntry(config);
    }

    @Override
    protected void applyConfiguration(Map<String, String> config)
    {
        super.applyConfiguration(config);
        thinPoolName = getAsString(config, StorageConstants.CONFIG_LVM_THIN_POOL_KEY, thinPoolName);
        volumeGroup = getAsString(config, StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY, baseVolumeGroup);
        lvmConvertCommand = getAsString(config, StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY, lvmConvertCommand);
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return true;
    }

    @Override
    protected String[] getCreateSnapshotCommand(String identifier, String snapshotName)
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmCreateCommand,
            "--snapshot",           // -s
            "--name", snapshotName, // -n
            qualifiedIdentifier
        };
        return command;
    }

    @Override
    protected String[] getCloneSnapshotCommand(String snapshotName1, String snapshotName2)
    {
        return getCreateSnapshotCommand(snapshotName1, snapshotName2);
    }

    @Override
    public void restoreSnapshot(String snapshotName) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + snapshotName;
        final String[] command = new String[]
        {
            lvmConvertCommand,
            "--merge", snapshotName
        };

        try
        {
            // FIXME: might take a long time. timeout may happen
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to restore snapshot [%s]. ", qualifiedIdentifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to restore a snapshot",
                String.format("Failed to restore snapshot [%s]", qualifiedIdentifier),
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
    protected String[] getDeleteSnapshotCommand(String identifier)
    {
        return getDeleteCommand(identifier);
    }

    @Override
    public void createSnapshot(String identifier, String snapshotName) throws StorageException
    {
        super.createSnapshot(identifier, snapshotName);
        startVolume(snapshotName);
    }

    @Override
    public void cloneSnapshot(String snapshotName1, String snapshotName2) throws StorageException
    {
        super.cloneSnapshot(snapshotName1, snapshotName2);
        startVolume(snapshotName2);
    }

    private void checkThinPoolEntry(Map<String, String> config) throws StorageException
    {
        final String newThinPoolName = config.get(StorageConstants.CONFIG_LVM_THIN_POOL_KEY).trim();
        if (newThinPoolName != null)
        {
            try
            {
                Checks.nameCheck(
                    newThinPoolName,
                    1,
                    Integer.MAX_VALUE,
                    VALID_CHARS,
                    VALID_INNER_CHARS
                );
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new StorageException(
                    "Invalid configuration",
                    null,
                    String.format("Invalid name for thin pool: %s", newThinPoolName),
                    "Specify a valid and existing thin pool name",
                    null
                );
            }
            final String[] thinPoolCheckCommand = new String[]
                {
                    lvmVgsCommand,
                    "-o", "vg_name",
                    "--noheadings"
                };
            try
            {

                final OutputData output = extCommand.exec(thinPoolCheckCommand);
                final String stdOut = new String(output.stdoutData);
                final String[] lines = stdOut.split("\n");

                boolean found = false;
                for (String line : lines)
                {
                    if (line.trim().equals(newThinPoolName))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    throw new StorageException(
                        "Invalid configuration",
                        "Unknown thin pool",
                        String.format("Thin pool [%s] not found.", newThinPoolName),
                        "Specify a valid and existing thin pool name or create the desired thin pool manually",
                        null
                    );
                }
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                throw new StorageException(
                    "Failed to verify thin pool name",
                    null,
                    (exc instanceof ChildProcessTimeoutException) ?
                        "External command timed out" :
                        "External command threw an IOException",
                    null,
                    String.format("External command: %s", glue(thinPoolCheckCommand, " ")),
                    exc
                );
            }
        }
    }
}
