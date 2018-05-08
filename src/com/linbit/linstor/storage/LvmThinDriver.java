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
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

/**
 * Logical Volume Manager - Storage driver for linstor
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmThinDriver extends LvmDriver
{
    public static final String LVM_THIN_POOL_DEFAULT = "linstorthinpool";

    public static final String LVM_CONVERT_DEFAULT = "lvconvert";

    private static final String ID_SNAP_DELIMITER = "_";

    protected String lvmConvertCommand = LVM_CONVERT_DEFAULT;

    protected String thinPoolName = LVM_THIN_POOL_DEFAULT;

    public LvmThinDriver(
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StorageDriverKind storageDriverKind
    )
    {
        super(errorReporter, fileSystemWatch, timer, storageDriverKind);
    }

    @Override
    public void startVolume(final String identifier, String cryptKey) throws StorageException
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmChangeCommand,
            "-ay",  // activate volume
            "-K",   // these parameters are needed to set a
            // snapshot to active and enabled
            qualifiedIdentifier
        };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
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
        super.startVolume(identifier, cryptKey); // call to possibly open dm-crypt
    }

    @Override
    public void stopVolume(final String identifier, boolean isEncrypted) throws StorageException
    {
        super.stopVolume(identifier, isEncrypted); // call to possibly close dm-crypt

        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmChangeCommand,
            "-an",
            qualifiedIdentifier
        };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
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

        final String size = Long.toString(extentSize);
        traits.put(DriverTraits.KEY_ALLOC_UNIT, size);

        return traits;
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
        thinPoolName = getThinPoolNameFromConfig(config);
        lvmConvertCommand = getAsString(config, StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY, lvmConvertCommand);
    }

    @Override
    protected String getSnapshotIdentifier(String identifier, String snapshotName, boolean isEncrypted)
    {
        String path;
        if (isEncrypted)
        {
            path = getCryptVolumePath(identifier + ID_SNAP_DELIMITER + snapshotName);
        }
        else
        {
            path = identifier + ID_SNAP_DELIMITER + snapshotName;
        }
        return path;
    }

    @Override
    protected String[] getCreateSnapshotCommand(String identifier, String snapshotName, boolean isEncrypted)
    {
        final String qualifiedIdentifier = volumeGroup + File.separator + identifier;
        final String[] command = new String[]
        {
            lvmCreateCommand,
            "--snapshot",           // -s
            "--name", getSnapshotIdentifier(identifier, snapshotName, isEncrypted), // -n
            qualifiedIdentifier
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
    {
        final String[] command = new String[]
        {
            lvmCreateCommand,
            "--snapshot",           // -s
            "--name", targetIdentifier, // -n
            volumeGroup + File.separator + getSnapshotIdentifier(sourceIdentifier, snapshotName, isEncrypted)
        };
        return command;
    }

    @Override
    protected String[] getDeleteSnapshotCommand(String identifier, String snapshotName, boolean isEncrypted)
    {
        return getDeleteCommand(getSnapshotIdentifier(identifier, snapshotName, isEncrypted));
    }

    @Override
    public void restoreSnapshot(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        String cryptKey
    )
        throws StorageException
    {
        super.restoreSnapshot(sourceIdentifier, snapshotName, targetIdentifier, cryptKey);
        startVolume(targetIdentifier, cryptKey);
    }

    private void checkThinPoolEntry(Map<String, String> config) throws StorageException
    {
        super.checkVolumeGroupEntry(config);

        String newThinPoolName = getThinPoolNameFromConfig(config).trim();
        String newBasePoolName = getVolumeGroupFromConfig(config).trim();
        checkName(newThinPoolName);

        checkThinPoolExists(config, newBasePoolName + "/" + newThinPoolName);
    }

    private void checkThinPoolExists(Map<String, String> config, String newFullThinPoolId) throws StorageException
    {
        final String[] checkCommand = new String[]
            {
                getLvmLvsCommandFromConfig(config),
                newFullThinPoolId
            };
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData output = extCommand.exec(checkCommand);
            if (output.exitCode != 0)
            {
                throw new StorageException(
                    "Invalid configuration",
                    "Unknown thin pool",
                    String.format("Thin pool [%s] not found.", newFullThinPoolId),
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
                        String.format("External command: %s", glue(checkCommand, " ")),
                        exc
                );
        }
    }

    private void checkName(String newThinPoolName) throws StorageException
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
    }

    private String getThinPoolNameFromConfig(Map<String, String> config)
    {
        return getAsString(config, StorageConstants.CONFIG_LVM_THIN_POOL_KEY, thinPoolName);
    }
}
