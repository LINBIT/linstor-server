package com.linbit.linstor.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.NegativeTimeException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.fsevent.FsWatchTimeoutException;
import com.linbit.linstor.timer.CoreTimer;

public abstract class AbsStorageDriver implements StorageDriver
{
    public static final int EXTENT_SIZE_ALIGN_TOLERANCE_DEFAULT = 2;
    public static final long FILE_EVENT_TIMEOUT_DEFAULT = 15_000;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'-'};

    protected final ErrorReporter errorReporter;
    protected final FileSystemWatch fileSystemWatch;
    protected final CoreTimer timer;
    protected final StorageDriverKind storageDriverKind;
    protected long fileEventTimeout = FILE_EVENT_TIMEOUT_DEFAULT;

    protected int sizeAlignmentToleranceFactor = EXTENT_SIZE_ALIGN_TOLERANCE_DEFAULT;

    public AbsStorageDriver(
        ErrorReporter errorReporterRef,
        FileSystemWatch fileSystemWatchRef,
        CoreTimer timerRef,
        StorageDriverKind storageDriverKindRef
    )
    {
        errorReporter = errorReporterRef;
        fileSystemWatch = fileSystemWatchRef;
        timer = timerRef;
        storageDriverKind = storageDriverKindRef;
    }

    @Override
    public StorageDriverKind getKind()
    {
        return storageDriverKind;
    }

    @Override
    public void startVolume(String identifier) throws StorageException
    {
        // do nothing unless overridden
    }


    @Override
    public void stopVolume(String identifier) throws StorageException
    {
        // do nothing unless overridden
    }


    @Override
    public String createVolume(final String identifier, long size)
        throws StorageException, MaxSizeException, MinSizeException
    {
        final long extent = getExtentSize();
        // Calculate effective size from requested size
        long effSize = size;
        if (effSize % extent != 0)
        {
            long origSize = effSize;
            effSize = ((effSize / extent) + 1) * extent; // rounding up needed for zfs
            errorReporter.logInfo(
                String.format(
                    "Aligning size from %d KiB to %d KiB to be a multiple of extent size %d KiB",
                    origSize,
                    effSize,
                    extent
                )
            );
        }

        MetaData.checkMinDrbdSizeNet(effSize);
        MetaData.checkMaxDrbdSize(effSize);

        String volumePath = null;

        String[] command = getCreateCommand(identifier, effSize);
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData output;
            try
            {
                output = extCommand.exec(command);
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                throw new StorageException(
                    "Failed to create volume",
                    String.format("Failed to create volume [%s] with size %d", identifier, effSize),
                    (exc instanceof ChildProcessTimeoutException) ?
                        "External command timed out" :
                        "External command threw an IOException",
                    null,
                    String.format("External command: %s", glue(command, " ")),
                    exc
                );
            }

            checkExitCode(output, command);

            FileEntryGroupBuilder groupBuilder = new FileSystemWatch.FileEntryGroupBuilder();
            groupBuilder.newEntry(getExpectedVolumePath(identifier), Event.CREATE);
            FileEntryGroup entryGroup = groupBuilder.create(fileSystemWatch, null);
            entryGroup.waitGroup(fileEventTimeout);

            final VolumeInfo info = getVolumeInfo(identifier);
            volumePath = info.getPath();
        }
        catch (NegativeTimeException negTimeExc)
        {
            throw new ImplementationError(
                "Negative waiting time was configured. " +
                "The setConfiguration method should have rejected such a value",
                negTimeExc
            );
        }
        catch (ValueOutOfRangeException valRangeExc)
        {
            throw new ImplementationError(
                "setConfigure method should have rejected a waiting time exceeding long-range (17. Aug. 292278994)",
                valRangeExc
            );
        }
        catch (InterruptedException interExc)
        {
            throw new StorageException(
                "Failed to verify volume creation",
                String.format("Failed to verify the creation of volume [%s] with size %d", identifier, effSize),
                "Verification of volume creation interrupted",
                null,
                String.format(
                    "External command for creating volume executed, file watch listener registered " +
                    "for device [%s] to show up, waiting for device to show up was interrupted",
                    getExpectedVolumePath(identifier)
                ),
                interExc
            );
        }
        catch (FsWatchTimeoutException exc)
        {
            throw new StorageException(
                "Failed to verify volume creation",
                String.format("Failed to verify the creation of volume [%s] with size %d", identifier, effSize),
                "The volume didn't show up in the expected path",
                null,
                String.format(
                    "External command for creating volume executed, file watch listener for device [%s] " +
                    "to show up timed out after %d ms",
                    getExpectedVolumePath(identifier),
                    fileEventTimeout
                ),
                exc
            );
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to verify volume creation",
                String.format("Failed to verify the creation of volume [%s] with size %d", identifier, effSize),
                "IOException occured during registering a file watch listener",
                null,
                String.format("Could not watch path: %s", getExpectedVolumePath(identifier)),
                exc
            );
        }

        return volumePath;
    }

    @Override
    public void deleteVolume(final String identifier) throws StorageException
    {
        final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
        final String[] command = getDeleteCommand(identifier);
        try
        {

            final OutputData output;

            try
            {
                output = extCommand.exec(command);
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                throw new StorageException(
                    "Failed to delete volume",
                    String.format("Failed to delete volume [%s]", identifier),
                    (exc instanceof ChildProcessTimeoutException) ?
                        "External command timed out" :
                        "External command threw an IOException",
                    null,
                    String.format("External command: %s", glue(command, " ")),
                    exc
                );
            }

            if (output.exitCode != 0)
            {
                // before shouting loud that the command failed,
                // we will just verify if the volume is still available.
                // if it is, we shout, if not, we say everything is fine

                VolumeInfo volumeInfo = getVolumeInfo(identifier, false);
                if (volumeInfo != null)
                {
                    checkExitCode(output, command); // will throw the usual message
                }
            }

            FileEntryGroupBuilder groupBuilder = new FileSystemWatch.FileEntryGroupBuilder();
            groupBuilder.newEntry(getExpectedVolumePath(identifier), Event.DELETE);
            FileEntryGroup entryGroup = groupBuilder.create(fileSystemWatch, null);
            entryGroup.waitGroup(fileEventTimeout);
        }
        catch (NoSuchFileException noSuchFileExc)
        {
            // FIXME fileSystemWatch should be able to register on non-existent directories

            // following happend:
            // we successfully executed "lvremove -f pool/volume"
            // however, if the removed volume was the last volume in pool, lvm seems to
            // remove /dev/pool also.
            // if we try to register to that (now non-existent) /dev/pool directory our DELETE event
            // we get a NoSuchFileException /dev/pool
            // however, as we tried, and succeeded in removing the volume, we can ignore this
            // exception for now
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to verify volume deletion",
                String.format("Failed to verify deletion of volume [%s]", identifier),
                "IOException occured during registering file watch event listener",
                null,
                String.format("Could not watch path: %s", getExpectedVolumePath(identifier)),
                exc
            );
        }
        catch (FsWatchTimeoutException exc)
        {
            throw new StorageException(
                "Failed to verify volume deletion",
                String.format("Failed to verify deletion of volume [%s]", identifier),
                "Registerd file watch event listener timed out",
                null,
                String.format("External command executed and returned, but the device [%s] was " +
                    "not removed after %d ms.",
                    getExpectedVolumePath(identifier),
                    fileEventTimeout
                ),
                exc
            );
        }
        catch (NegativeTimeException exc)
        {
            throw new ImplementationError(
                "Negative waiting time was configured. " +
                "The setConfiguration method should have rejected such a value",
                exc
            );
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(
                "setConfigure method should have rejected a waiting time exceeding long-range (17. Aug. 292278994)",
                exc
            );
        }
        catch (InterruptedException exc)
        {
            throw new StorageException(
                "Failed to verify volume deletion",
                String.format("Failed to verify deletion of volume [%s]", identifier),
                "Waiting for device deletion interrupted",
                null,
                String.format("External command executed and returned, but the device [%s] was " +
                    "not removed at the time of the interruption",
                    getExpectedVolumePath(identifier)
                ),
                exc
            );
        }
    }

    @Override
    public void checkVolume(String identifier, long size) throws StorageException
    {
        try
        {
            MetaData.checkMaxDrbdSize(size);
        }
        catch (MaxSizeException exc)
        {
            throw new StorageException(
                "CheckVolume failed",
                null,
                String.format("The size to check [%d] exceeds the current maximum device size: %d KiB",
                    size,
                    MetaData.DRBD_MAX_kiB
                ),
                "Specify a valid size for check",
                null
            );
        }

        final VolumeInfo info = getVolumeInfo(identifier);

        if (info.getSize() < size)
        {
            throw new StorageException(
                "CheckVolume failed",
                String.format("CheckVolume failed for volume [%s]", identifier),
                "Volume does not have the required size",
                null,
                String.format(
                    "Volume [%s] has size %d (KiB) but check required at least %d (KiB)",
                    identifier,
                    info.getSize(),
                    size
                )
            );
        }

        final long extentSize = getExtentSize();
        final long floorSize = (size / extentSize) * extentSize;

        final long toleratedSize = floorSize + extentSize * sizeAlignmentToleranceFactor;
        if (info.getSize() > toleratedSize)
        {
            throw new StorageException(
                "CheckVolume failed",
                String.format("CheckVolume failed for volume [%s]", identifier),
                "Volume is larger than tolerated",
                String.format(
                    "Note: it is possible to increase the tolerance factor. Configuration key: %s",
                    StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY
                ),
                String.format(
                    "Volume [%s] is larger size [%d] than tolerated [%d]",
                    identifier,
                    info.getSize(),
                    toleratedSize
                )
            );
        }
    }

    @Override
    public String getVolumePath(String identifier) throws StorageException
    {
        VolumeInfo info = getVolumeInfo(identifier);
        return info.getPath();
    }

    @Override
    public long getSize(String identifier) throws StorageException
    {
        VolumeInfo info = getVolumeInfo(identifier);
        return info.getSize();
    }

    @Override
    public final void setConfiguration(final Map<String, String> config) throws StorageException
    {
        // split into two functions because multiple inheritance levels can still
        // perform an "all or nothing" applyConfiguration without the need of rollbacks
        checkConfiguration(config);
        applyConfiguration(config);
    }

    @Override
    public void createSnapshot(String identifier, String snapshotName) throws StorageException
    {
        if (!storageDriverKind.isSnapshotSupported())
        {
            throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
        }

        final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
        final String[] command = getCreateSnapshotCommand(identifier, snapshotName);
        try
        {
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(
                outputData, command,
                "Failed to create snapshot [%s] for volume [%s]", snapshotName, identifier
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Snapshot creation failed",
                String.format("Failed to create snapshot \"%s\" of volume \"%s\"", snapshotName, identifier),
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
    public void restoreSnapshot(String sourceIdentifier, String snapshotName, String targetIdentifier)
        throws StorageException
    {
        if (!storageDriverKind.isSnapshotSupported())
        {
            throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
        }
        final String[] command = getRestoreSnapshotCommand(sourceIdentifier, snapshotName, targetIdentifier);

        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(
                outputData,
                command,
                "Failed to restore snapshot [%s] from volume [%s] to volume [%s] ",
                snapshotName,
                sourceIdentifier,
                targetIdentifier
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to restore a snapshot",
                String.format(
                    "Failed to restore snapshot [%s] from volume [%s] to volume [%s]",
                    snapshotName,
                    sourceIdentifier,
                    targetIdentifier
                ),
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
    public void deleteSnapshot(String identifier, String snapshotName) throws StorageException
    {
        if (!storageDriverKind.isSnapshotSupported())
        {
            throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
        }

        final String[] command = getDeleteSnapshotCommand(identifier, snapshotName);
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(
                outputData, command,
                "Failed to delete snapshot [%s] of volume [%s]. ", snapshotName, identifier
            );
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Deleting a snapshot failed",
                String.format("Failed to delete the snapshot [%s] of volume [%s]", snapshotName, identifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command [%s] timed out ", glue(command, " ")),
                exc
            );
        }
    }

    /**
     * Simple check that throws a {@link StorageException} if the exit code is
     * not 0.
     *
     * @param output
     *            The {@link OutputData} which contains the exit code
     * @param command
     *            The <code>String[]</code> that was called (used in the
     *            exception message)
     * @throws StorageException
     */
    protected void checkExitCode(OutputData output, String[] command)
        throws StorageException
    {
        checkExitCode(output, command, null, (Object[]) null);
    }

    /**
     * Simple check that throws a {@link StorageException} if the exit code is
     * not 0.
     *
     * @param output
     *            The {@link OutputData} which contains the exit code
     * @param command
     *            The <code>String[]</code> that was called (used in the
     *            exception message)
     * @param format
     *            An optional additional message which is printed before the default
     *            "command '%s' returned with exitcode %d. Error message: %s" message
     * @param args
     *            The arguments for the format parameter
     * @throws StorageException
     *            If the exitCode of output is not 0, a {@link StorageException} is thrown.
     */
    protected void checkExitCode(OutputData output, String[] command, String format, Object... args)
        throws StorageException
    {
        if (output.exitCode != 0)
        {
            String gluedCommand = glue(command, " ");
            StringBuilder sb = new StringBuilder();
            if (format != null && format.length() > 0)
            {
                sb.append(String.format(format, args));
            }
            sb.append(
                String.format(
                    "Command '%s' returned with exitcode %d. %n%n" +
                        "Standard out: %n" +
                        "%s" +
                        "%n%n" +
                        "Error message: %n" +
                        "%s" +
                        "%n",
                    gluedCommand,
                    output.exitCode,
                    new String(output.stdoutData),
                    new String(output.stderrData)
                )
            );
            throw new StorageException(sb.toString());
        }
    }

    /**
     * Glues the given <code>String[]</code> with a specified delimiter
     *
     * @param array
     * @param delimiter
     * @return
     */
    protected String glue(String[] array, String delimiter)
    {
        StringBuilder sb = new StringBuilder();
        for (String element : array)
        {
            sb.append(element).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    /**
     * If the key is present, this method returns its value, otherwise the defaultValue
     * parameter is returned
     *
     * @param map
     * @param key
     * @param defaultValue
     * @return
     */
    protected String getAsString(Map<String, String> map, String key, String defaultValue)
    {
        String value = map.get(key);
        if (value == null)
        {
            value = defaultValue;
        }
        return value;
    }

    /**
     *
     * Checks if the given key exists and is parsable as int.
     *
     * @param map
     * @param key
     * @param defaultValue
     * @return
     * @throws StorageException
     */
    protected int checkedGetAsInt(final Map<String, String> map, final String key, int defaultValue)
        throws StorageException
    {
        int result;
        try
        {
            result = uncheckedGetAsInt(map, key, defaultValue);
        }
        catch (NumberFormatException numberFormatExc)
        {
            throw new StorageException(
                "Invalid configuration",
                String.format("Key [%s] was expected to contain an int value, but was [%s]", key, map.get(key)),
                String.format("Failed to parse [%s] as an int value", map.get(key)),
                "Specify a valid value for the key",
                null,
                numberFormatExc
            );
        }
        return result;
    }

    /**
     * If the key is present, this method returns its value, otherwise the defaultValue
     * parameter is returned
     *
     * @param map
     * @param key
     * @param defaultValue
     * @return
     */
    protected int uncheckedGetAsInt(Map<String, String> map, String key, int defaultValue)
    {
        String value = map.get(key);
        int ret;
        if (value != null)
        {
            ret = Integer.parseInt(value);
        }
        else
        {
            ret = defaultValue;
        }
        return ret;
    }

    /**
     * If the key is present, this method returns its value, otherwise the defaultValue
     * parameter is returned
     *
     * @param map
     * @param key
     * @param defaultValue
     * @return
     * @throws StorageException
     */
    protected long checkedGetAsLong(Map<String, String> map, String key, long defaultValue) throws StorageException
    {
        long result;
        try
        {
            result = uncheckedGetAsLong(map, key, defaultValue);
        }
        catch (NumberFormatException numberFormatExc)
        {
            throw new StorageException(
                "Invalid configuration",
                String.format("Key [%s] was expected to contain an long value, but was [%s]", key, map.get(key)),
                String.format("Failed to parse [%s] as an long value", map.get(key)),
                "Specify a valid value for the key",
                null,
                numberFormatExc
            );
        }
        return result;
    }

    /**
     * If the key is present, this method returns its value, otherwise the defaultValue
     * parameter is returned
     *
     * @param map
     * @param key
     * @param defaultValue
     * @return
     */
    protected long uncheckedGetAsLong(Map<String, String> map, String key, long defaultValue)
    {
        String value = map.get(key);
        long ret;
        if (value != null)
        {
            ret = Long.parseLong(value);
        }
        else
        {
            ret = defaultValue;
        }
        return ret;
    }

    /**
     * Performs a {@link Map#get} and throws a StorageException if the value is
     * null or if the value is not found as a path on the system or the file is not executable.
     *
     * @param map
     * @param key
     * @throws StorageException
     */
    protected void checkCommand(
        final Map<String, String> map,
        final String key
    ) throws StorageException
    {
        String command = map.get(key);
        if (command != null)
        {
            Path[] pathFolders = getPathFolders();

            boolean commandFound = false;
            for (Path folder : pathFolders)
            {
                Path commandPath = folder.resolve(command);
                if (Files.exists(commandPath) && Files.isExecutable(commandPath))
                {
                    commandFound = true;
                    break;
                }
            }
            if (!commandFound)
            {
                String[] pathStrings = new String[pathFolders.length];
                for (int idx = 0; idx < pathStrings.length; ++idx)
                {
                    pathStrings[idx] = pathFolders[idx].toAbsolutePath().toString();
                }
                throw new StorageException(
                    "Command not found",
                    String.format(
                        "Executable for command with key [%s] and value [%s] was not found",
                        key,
                        command
                    ),
                    String.format(
                        "The command [%s] was not found in following directories: %s",
                        command,
                        glue(pathStrings, ", ")
                    ),
                    "Specify an existing command or extend the PATH variable",
                    null
                );
            }
        }
    }

    protected Path[] getPathFolders() throws StorageException
    {
        String path = System.getenv("PATH");
        if (path == null)
        {
            path = System.getenv("path");
        }
        if (path == null)
        {
            path = System.getenv("Path");
        }
        if (path == null)
        {
            throw new StorageException(
                "PATH environment variable not found",
                "No environment variable called 'PATH', 'path' or 'Path' was found",
                null,
                "Set any of the following environment variables accordingly: 'PATH', 'path' or 'Path'",
                "The PATH variable is needed to verify the existence of the configured commands"
            );
        }

        String[] split = path.split(File.pathSeparator);

        Path[] folders = new Path[split.length];
        for (int idx = 0; idx < split.length; idx++)
        {
            folders[idx] = Paths.get(split[idx]);
        }
        return folders;
    }


    protected void checkToleranceFactor(Map<String, String> config) throws StorageException
    {
        int toleranceFactor = checkedGetAsInt(
            config, StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY, sizeAlignmentToleranceFactor
        );
        try
        {
            Checks.rangeCheck(toleranceFactor, 1, Integer.MAX_VALUE);
        }
        catch (ValueOutOfRangeException rangeExc)
        {
            throw new StorageException(
                "Tolerance factor is out of range",
                String.format(
                    "Tolerance factor has to be in range of 1 - %d, but was %d",
                    Integer.MAX_VALUE,
                    toleranceFactor
                ),
                null,
                "Specify a tolerance factor within the range of 1 - " + Integer.MAX_VALUE,
                null,
                rangeExc
            );
        }
    }

    protected VolumeInfo getVolumeInfo(String identifier) throws StorageException
    {
        return getVolumeInfo(identifier, true);
    }

    protected abstract String getExpectedVolumePath(String identifier);

    protected abstract VolumeInfo getVolumeInfo(String identifier, boolean failIfNull) throws StorageException;

    protected abstract long getExtentSize() throws StorageException;

    protected abstract void checkConfiguration(Map<String, String> config) throws StorageException;

    protected abstract void applyConfiguration(Map<String, String> config);

    protected abstract String[] getCreateCommand(String identifier, long size);

    protected abstract String[] getDeleteCommand(String identifier);

    protected abstract String[] getCreateSnapshotCommand(String identifier, String snapshotName);

    protected abstract String[] getRestoreSnapshotCommand(
        String sourceIdentifier,
        String snapshotName,
        String identifier
    )
        throws StorageException;

    protected abstract String[] getDeleteSnapshotCommand(String identifier, String snapshotName);
}
