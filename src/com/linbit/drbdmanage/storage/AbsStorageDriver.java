package com.linbit.drbdmanage.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.NegativeTimeException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.SatelliteCoreServices;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FsWatchTimeoutException;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;

public abstract class AbsStorageDriver implements StorageDriver
{
    public static final int EXTENT_SIZE_ALIGN_TOLERANCE_DEFAULT = 2;
    public static final long FILE_EVENT_TIMEOUT_DEFAULT = 15_000;

    public static final byte[] VALID_CHARS = { '_' };
    public static final byte[] VALID_INNER_CHARS = { '-' };

    protected ExtCmd extCommand;
    protected FileSystemWatch fileSystemWatch;
    protected long fileEventTimeout = FILE_EVENT_TIMEOUT_DEFAULT;

    protected int sizeAlignmentToleranceFactor = EXTENT_SIZE_ALIGN_TOLERANCE_DEFAULT;

    @Override
    public void initialize(final SatelliteCoreServices coreSvc) throws StorageException
    {
        extCommand = new ExtCmd(coreSvc.getTimer());
        fileSystemWatch = coreSvc.getFsWatch();
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
        if (size % extent != 0)
        {
            // TODO: log that we are aligning the size
            size = ((size / extent) + 1) * extent; // rounding up needed for zfs
        }

        MetaData.checkMinDrbdSizeNet(size);
        MetaData.checkMaxDrbdSize(size);

        String volumePath = null;

        try
        {
            String[] command = getCreateCommand(identifier, size);
            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            FileEntryGroupBuilder groupBuilder = new FileSystemWatch.FileEntryGroupBuilder();
            groupBuilder.newEntry(getExpectedVolumePath(identifier), Event.CREATE);
            FileEntryGroup entryGroup = groupBuilder.create(fileSystemWatch, null);
            entryGroup.waitGroup(fileEventTimeout);

            final VolumeInfo info = getVolumeInfo(identifier);
            volumePath = info.path;
        }
        catch (ChildProcessTimeoutException | IOException | FsWatchTimeoutException |
            NegativeTimeException | ValueOutOfRangeException | InterruptedException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Could not create volume [%s] with size %d (kiB)",
                    identifier,
                    size
                ),
                exc
            );
        }

        return volumePath;
    }

    @Override
    public void deleteVolume(final String identifier) throws StorageException
    {
        try
        {
            final String[] command = getDeleteCommand(identifier);

            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

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
            // however, the removed volume was the last volume in pool, thus lvm seems to
            // remove /dev/pool also.
            // if we try to register to that (now non-existent) /dev/pool directory our DELETE event
            // we get a NoSuchFileException /dev/pool
            // however, as we tried, and succeeded in removing the volume, we can ignore this
            // exception for now
        }
        catch (ChildProcessTimeoutException | IOException | FsWatchTimeoutException | NegativeTimeException |
               ValueOutOfRangeException | InterruptedException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Could not remove volume [%s]", identifier), exc
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
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Range to check [%d] is invalid", size), exc
            );
        }

        final VolumeInfo info = getVolumeInfo(identifier);

        if (info.size < size)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format(
                    "Volume [%s] has less [%d] than specified [%d] size",
                    identifier, info.size, size
                )
            );
        }

        final long extentSize = getExtentSize();
        final long floorSize = (size / extentSize) * extentSize;

        final long toleratedSize = floorSize + extentSize * sizeAlignmentToleranceFactor;
        if (info.size > toleratedSize)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Volume [%s] is larger [%d] than tolerated [%d]",
                    identifier,
                    info.size,
                    toleratedSize
                )
            );
        }
    }

    @Override
    public String getVolumePath(String identifier) throws StorageException
    {
        VolumeInfo info = getVolumeInfo(identifier);
        return info.path;
    }

    @Override
    public long getSize(String identifier) throws StorageException
    {
        VolumeInfo info = getVolumeInfo(identifier);
        return info.size;
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
        if (!isSnapshotSupported())
        {
            throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
        }

        final String[] command = getCreateSnapshotCommand(identifier, snapshotName);
        try
        {
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to create snapshot [%s] for volume [%s]. ", snapshotName, identifier);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Failed to create snapshot [%s] for volume [%s]. ", snapshotName, identifier),
                exc
            );
        }
    }
    @Override
    public void cloneSnapshot(String snapshotName1, String snapshotName2) throws StorageException
    {
        if (!isSnapshotSupported())
        {
            throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
        }

        final String[] command = getCloneSnapshotCommand(snapshotName1, snapshotName2);
        try
        {
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to clone snapshot [%s] into [%s]. ", snapshotName1, snapshotName2);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Failed to clone snapshot [%s] into [%s]. ", snapshotName1, snapshotName2),
                exc
            );
        }
    }

    // TODO add JavaDoc
    // TODO extract to interface
    @SuppressWarnings("unused")
    public void restoreSnapshot(String snapshotName) throws StorageException
    {
        throw new UnsupportedOperationException("Snapshots are not supported by "+ getClass());
    }

    @Override
    public void deleteSnapshot(String snapshotName) throws StorageException
    {
        if (!isSnapshotSupported())
        {
            throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
        }

        final String[] command = getDeleteSnapshotCommand(snapshotName);
        try
        {
            final OutputData outputData = extCommand.exec(command);
            checkExitCode(outputData, command, "Failed to delete snapshot [%s]. ", snapshotName);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Failed to delete snapshot [%s]. ", snapshotName),
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
            sb.append(String.format("Command '%s' returned with exitcode %d. Error message: %s",
                    gluedCommand,
                    output.exitCode,
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
    protected int checkedGetAsInt(final Map<String, String> map, final String key, int defaultValue) throws StorageException
    {
        try
        {
            return uncheckedGetAsInt(map, key, defaultValue);
        }
        catch (NumberFormatException numberFormatExc)
        {
            // TODO: Detailed error reporting
            throw new StorageException("expected int value", numberFormatExc);
        }
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
        try
        {
            return uncheckedGetAsLong(map, key, defaultValue);
        }
        catch (NumberFormatException numberFormatExc)
        {
            // TODO: Detailed error reporting
            throw new StorageException("expected long value", numberFormatExc);
        }
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
     * @param exFormat
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
                // TODO: Detailed error reporting
                throw new StorageException(String.format("Executable for [%s] not found: %s", key, command));
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
            // TODO: Detailed error reporting
            throw new StorageException("Could not load PATH environment (needed to validate configured commands)");
        }

        String[] split = path.split(File.pathSeparator);

        Path[] folders = new Path[split.length];
        for (int i = 0; i < split.length; i++)
        {
            folders[i] = Paths.get(split[i]);
        }
        return folders;
    }


    protected int checkToleranceFactor(Map<String, String> config) throws StorageException
    {
        int toleranceFactor = checkedGetAsInt(config, StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY,sizeAlignmentToleranceFactor);
        try
        {
            Checks.rangeCheck(toleranceFactor, 1, Integer.MAX_VALUE);
        }
        catch (ValueOutOfRangeException rangeExc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format(
                    "Tolerance factor has to be in range of 1 - %d, but was %d",
                    Integer.MAX_VALUE, toleranceFactor
                ),
                rangeExc
            );
        }
        return toleranceFactor;
    }

    protected abstract String getExpectedVolumePath(String identifier);

    protected abstract VolumeInfo getVolumeInfo(String identifier) throws StorageException;

    protected abstract long getExtentSize() throws StorageException;

    protected abstract void checkConfiguration(Map<String, String> config) throws StorageException;

    protected abstract void applyConfiguration(Map<String, String> config);

    protected abstract String[] getCreateCommand(String identifier, long size);

    protected abstract String[] getDeleteCommand(String identifier);

    protected abstract String[] getCreateSnapshotCommand(String identifier, String snapshotName);

    protected abstract String[] getCloneSnapshotCommand(String snapshotName1, String snapshotName2) throws StorageException;

    protected abstract String[] getDeleteSnapshotCommand(String snapshotName);
}
