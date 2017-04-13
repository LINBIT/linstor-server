package com.linbit.drbdmanage.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.linbit.Checks;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.InvalidNameException;
import com.linbit.NegativeTimeException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FsWatchTimeoutException;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;

public class LvmDriver implements StorageDriver
{
    public static final String LVM_ALIGN_TOLERANCE_DEFAULT = "2";
    public static final String LVM_VOLUME_GROUP_DEFAULT = "drbdpool";

    public static final String LVM_CREATE_DEFAULT = "lvcreate";
    public static final String LVM_REMOVE_DEFAULT = "lvremove";
    public static final String LVM_CHANGE_DEFAULT = "lvchange";
    public static final String LVM_LVS_DEFAULT = "lvs";
    public static final String LVM_VGS_DEFAULT = "vgs";
    public static final String LVM_FILE_EVENT_TIMEOUT_DEFAULT = "15000";

    public static final byte[] VALID_CHARS = { '_' };
    public static final byte[] VALID_INNER_CHARS = { '-' };

    protected String lvmCreateCommand = LVM_CREATE_DEFAULT;
    protected String lvmLvsCommand = LVM_LVS_DEFAULT;
    protected String lvmVgsCommand = LVM_VGS_DEFAULT;
    protected String lvmRemoveCommand = LVM_REMOVE_DEFAULT;
    protected String lvmChangeCommand = LVM_CHANGE_DEFAULT;

    protected String volumeGroup = LVM_VOLUME_GROUP_DEFAULT;
    protected int sizeAlignmentToleranceFactor = Integer.parseInt(LVM_ALIGN_TOLERANCE_DEFAULT);

    protected ExtCmd extCommand;
    protected FileSystemWatch fileSystemWatch;
    protected long fileEventTimeout = Long.parseLong(LVM_FILE_EVENT_TIMEOUT_DEFAULT);

    public LvmDriver()
    {
    }

    LvmDriver(final ExtCmd ec) throws StorageException
    {
        this.extCommand = ec;
    }

    public void initialize(final CoreServices coreSvc) throws StorageException
    {
        extCommand = new ExtCmd(coreSvc.getTimer());
        fileSystemWatch = coreSvc.getFsWatch();
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
        final String configvolumeGroup = checkVolumeGroupEntry(config, volumeGroup);
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

        volumeGroup = configvolumeGroup;
        sizeAlignmentToleranceFactor = configToleranceFactor;
    }

    @Override
    public void startVolume(final String identifier) throws StorageException
    {
        // ignored in thick mode
    }

    @Override
    public void stopVolume(final String identifier) throws StorageException
    {
        // ignored in thick mode
    }

    @Override
    public String createVolume(final String identifier, final long size)
        throws StorageException, MaxSizeException, MinSizeException
    {
        MetaData.checkMinDrbdSizeNet(size);
        MetaData.checkMaxDrbdSize(size);

        String lvPath = null;

        try
        {
            String[] command = getLvcreateCommand(identifier, size);
            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            FileEntryGroupBuilder groupBuilder = new FileSystemWatch.FileEntryGroupBuilder();
            groupBuilder.newEntry("/dev/"+volumeGroup+"/"+identifier, Event.CREATE);
            FileEntryGroup entryGroup = groupBuilder.create(fileSystemWatch, null);
            entryGroup.waitGroup(fileEventTimeout);

            final LvsInfo info = getLvsInfoByIdentifier(identifier);
            lvPath = info.path;
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

        return lvPath;
    }

    @Override
    public void deleteVolume(final String identifier) throws StorageException
    {
        try
        {
            final String[] command = new String[]
            {
                lvmRemoveCommand,
                "-f", // skip the "are you sure?" thing...
                volumeGroup + File.separator + identifier
            };

            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            FileEntryGroupBuilder groupBuilder = new FileSystemWatch.FileEntryGroupBuilder();
            groupBuilder.newEntry("/dev/"+volumeGroup+"/"+identifier, Event.DELETE);
            FileEntryGroup entryGroup = groupBuilder.create(fileSystemWatch, null);
            entryGroup.waitGroup(fileEventTimeout);
        }
        catch (ChildProcessTimeoutException | IOException | FsWatchTimeoutException | NegativeTimeException | ValueOutOfRangeException | InterruptedException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Could not remove volume [%s]", identifier), exc
            );
        }
    }

    @Override
    public void checkVolume(final String identifier, final long size)
        throws StorageException
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

        final LvsInfo info = getLvsInfoByIdentifier(identifier);

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
    public String getVolumePath(final String identifier) throws StorageException
    {
        final LvsInfo info = getLvsInfoByIdentifier(identifier);
        return info.path;
    }

    @Override
    public long getSize(final String identifier) throws StorageException
    {
        final LvsInfo info = getLvsInfoByIdentifier(identifier);
        return info.size;
    }

    @Override
    public Map<String, String> getTraits() throws StorageException
    {
        long extentSize = getExtentSize();

        final HashMap<String, String> traits = new HashMap<>();
        traits.put(DriverTraits.KEY_PROV, DriverTraits.PROV_FAT);

        final String size = Long.toString(extentSize);
        traits.put(DriverTraits.KEY_ALLOC_UNIT, size);

        return traits;
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

        return keySet;
    }

    protected String[] getLvcreateCommand(final String identifier, final long size)
    {
        return new String[]
        {
            lvmCreateCommand,
            "--size", size + "k",
            "-n", identifier,
            volumeGroup
        };
    }


    /**
     * Runs an <code>lvs</code> command, fetches the results and returns
     * the {@link LvsInfo} of the specified identifier.
     *
     * If the identifier could not be found in the list, a
     * {@link StorageException}
     * is thrown
     *
     * @param identifier
     * @return non-null {@link LvsInfo} of the requested identifier
     * @throws StorageException
     */
    protected LvsInfo getLvsInfoByIdentifier(String identifier)
        throws StorageException
    {
        LvsInfo info = null;

        HashMap<String, LvsInfo> infoMap;
        try
        {
            infoMap = LvsInfo.getAllInfo(extCommand, lvmLvsCommand, volumeGroup);
            info = infoMap.get(identifier);

            if (info == null)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("Volume [%s] not found", identifier)
                );
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Could not run [%s] for volume group [%s]",
                    lvmLvsCommand,
                    volumeGroup
                ),
                exc
            );
        }
        return info;
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
        if (output.exitCode != 0)
        {
            String gluedCommand = glue(command, " ");
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("%s returned with exitcode %d", gluedCommand, output.exitCode)
            );
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
     * performs a <code>vgs</code> command and returns the extent size of the
     * current volume group.
     *
     * @return
     * @throws StorageException
     */
    protected long getExtentSize() throws StorageException
    {
        long extentSize = 0;
        try
        {
            final String[] command = new String[]
            {
                lvmVgsCommand,
                volumeGroup,
                "-o", "vg_extent_size",
                "--units", "k",
                "--noheadings"
            };

            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            String rawOut = new String(output.stdoutData);
            // cut everything after the decimal dot
            rawOut = rawOut.substring(0, rawOut.indexOf("."));
            extentSize = Long.parseLong(rawOut);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            // TODO: Detailed error reporting
            throw new StorageException(
                String.format("Could not call [%s]", lvmVgsCommand), exc
            );
        }
        return extentSize;
    }


    /**
     * Checks if the given map contains the key for volume group
     * ({@link LvmConstants#CONFIG_VOLUME_GROUP_KEY}) and
     * verifies if the volume group exists (with <code>vgs</code> command) and
     * performs a {@link Checks#nameCheck}.
     *
     * @param config
     * @throws StorageException
     */
    protected String checkVolumeGroupEntry(final Map<String, String> config, final String defaultReturn) throws StorageException
    {
        final String value = config.get(LvmConstants.CONFIG_VOLUME_GROUP_KEY);
        final String ret;
        if (value != null)
        {
            final String volumeGroup = value.trim();

            try
            {
                Checks.nameCheck(
                    volumeGroup,
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
                    String.format("Invalid volumeName [%s]", volumeGroup),
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
                    if (line.trim().equals(volumeGroup))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    // TODO: Detailed error reporting
                    throw new StorageException(
                        String.format("Volume group [%s] not found.", volumeGroup)
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

    /**
     * Performs a {@link Map#get} and throws a StorageException if the value is
     * null.
     *
     * @param map
     * @param key
     * @param exFormat
     * @return
     * @throws StorageException
     */
    protected String checkedCommandAndGet(
        final Map<String, String> map,
        final String key,
        final String defaultValue
    ) throws StorageException
    {
        String command = map.get(key);
        if (command == null)
        {
            command = defaultValue;
        }
        else
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
        return command;
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
        String value = map.get(key);
        int iValue;
        if (value == null)
        {
            iValue = defaultValue;
        }
        else
        {
            try
            {
                iValue = Integer.parseInt(value);
            }
            catch (NumberFormatException numberExc)
            {
                // TODO: Detailed error reporting
                throw new StorageException(
                    String.format("[%s] not a number", value),
                    numberExc
                );
            }
        }
        return iValue;
    }
}
