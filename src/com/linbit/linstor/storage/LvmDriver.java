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

public class LvmDriver extends AbsStorageDriver
{
    public static final String LVM_VOLUME_GROUP_DEFAULT = "drbdpool";

    public static final String LVM_CREATE_DEFAULT = "lvcreate";
    public static final String LVM_REMOVE_DEFAULT = "lvremove";
    public static final String LVM_CHANGE_DEFAULT = "lvchange";
    public static final String LVM_LVS_DEFAULT = "lvs";
    public static final String LVM_VGS_DEFAULT = "vgs";

    protected String lvmCreateCommand = LVM_CREATE_DEFAULT;
    protected String lvmLvsCommand = LVM_LVS_DEFAULT;
    protected String lvmVgsCommand = LVM_VGS_DEFAULT;
    protected String lvmRemoveCommand = LVM_REMOVE_DEFAULT;
    protected String lvmChangeCommand = LVM_CHANGE_DEFAULT;

    protected String volumeGroup = LVM_VOLUME_GROUP_DEFAULT;

    public LvmDriver()
    {
    }

    LvmDriver(final ExtCmd ec)
    {
        this.extCommand = ec;
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

        keySet.add(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        keySet.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);

        return keySet;
    }


    @Override
    protected String[] getCreateCommand(final String identifier, final long size)
    {
        return new String[]
        {
            lvmCreateCommand,
            "--size", size + "k",
            "-n", identifier,
            volumeGroup
        };
    }

    @Override
    protected String[] getDeleteCommand(String identifier)
    {
        return new String[]
        {
            lvmRemoveCommand,
            "-f", // skip the "are you sure?"
            volumeGroup + File.separator + identifier
        };
    }

    /**
     * Runs an <code>lvs</code> command, fetches the results and returns
     * the {@link LvsInfo} of the specified identifier. <br>
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
                throw new StorageException(
                    "Volume not found",
                    String.format("The volume [%s] was not found", identifier),
                    null,
                    null,
                    String.format("External command for querying (all) volumes: %s",
                        glue(
                            LvsInfo.getCommand(lvmLvsCommand, volumeGroup),
                            " "
                        )
                    )
                );
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to query volume information",
                null,
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format(
                    "External command: %s",
                    glue (
                        LvsInfo.getCommand(lvmLvsCommand, volumeGroup),
                        " "
                    )
                ),
                exc
            );
        }
        return info;
    }

    /**
     * performs a <code>vgs</code> command and returns the extent size of the
     * current volume group.
     *
     * @return
     * @throws StorageException
     */
    @Override
    protected long getExtentSize() throws StorageException
    {
        long extentSize = 0;
        final String[] command = new String[]
            {
                lvmVgsCommand,
                volumeGroup,
                "-o", "vg_extent_size",
                "--units", "k",
                "--noheadings"
            };
        try
        {
            final OutputData output = extCommand.exec(command);

            checkExitCode(output, command);

            String rawOut = new String(output.stdoutData);
            // cut everything after the decimal dot
            int indexOf = rawOut.indexOf(".");
            if (indexOf == -1)
            {
                indexOf = rawOut.indexOf(",");
            }
            rawOut = rawOut.substring(0, indexOf);
            extentSize = Long.parseLong(rawOut.trim());
        }

        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to query volume's extent size",
                String.format("Failed to query the extent size of volume: %s", volumeGroup),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format("External command: %s", glue(command, " ")),
                exc
            );
        }
        return extentSize;
    }



    @Override
    protected String getExpectedVolumePath(String identifier)
    {
        return File.separator + "dev" +
            File.separator + volumeGroup +
            File.separator +identifier;
    }

    @Override
    protected VolumeInfo getVolumeInfo(String identifier) throws StorageException
    {
        return getLvsInfoByIdentifier(identifier);
    }

    @Override
    protected void checkConfiguration(Map<String, String> config) throws StorageException
    {
        checkCommand(config, StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY);
        checkCommand(config, StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY);
        checkCommand(config, StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY);
        checkCommand(config, StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        checkCommand(config, StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        checkVolumeGroupEntry(config);
        checkToleranceFactor(config);
    }

    @Override
    protected void applyConfiguration(Map<String, String> config)
    {
        lvmCreateCommand = getAsString(config, StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY, lvmCreateCommand);
        lvmRemoveCommand = getAsString(config, StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY, lvmRemoveCommand);
        lvmChangeCommand = getAsString(config, StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY, lvmChangeCommand);
        lvmLvsCommand = getAsString(config, StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY, lvmLvsCommand);
        lvmVgsCommand = getAsString(config, StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY, lvmVgsCommand);

        volumeGroup = getAsString(config, StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
        sizeAlignmentToleranceFactor = uncheckedGetAsInt(config, StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY, sizeAlignmentToleranceFactor);
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return false;
    }

    @Override
    protected String[] getCreateSnapshotCommand(String identifier, String snapshotName)
    {
        throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
    }

    @Override
    protected String[] getRestoreSnapshotCommand(String sourceIdentifier, String snapshotName, String identifier)
    {
        throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
    }


    @Override
    protected String[] getDeleteSnapshotCommand(String identifier, String snapshotName)
    {
        throw new UnsupportedOperationException("Snapshots are not supported by " + getClass());
    }

    /**
     * Checks if the given map contains the key for volume group
     * ({@link StorageConstants#CONFIG_LVM_VOLUME_GROUP_KEY}) and
     * verifies if the volume group exists (with <code>vgs</code> command) and
     * performs a {@link Checks#nameCheck}.
     *
     * @param config
     * @throws StorageException
     */
    protected void checkVolumeGroupEntry(final Map<String, String> config) throws StorageException
    {
        String newVolumeGroup = config.get(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        if (newVolumeGroup != null)
        {
            newVolumeGroup = newVolumeGroup.trim();
            try
            {
                Checks.nameCheck(
                    newVolumeGroup,
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
                    String.format("Invalid name for volume group: %s", newVolumeGroup),
                    "Specify a valid and existing volume group name",
                    null
                );
            }

            final String[] volumeGroupCheckCommand = new String[]
                {
                    lvmVgsCommand,
                    "-o", "vg_name",
                    "--noheadings"
                };
            try
            {

                final OutputData output = extCommand.exec(volumeGroupCheckCommand);
                final String stdOut = new String(output.stdoutData);
                final String[] lines = stdOut.split("\n");
                boolean found = false;
                for (String line : lines)
                {
                    if (line.trim().equals(newVolumeGroup))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    throw new StorageException(
                        "Invalid configuration",
                        "Unknown volume group",
                        String.format("Volume group [%s] not found.", newVolumeGroup),
                        "Specify a valid and existing volume group name or create the desired volume group manually",
                        null
                    );
                }
            }
            catch (ChildProcessTimeoutException | IOException exc)
            {
                throw new StorageException(
                    "Failed to verify volume group name",
                    null,
                    (exc instanceof ChildProcessTimeoutException) ?
                        "External command timed out" :
                        "External command threw an IOException",
                    null,
                    String.format("External command: %s", glue(volumeGroupCheckCommand, " ")),
                    exc
                );
            }
        }
    }
}
