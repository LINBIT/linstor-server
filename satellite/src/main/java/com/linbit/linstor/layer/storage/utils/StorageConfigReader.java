package com.linbit.linstor.layer.storage.utils;

import com.linbit.Checks;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class StorageConfigReader
{
    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public static void checkVolumeGroupEntry(ExtCmdFactory extCmdFactory, ReadOnlyProps props)
        throws StorageException
    {
        String volumeGroup;
        try
        {
            volumeGroup = props.getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY).split("/")[0];
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded storPool prop key", exc);
        }
        checkVolumeGroupEntry(extCmdFactory, volumeGroup);
    }

    private static void checkVolumeGroupEntry(ExtCmdFactory extCmdFactory, String volumeGroup) throws StorageException
    {
        if (volumeGroup != null)
        {
            final String volumeGroupTrimmed = volumeGroup.trim();

            try
            {
                Checks.nameCheck(
                    volumeGroupTrimmed,
                    1,
                    Integer.MAX_VALUE,
                    VALID_CHARS,
                    VALID_INNER_CHARS
                );
            }
            catch (InvalidNameException invalidNameExc)
            {
                final String cause = String.format("Invalid name for volume group: %s", volumeGroupTrimmed);
                throw new StorageException(
                    "Invalid configuration, " + cause,
                    null,
                    cause,
                    "Specify a valid and existing volume group name",
                    null
                );
            }

            LvmUtils.checkVgExists(extCmdFactory, volumeGroupTrimmed); // throws an exception
            // if volume group does not exist
        }
    }

    public static void checkThinPoolEntry(ExtCmdFactory extCmdFactory, ReadOnlyProps props) throws StorageException
    {
        String volumeGroup;
        String thinPool;
        try
        {
            String prop = props.getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
            String[] split = prop.split("/");
            if (split.length != 2)
            {
                throw new StorageException(
                    "Storage pool name '" + prop + "' is not a valid thinpool name ('$VG/$THIN_POOL')"
                );
            }
            volumeGroup = split[0];
            thinPool = split[1];
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded storPool prop key", exc);
        }
        checkVolumeGroupEntry(extCmdFactory, volumeGroup);

        try
        {
            Checks.nameCheck(
                thinPool,
                1,
                Integer.MAX_VALUE,
                VALID_CHARS,
                VALID_INNER_CHARS
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            final String cause = String.format("Invalid name for thin pool: %s", thinPool);
            throw new StorageException(
                "Invalid configuration, " + cause,
                null,
                cause,
                "Specify a valid and existing thin pool",
                null
            );
        }

        LvmUtils.checkThinPoolExists(extCmdFactory, volumeGroup, thinPool); // throws an exception
        // if volume group does not exist
    }

    public static void checkToleranceFactor(ReadOnlyProps props) throws StorageException
    {
        String toleranceFactorStr = null;
        int toleranceFactor = -1;
        try
        {
            toleranceFactorStr = props.getProp(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);
            if (toleranceFactorStr != null)
            {
                toleranceFactor = Integer.parseInt(toleranceFactorStr);
                Checks.rangeCheck(toleranceFactor, 1, Integer.MAX_VALUE);
            }
        }
        catch (NumberFormatException nfexc)
        {
            throw new StorageException(
                "ToleranceFactor ('" + toleranceFactorStr + "' is not parsable as integer",
                nfexc
            );
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
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded storPool prop key", exc);
        }
    }

    public static void checkFileStorageDirectoryEntry(ReadOnlyProps propsRef)
        throws StorageException
    {
        Path path = null;
        try
        {
            String dirStr = propsRef.getProp(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);
            if (dirStr == null)
            {
                throw new StorageException("Mandatory property for storage directory missing");
            }
            path = Paths.get(dirStr);
            if (!path.isAbsolute())
            {
                throw new StorageException(
                    "Path for storage directory (FILE provider) has to be an absolute path.\n" +
                        path
                );
            }
            if (!Files.exists(path))
            {
                if (path.getParent() == null || !Files.exists(path.getParent()))
                {
                    throw new StorageException("Parent directory of '" + path + "' does not exist. ");
                }
                Files.createDirectory(path);
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded storPool prop key", exc);
        }
        catch (IOException exc)
        {
            throw new StorageException("IOException occurred when creating directory '" + path + "'", exc);
        }
    }

    private StorageConfigReader()
    {
    }
}
