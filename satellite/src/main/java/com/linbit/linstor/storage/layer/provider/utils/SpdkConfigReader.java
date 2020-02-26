package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.Checks;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.SpdkUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class SpdkConfigReader
{
    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    private SpdkConfigReader()
    {
    }

    public static void checkVolumeGroupEntry(ExtCmd extCmd, Props props)
        throws StorageException
    {
        String volumeGroup;
        try
        {
            volumeGroup = props.getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded storPool prop key", exc);
        }
        if (volumeGroup != null)
        {
            volumeGroup = volumeGroup.trim();

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
            catch (InvalidNameException invalidNameExc)
            {
                final String cause = String.format("Invalid name for volume group: %s", volumeGroup);
                throw new StorageException(
                    "Invalid configuration, " + cause,
                    null,
                    cause,
                    "Specify a valid and existing volume group name",
                    null
                );
            }

            // throws an exception if volume group does not exist
            SpdkUtils.checkVgExists(extCmd, volumeGroup);
        }
    }

    public static void checkToleranceFactor(Props props) throws StorageException
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
}
