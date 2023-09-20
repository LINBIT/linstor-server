package com.linbit.linstor.core;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

public class SysBlockUtils
{
    public static long getDrbdSizeInSectors(ExtCmdFactory extCmdFactoryRef, int minorNr) throws StorageException
    {
        String sizePath = "/sys/block/drbd" + minorNr + "/size";
        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create(),
            new String[] {"cat", sizePath},
            "Failed to query size of " + sizePath,
            "Failed to query size of " + sizePath
        );
        return Long.parseLong(new String(outputData.stdoutData).trim());
    }
}
